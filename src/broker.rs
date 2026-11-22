//! In-memory broker state: exchanges, queues, bindings, consumers, unacked messages.

use crate::codec::*;
use crate::store::{decode_args, encode_args, BindRow, Op, Store};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::UnboundedSender;

pub type ConnId = u64;
pub type Tx = UnboundedSender<Vec<u8>>;

pub struct Msg {
    pub exchange: String,
    pub routing_key: String,
    pub headers: Table,
    /// Raw basic properties (flags + values), forwarded verbatim.
    pub props: Vec<u8>,
    pub body: Vec<u8>,
    /// delivery_mode == 2
    pub persistent: bool,
    /// per-message TTL from the `expiration` property
    pub ttl_ms: Option<u64>,
}

/// A message sitting in a queue (or unacked on a channel).
pub struct Entry {
    msg: Arc<Msg>,
    redelivered: bool,
    /// Row id in the store, 0 if not persisted.
    id: i64,
    /// Times returned to the queue (only counted on quorum queues).
    delivery_count: u32,
    expires_at: Option<Instant>,
}

#[derive(Clone, Copy, PartialEq, Default, Debug)]
pub enum Overflow {
    #[default]
    DropHead,
    RejectPublish,
    RejectPublishDlx,
}

/// Behaviour derived from queue arguments (`x-*`).
#[derive(Clone, Default)]
pub struct QueueCfg {
    dlx: Option<String>,
    dlrk: Option<String>,
    ttl_ms: Option<u64>,
    max_length: Option<usize>,
    overflow: Overflow,
    quorum: bool,
    delivery_limit: Option<u32>,
}

fn int_of(v: &Value) -> Option<i64> {
    match v {
        Value::I8(x) => Some(*x as i64),
        Value::U8(x) => Some(*x as i64),
        Value::I16(x) => Some(*x as i64),
        Value::U16(x) => Some(*x as i64),
        Value::I32(x) => Some(*x as i64),
        Value::U32(x) => Some(*x as i64),
        Value::I64(x) => Some(*x),
        _ => None,
    }
}

fn str_of(v: &Value) -> Option<String> {
    match v {
        Value::Str(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

fn bad_arg(name: &str, key: &str) -> AmqpErr {
    AmqpErr::chan(
        PRECONDITION_FAILED,
        format!("PRECONDITION_FAILED - invalid arg '{key}' for queue '{name}' in vhost '/'"),
    )
}

fn parse_cfg(name: &str, args: &Table) -> Res<QueueCfg> {
    let mut c = QueueCfg::default();
    let mut limit = None;
    for (k, v) in args {
        let bad = || bad_arg(name, k);
        match k.as_str() {
            "x-dead-letter-exchange" => c.dlx = Some(str_of(v).ok_or_else(bad)?),
            "x-dead-letter-routing-key" => c.dlrk = Some(str_of(v).ok_or_else(bad)?),
            "x-message-ttl" => {
                c.ttl_ms = Some(int_of(v).filter(|n| *n >= 0).ok_or_else(bad)? as u64)
            }
            "x-max-length" => {
                c.max_length = Some(int_of(v).filter(|n| *n >= 0).ok_or_else(bad)? as usize)
            }
            "x-overflow" => {
                c.overflow = match str_of(v).as_deref() {
                    Some("drop-head") => Overflow::DropHead,
                    Some("reject-publish") => Overflow::RejectPublish,
                    Some("reject-publish-dlx") => Overflow::RejectPublishDlx,
                    _ => return Err(bad()),
                }
            }
            "x-queue-type" => match str_of(v).as_deref() {
                Some("classic") => {}
                Some("quorum") => c.quorum = true,
                _ => return Err(bad()),
            },
            "x-delivery-limit" => limit = Some(int_of(v).ok_or_else(bad)?),
            _ => {}
        }
    }
    if c.quorum {
        // quorum queues in RabbitMQ 4.x default to a delivery limit of 20
        c.delivery_limit = match limit {
            Some(n) if n < 0 => None,
            Some(n) => Some(n.min(u32::MAX as i64) as u32),
            None => Some(20),
        };
    }
    Ok(c)
}

fn fmt_val(v: Option<&Value>) -> String {
    match v {
        None => "none".into(),
        Some(x) => int_of(x)
            .map(|n| n.to_string())
            .or_else(|| str_of(x))
            .unwrap_or_else(|| format!("{x:?}")),
    }
}

/// First argument key whose value differs between two tables.
fn args_diff<'a>(
    a: &'a Table,
    b: &'a Table,
) -> Option<(String, Option<&'a Value>, Option<&'a Value>)> {
    fn get<'a>(t: &'a Table, k: &str) -> Option<&'a Value> {
        t.iter().find(|(n, _)| n == k).map(|(_, v)| v)
    }
    for (k, _) in a.iter().chain(b.iter()) {
        let (x, y) = (get(a, k), get(b, k));
        let same = match (x, y) {
            (None, None) => true,
            (Some(p), Some(q)) => match (int_of(p), int_of(q)) {
                (Some(m), Some(n)) => m == n,
                _ => p == q,
            },
            _ => false,
        };
        if !same {
            return Some((k.clone(), x, y));
        }
    }
    None
}

fn group_by_queue(items: Vec<(String, Entry)>) -> Vec<(String, Vec<Entry>)> {
    let mut out: Vec<(String, Vec<Entry>)> = Vec::new();
    for (qn, e) in items {
        match out.iter_mut().find(|(n, _)| *n == qn) {
            Some((_, v)) => v.push(e),
            None => out.push((qn, vec![e])),
        }
    }
    out
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Properties to send with a delivery: adds `x-delivery-count` once a message was returned.
fn out_props(msg: &Msg, count: u32) -> Cow<'_, [u8]> {
    if count == 0 {
        return Cow::Borrowed(&msg.props);
    }
    match parse_props(&msg.props) {
        Ok(mut p) => {
            let h = p.headers.get_or_insert_with(Vec::new);
            h.retain(|(k, _)| k != "x-delivery-count");
            h.push(("x-delivery-count".into(), Value::I64(count as i64)));
            Cow::Owned(p.encode())
        }
        Err(_) => Cow::Borrowed(&msg.props),
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Kind {
    Direct,
    Fanout,
    Topic,
    Headers,
}

impl Kind {
    fn parse(s: &str) -> Option<Kind> {
        Some(match s {
            "direct" => Kind::Direct,
            "fanout" => Kind::Fanout,
            "topic" => Kind::Topic,
            "headers" => Kind::Headers,
            _ => return None,
        })
    }
    fn name(self) -> &'static str {
        match self {
            Kind::Direct => "direct",
            Kind::Fanout => "fanout",
            Kind::Topic => "topic",
            Kind::Headers => "headers",
        }
    }
}

#[derive(Clone, PartialEq)]
pub enum Dest {
    Queue(String),
    Exchange(String),
}

#[derive(Clone)]
pub struct Binding {
    dest: Dest,
    key: String,
    args: Table,
}

pub struct Exchange {
    kind: Kind,
    durable: bool,
    auto_delete: bool,
    bindings: Vec<Binding>,
    had_bindings: bool,
}

pub struct Consumer {
    tag: String,
    conn: ConnId,
    ch: u16,
    no_ack: bool,
    exclusive: bool,
}

pub struct Queue {
    durable: bool,
    exclusive: Option<ConnId>,
    auto_delete: bool,
    msgs: VecDeque<Entry>,
    consumers: Vec<Consumer>,
    rr: usize,
    ever_consumed: bool,
    args: Table,
    cfg: QueueCfg,
}

struct ConnInfo {
    tx: Tx,
    frame_max: usize,
    peer: String,
}

#[derive(Default)]
struct Chan {
    next_tag: u64,
    unacked: BTreeMap<u64, (String, Entry)>,
    prefetch: u32,
    confirm_seq: Option<u64>,
    consumers: Vec<(String, String)>, // (tag, queue)
}

#[derive(Default)]
pub struct Broker {
    exchanges: HashMap<String, Exchange>,
    queues: HashMap<String, Queue>,
    conns: HashMap<ConnId, ConnInfo>,
    chans: HashMap<(ConnId, u16), Chan>,
    next_conn: u64,
    counter: u64,
    store: Option<Store>,
    storage: Option<String>,
    next_msg: i64,
    /// Added to the real clock for message TTLs; moved forward via the chaos API.
    clock_skew: Duration,
    /// RabbitMQ major version whose stricter behaviour is emulated (0 or 3 = permissive, 4 = RabbitMQ 4).
    rabbitmq_major: u8,
    /// How long each publisher confirm is held back. Zero confirms at once.
    confirm_delay: Duration,
    /// The next publishes to decline: they are dropped and answered with basic.nack.
    nack_remaining: u32,
    /// When the last delayed confirm is due, so confirms leave in the order they were made.
    last_confirm_due: Option<tokio::time::Instant>,
}

fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

fn bind_row(source: &str, b: &Binding) -> BindRow {
    let (dest, dest_is_queue) = match &b.dest {
        Dest::Queue(q) => (q.clone(), true),
        Dest::Exchange(x) => (x.clone(), false),
    };
    BindRow {
        source: source.into(),
        dest,
        dest_is_queue,
        key: b.key.clone(),
        args: encode_args(&b.args),
    }
}

fn no_queue(n: &str) -> AmqpErr {
    AmqpErr::chan(
        NOT_FOUND,
        format!("NOT_FOUND - no queue '{n}' in vhost '/'"),
    )
}
fn no_exchange(n: &str) -> AmqpErr {
    AmqpErr::chan(
        NOT_FOUND,
        format!("NOT_FOUND - no exchange '{n}' in vhost '/'"),
    )
}

pub fn topic_match(pat: &str, key: &str) -> bool {
    fn m(p: &[&str], k: &[&str]) -> bool {
        match p.first() {
            None => k.is_empty(),
            Some(&"#") => m(&p[1..], k) || (!k.is_empty() && m(p, &k[1..])),
            Some(w) => !k.is_empty() && (*w == "*" || *w == k[0]) && m(&p[1..], &k[1..]),
        }
    }
    let p: Vec<&str> = pat.split('.').collect();
    let k: Vec<&str> = key.split('.').collect();
    m(&p, &k)
}

fn headers_match(args: &Table, hdr: &Table) -> bool {
    let any = args
        .iter()
        .any(|(k, v)| k == "x-match" && *v == Value::Str(b"any".to_vec()));
    let mut seen = false;
    for (k, v) in args.iter().filter(|(k, _)| !k.starts_with("x-")) {
        seen = true;
        let hit = hdr.iter().any(|(hk, hv)| hk == k && hv == v);
        if any && hit {
            return true;
        }
        if !any && !hit {
            return false;
        }
    }
    !any || !seen
}

impl Broker {
    /// Emulate the stricter behaviour of a RabbitMQ major version. At 4, declaring a transient queue that is not
    /// exclusive is refused, as RabbitMQ 4 does unless `transient_nonexcl_queues` is permitted.
    pub fn set_rabbitmq_major(&mut self, major: u8) {
        self.rabbitmq_major = major;
    }

    /// Hold every publisher confirm back by `delay`, as a loaded or distant broker does. Confirms keep their order.
    pub fn set_confirm_delay(&mut self, delay: Duration) {
        self.confirm_delay = delay;
    }

    /// Decline the next `count` publishes on channels in confirm mode: the message is dropped and the broker answers basic.nack.
    pub fn nack_next_publishes(&mut self, count: u32) {
        self.nack_remaining = count;
    }

    // Sends a frame now, or after the confirm delay when one is set. Delayed frames keep their order.
    fn send_confirm(&mut self, conn: ConnId, bytes: Vec<u8>) {
        if self.confirm_delay.is_zero() {
            self.send(conn, bytes);
            return;
        }
        let Some(tx) = self.conns.get(&conn).map(|c| c.tx.clone()) else {
            return;
        };
        let now = tokio::time::Instant::now();
        let mut due = now + self.confirm_delay;
        if let Some(last) = self.last_confirm_due {
            if due <= last {
                due = last + Duration::from_micros(1);
            }
        }
        self.last_confirm_due = Some(due);
        tokio::spawn(async move {
            tokio::time::sleep_until(due).await;
            let _ = tx.send(bytes);
        });
    }

    pub fn new() -> Self {
        let mut b = Broker::default();
        for (n, k) in [
            ("", Kind::Direct),
            ("amq.direct", Kind::Direct),
            ("amq.fanout", Kind::Fanout),
            ("amq.topic", Kind::Topic),
            ("amq.headers", Kind::Headers),
            ("amq.match", Kind::Headers),
        ] {
            b.exchanges.insert(
                n.into(),
                Exchange {
                    kind: k,
                    durable: true,
                    auto_delete: false,
                    bindings: vec![],
                    had_bindings: false,
                },
            );
        }
        b
    }

    /// Open (or create) a SQLite-backed broker and restore its durable state.
    pub fn open(path: &str) -> Result<Self, String> {
        let (store, l) = Store::open(path)?;
        let mut b = Broker::new();
        for (name, kind, auto_delete) in l.exchanges {
            if let Some(kind) = Kind::parse(&kind) {
                b.exchanges.insert(
                    name,
                    Exchange {
                        kind,
                        durable: true,
                        auto_delete,
                        bindings: vec![],
                        had_bindings: false,
                    },
                );
            }
        }
        for (name, auto_delete, blob) in l.queues {
            let args = decode_args(&blob);
            let cfg = parse_cfg(&name, &args).unwrap_or_default();
            b.queues.insert(
                name,
                Queue {
                    durable: true,
                    exclusive: None,
                    auto_delete,
                    msgs: VecDeque::new(),
                    consumers: vec![],
                    rr: 0,
                    ever_consumed: false,
                    args,
                    cfg,
                },
            );
        }
        for r in l.bindings {
            let dest = if r.dest_is_queue {
                Dest::Queue(r.dest)
            } else {
                Dest::Exchange(r.dest)
            };
            let dest_ok = match &dest {
                Dest::Queue(q) => b.queues.contains_key(q),
                Dest::Exchange(x) => b.exchanges.contains_key(x),
            };
            if let (true, Some(e)) = (dest_ok, b.exchanges.get_mut(&r.source)) {
                e.bindings.push(Binding {
                    dest,
                    key: r.key,
                    args: decode_args(&r.args),
                });
                e.had_bindings = true;
            }
        }
        for m in l.messages {
            b.next_msg = b.next_msg.max(m.id);
            let Some(q) = b.queues.get_mut(&m.queue) else {
                continue;
            };
            let parsed = parse_props(&m.props).unwrap_or_default();
            let ttl_ms = parsed.expiration.as_deref().and_then(|e| e.parse().ok());
            let ttl = match (q.cfg.ttl_ms, ttl_ms) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            let msg = Arc::new(Msg {
                exchange: m.exchange,
                routing_key: m.routing_key,
                headers: parsed.headers.unwrap_or_default(),
                props: m.props,
                body: m.body,
                persistent: true,
                ttl_ms,
            });
            q.msgs.push_back(Entry {
                msg,
                redelivered: false,
                id: m.id,
                delivery_count: 0,
                expires_at: ttl.map(|t| Instant::now() + Duration::from_millis(t)),
            });
        }
        b.store = Some(store);
        b.storage = Some(path.to_string());
        Ok(b)
    }

    /// (queues, ready messages) currently held — used for the startup banner.
    pub fn counts(&self) -> (usize, usize) {
        (
            self.queues.len(),
            self.queues.values().map(|q| q.msgs.len()).sum(),
        )
    }

    pub fn store(&self) -> Option<Store> {
        self.store.clone()
    }

    fn persist(&self, op: impl FnOnce() -> Op) {
        if let Some(s) = &self.store {
            s.send(op());
        }
    }

    pub fn next_id(&mut self) -> u64 {
        self.counter += 1;
        self.counter
    }

    // ---- connections & channels -------------------------------------------------

    pub fn register(&mut self, tx: Tx, frame_max: usize, peer: String) -> ConnId {
        self.next_conn += 1;
        self.conns.insert(
            self.next_conn,
            ConnInfo {
                tx,
                frame_max,
                peer,
            },
        );
        self.next_conn
    }

    pub fn open_chan(&mut self, conn: ConnId, ch: u16) {
        self.chans.insert(
            (conn, ch),
            Chan {
                next_tag: 1,
                ..Default::default()
            },
        );
    }

    pub fn close_chan(&mut self, conn: ConnId, ch: u16) {
        let Some(c) = self.chans.remove(&(conn, ch)) else {
            return;
        };
        for (tag, qn) in &c.consumers {
            if let Some(q) = self.queues.get_mut(qn) {
                q.consumers
                    .retain(|x| !(x.conn == conn && x.ch == ch && x.tag == *tag));
            }
            self.maybe_autodelete(qn);
        }
        let entries: Vec<(String, Entry)> = c.unacked.into_values().collect();
        for (qn, msgs) in group_by_queue(entries) {
            self.requeue(&qn, msgs);
            self.dispatch(&qn);
        }
    }

    pub fn close_conn(&mut self, conn: ConnId) {
        let chs: Vec<u16> = self
            .chans
            .keys()
            .filter(|(c, _)| *c == conn)
            .map(|(_, ch)| *ch)
            .collect();
        for ch in chs {
            self.close_chan(conn, ch);
        }
        let owned: Vec<String> = self
            .queues
            .iter()
            .filter(|(_, q)| q.exclusive == Some(conn))
            .map(|(n, _)| n.clone())
            .collect();
        for n in owned {
            self.remove_queue(&n);
        }
        self.conns.remove(&conn);
    }

    fn send(&self, conn: ConnId, bytes: Vec<u8>) {
        if let Some(c) = self.conns.get(&conn) {
            let _ = c.tx.send(bytes);
        }
    }

    // ---- exchanges --------------------------------------------------------------

    pub fn declare_exchange(
        &mut self,
        name: &str,
        kind: &str,
        passive: bool,
        durable: bool,
        auto_delete: bool,
    ) -> Res<()> {
        if let Some(e) = self.exchanges.get(name) {
            if passive {
                return Ok(());
            }
            if e.kind.name() != kind {
                return Err(AmqpErr::chan(
                    PRECONDITION_FAILED,
                    format!(
                        "PRECONDITION_FAILED - inequivalent arg 'type' for exchange '{name}' in vhost '/': received '{kind}' but current is '{}'",
                        e.kind.name()
                    ),
                ));
            }
            if e.durable != durable || e.auto_delete != auto_delete {
                return Err(AmqpErr::chan(
                    PRECONDITION_FAILED,
                    format!("PRECONDITION_FAILED - inequivalent arg 'durable/auto_delete' for exchange '{name}' in vhost '/'"),
                ));
            }
            return Ok(());
        }
        if passive {
            return Err(no_exchange(name));
        }
        if name.starts_with("amq.") {
            return Err(AmqpErr::chan(
                ACCESS_REFUSED,
                format!("ACCESS_REFUSED - exchange name '{name}' contains reserved prefix 'amq.*'"),
            ));
        }
        let k = Kind::parse(kind).ok_or_else(|| {
            AmqpErr::conn(
                COMMAND_INVALID,
                format!("COMMAND_INVALID - invalid exchange type '{kind}'"),
            )
        })?;
        self.exchanges.insert(
            name.into(),
            Exchange {
                kind: k,
                durable,
                auto_delete,
                bindings: vec![],
                had_bindings: false,
            },
        );
        if durable {
            self.persist(|| Op::Exchange {
                name: name.into(),
                kind: k.name().into(),
                auto_delete,
            });
        }
        Ok(())
    }

    pub fn delete_exchange(&mut self, name: &str, if_unused: bool) -> Res<()> {
        if name.is_empty() || name.starts_with("amq.") {
            return Err(AmqpErr::chan(
                ACCESS_REFUSED,
                format!("ACCESS_REFUSED - operation not permitted on the exchange '{name}'"),
            ));
        }
        let Some(e) = self.exchanges.get(name) else {
            return Ok(());
        };
        if if_unused && !e.bindings.is_empty() {
            return Err(AmqpErr::chan(
                PRECONDITION_FAILED,
                format!("PRECONDITION_FAILED - exchange '{name}' in vhost '/' in use"),
            ));
        }
        if let Some(e) = self.exchanges.remove(name) {
            if e.durable {
                self.persist(|| Op::DeleteExchange(name.into()));
            }
        }
        for e in self.exchanges.values_mut() {
            e.bindings.retain(|b| b.dest != Dest::Exchange(name.into()));
        }
        self.gc_exchanges();
        Ok(())
    }

    fn gc_exchanges(&mut self) {
        let dead: Vec<String> = self
            .exchanges
            .iter()
            .filter(|(_, e)| e.auto_delete && e.had_bindings && e.bindings.is_empty())
            .map(|(n, _)| n.clone())
            .collect();
        for n in dead {
            if let Some(e) = self.exchanges.remove(&n) {
                if e.durable {
                    self.persist(|| Op::DeleteExchange(n));
                }
            }
        }
    }

    pub fn bind(&mut self, dest: Dest, source: &str, key: &str, args: Table) -> Res<()> {
        match &dest {
            Dest::Queue(q) if !self.queues.contains_key(q) => return Err(no_queue(q)),
            Dest::Exchange(x) if !self.exchanges.contains_key(x) => return Err(no_exchange(x)),
            _ => {}
        }
        if source.is_empty() {
            return Err(AmqpErr::chan(
                ACCESS_REFUSED,
                "ACCESS_REFUSED - operation not permitted on the default exchange",
            ));
        }
        let dest_durable = match &dest {
            Dest::Queue(q) => self
                .queues
                .get(q)
                .is_some_and(|q| q.durable && q.exclusive.is_none()),
            Dest::Exchange(x) => self.exchanges.get(x).is_some_and(|x| x.durable),
        };
        let e = self
            .exchanges
            .get_mut(source)
            .ok_or_else(|| no_exchange(source))?;
        let b = Binding {
            dest,
            key: key.into(),
            args,
        };
        let row = (e.durable && dest_durable).then(|| bind_row(source, &b));
        if !e
            .bindings
            .iter()
            .any(|x| x.dest == b.dest && x.key == b.key && x.args == b.args)
        {
            e.bindings.push(b);
        }
        e.had_bindings = true;
        if let Some(row) = row {
            self.persist(|| Op::Bind(row));
        }
        Ok(())
    }

    pub fn unbind(&mut self, dest: Dest, source: &str, key: &str, args: &Table) -> Res<()> {
        let e = self
            .exchanges
            .get_mut(source)
            .ok_or_else(|| no_exchange(source))?;
        e.bindings
            .retain(|x| !(x.dest == dest && x.key == key && x.args == *args));
        if e.durable {
            let row = bind_row(
                source,
                &Binding {
                    dest,
                    key: key.into(),
                    args: args.clone(),
                },
            );
            self.persist(|| Op::Unbind(row));
        }
        self.gc_exchanges();
        Ok(())
    }

    // ---- queues -----------------------------------------------------------------

    fn check_queue(&self, conn: ConnId, name: &str) -> Res<&Queue> {
        let q = self.queues.get(name).ok_or_else(|| no_queue(name))?;
        match q.exclusive {
            Some(o) if o != conn => Err(AmqpErr::chan(
                RESOURCE_LOCKED,
                format!("RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '{name}' in vhost '/'"),
            )),
            _ => Ok(q),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn declare_queue(
        &mut self,
        conn: ConnId,
        name: &str,
        passive: bool,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        args: Table,
    ) -> Res<(String, u32, u32)> {
        if !passive && self.rabbitmq_major >= 4 && !durable && !exclusive {
            return Err(AmqpErr::conn(
                INTERNAL_ERROR,
                "INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated.\nBy default, this feature is not permitted anymore.\nThe feature will be removed from a future major RabbitMQ version, regardless of the configuration; actual version to be determined.\nTo continue using this feature when it is not permitted by default, set the following parameter in your configuration:\n    \"deprecated_features.permit.transient_nonexcl_queues = true\"",
            ));
        }
        let name = if name.is_empty() {
            let n = self.next_id();
            format!("amq.gen-{n:08x}")
        } else {
            name.to_string()
        };
        if self.queues.contains_key(&name) {
            let q = self.check_queue(conn, &name)?;
            if !passive && (q.durable != durable || q.auto_delete != auto_delete) {
                return Err(AmqpErr::chan(
                    PRECONDITION_FAILED,
                    format!("PRECONDITION_FAILED - inequivalent arg 'durable/auto_delete' for queue '{name}' in vhost '/'"),
                ));
            }
            if !passive {
                if let Some((k, got, cur)) = args_diff(&args, &q.args) {
                    return Err(AmqpErr::chan(
                        PRECONDITION_FAILED,
                        format!(
                            "PRECONDITION_FAILED - inequivalent arg '{k}' for queue '{name}' in vhost '/': received '{}' but current is '{}'",
                            fmt_val(got),
                            fmt_val(cur)
                        ),
                    ));
                }
            }
            return Ok((name.clone(), q.msgs.len() as u32, q.consumers.len() as u32));
        }
        if passive {
            return Err(no_queue(&name));
        }
        if name.starts_with("amq.") && !name.starts_with("amq.gen-") {
            return Err(AmqpErr::chan(
                ACCESS_REFUSED,
                format!("ACCESS_REFUSED - queue name '{name}' contains reserved prefix 'amq.*'"),
            ));
        }
        let cfg = parse_cfg(&name, &args)?;
        if cfg.quorum {
            let bad = if !durable {
                Some("non-durable")
            } else if exclusive {
                Some("exclusive")
            } else if auto_delete {
                Some("auto-delete")
            } else {
                None
            };
            if let Some(prop) = bad {
                return Err(AmqpErr::chan(
                    PRECONDITION_FAILED,
                    format!("PRECONDITION_FAILED - invalid property '{prop}' for queue '{name}' in vhost '/'"),
                ));
            }
        }
        if durable && !exclusive {
            self.persist(|| Op::Queue {
                name: name.clone(),
                auto_delete,
                args: encode_args(&args),
            });
        }
        self.queues.insert(
            name.clone(),
            Queue {
                durable,
                exclusive: exclusive.then_some(conn),
                auto_delete,
                msgs: VecDeque::new(),
                consumers: vec![],
                rr: 0,
                ever_consumed: false,
                args,
                cfg,
            },
        );
        Ok((name, 0, 0))
    }

    /// Drop all ready messages (unacked ones are untouched), returning how many.
    fn purge_ready(&mut self, name: &str) -> u32 {
        let q = self.queues.get_mut(name).unwrap();
        let n = q.msgs.len() as u32;
        let ids: Vec<i64> = q.msgs.drain(..).map(|e| e.id).filter(|i| *i != 0).collect();
        if !ids.is_empty() {
            self.persist(|| Op::Remove(ids));
        }
        n
    }

    pub fn check_queue_access(&self, conn: ConnId, name: &str) -> Res<()> {
        self.check_queue(conn, name).map(|_| ())
    }

    pub fn purge_queue(&mut self, conn: ConnId, name: &str) -> Res<u32> {
        self.check_queue(conn, name)?;
        Ok(self.purge_ready(name))
    }

    pub fn delete_queue(
        &mut self,
        conn: ConnId,
        name: &str,
        if_unused: bool,
        if_empty: bool,
    ) -> Res<u32> {
        let Some(q) = self.queues.get(name) else {
            return Ok(0);
        };
        self.check_queue(conn, name)?;
        if if_unused && !q.consumers.is_empty() {
            return Err(AmqpErr::chan(
                PRECONDITION_FAILED,
                format!("PRECONDITION_FAILED - queue '{name}' in vhost '/' in use"),
            ));
        }
        if if_empty && !q.msgs.is_empty() {
            return Err(AmqpErr::chan(
                PRECONDITION_FAILED,
                format!("PRECONDITION_FAILED - queue '{name}' in vhost '/' is not empty"),
            ));
        }
        let n = q.msgs.len() as u32;
        self.remove_queue(name);
        Ok(n)
    }

    fn remove_queue(&mut self, name: &str) {
        let Some(q) = self.queues.remove(name) else {
            return;
        };
        if q.durable && q.exclusive.is_none() {
            self.persist(|| Op::DeleteQueue(name.into()));
        }
        for c in q.consumers {
            // consumer_cancel_notify
            self.send(
                c.conn,
                method(c.ch, 60, 30, |w| {
                    w.short(&c.tag);
                    w.u8(1);
                }),
            );
            if let Some(ch) = self.chans.get_mut(&(c.conn, c.ch)) {
                ch.consumers.retain(|(t, _)| *t != c.tag);
            }
        }
        for e in self.exchanges.values_mut() {
            e.bindings.retain(|b| b.dest != Dest::Queue(name.into()));
        }
        self.gc_exchanges();
    }

    fn maybe_autodelete(&mut self, name: &str) {
        if let Some(q) = self.queues.get(name) {
            if q.auto_delete && q.ever_consumed && q.consumers.is_empty() {
                self.remove_queue(name);
            }
        }
    }

    // ---- publishing & routing ---------------------------------------------------

    fn route(
        &self,
        ex: &str,
        rk: &str,
        hdr: &Table,
        out: &mut Vec<String>,
        seen: &mut HashSet<String>,
    ) {
        if ex.is_empty() {
            if self.queues.contains_key(rk) && !out.iter().any(|q| q == rk) {
                out.push(rk.to_string());
            }
            return;
        }
        if !seen.insert(ex.to_string()) {
            return;
        }
        let Some(e) = self.exchanges.get(ex) else {
            return;
        };
        for b in &e.bindings {
            let hit = match e.kind {
                Kind::Direct => b.key == rk,
                Kind::Fanout => true,
                Kind::Topic => topic_match(&b.key, rk),
                Kind::Headers => headers_match(&b.args, hdr),
            };
            if !hit {
                continue;
            }
            match &b.dest {
                Dest::Queue(q) => {
                    if !out.contains(q) {
                        out.push(q.clone());
                    }
                }
                Dest::Exchange(x) => self.route(x, rk, hdr, out, seen),
            }
        }
    }

    pub fn publish(
        &mut self,
        conn: ConnId,
        ch: u16,
        exchange: &str,
        rk: &str,
        mandatory: bool,
        msg: Arc<Msg>,
    ) -> Res<()> {
        if !self.exchanges.contains_key(exchange) {
            return Err(no_exchange(exchange));
        }
        // Fault injection: decline this publish. The message is dropped and the publisher is told with basic.nack.
        let in_confirm_mode = self
            .chans
            .get(&(conn, ch))
            .is_some_and(|c| c.confirm_seq.is_some());
        let declined = in_confirm_mode && self.nack_remaining > 0;
        if declined {
            self.nack_remaining -= 1;
        }
        let mut qs = Vec::new();
        if !declined {
            self.route(exchange, rk, &msg.headers, &mut qs, &mut HashSet::new());
        }
        let mut rejected = declined;
        for qn in &qs {
            if !self.enqueue(qn, &msg) {
                rejected = true;
            }
            self.dispatch(qn);
        }
        let frame_max = self.conns.get(&conn).map_or(131072, |c| c.frame_max);
        if qs.is_empty() && mandatory && !declined {
            let mut b = method(ch, 60, 50, |w| {
                w.u16(312);
                w.short("NO_ROUTE");
                w.short(exchange);
                w.short(rk);
            });
            b.extend(content(ch, &msg.props, &msg.body, frame_max));
            self.send(conn, b);
        }
        let mut confirm = None;
        if let Some(c) = self.chans.get_mut(&(conn, ch)) {
            if let Some(seq) = c.confirm_seq.as_mut() {
                let tag = *seq;
                *seq += 1;
                // basic.nack when a queue rejected the publish (x-overflow=reject-publish) or one was declined, else basic.ack
                let id = if rejected { 120 } else { 80 };
                confirm = Some(method(ch, 60, id, |w| {
                    w.u64(tag);
                    w.u8(0);
                }));
            }
        }
        if let Some(frame) = confirm {
            self.send_confirm(conn, frame);
        }
        Ok(())
    }

    pub fn confirm_select(&mut self, conn: ConnId, ch: u16) {
        if let Some(c) = self.chans.get_mut(&(conn, ch)) {
            c.confirm_seq.get_or_insert(1);
        }
    }

    // ---- consuming --------------------------------------------------------------

    pub fn consume(
        &mut self,
        conn: ConnId,
        ch: u16,
        queue: &str,
        tag: &str,
        no_ack: bool,
        exclusive: bool,
    ) -> Res<String> {
        let q = self.check_queue(conn, queue)?;
        if q.consumers.iter().any(|c| c.exclusive) || (exclusive && !q.consumers.is_empty()) {
            return Err(AmqpErr::chan(
                ACCESS_REFUSED,
                format!("ACCESS_REFUSED - queue '{queue}' in vhost '/' in exclusive use"),
            ));
        }
        let tag = if tag.is_empty() {
            format!("amq.ctag-{:08x}", self.next_id())
        } else {
            tag.to_string()
        };
        let chan = self.chans.get_mut(&(conn, ch)).unwrap();
        if chan.consumers.iter().any(|(t, _)| *t == tag) {
            return Err(AmqpErr::chan(
                NOT_ALLOWED,
                format!("NOT_ALLOWED - attempt to reuse consumer tag '{tag}'"),
            ));
        }
        chan.consumers.push((tag.clone(), queue.to_string()));
        let q = self.queues.get_mut(queue).unwrap();
        q.ever_consumed = true;
        q.consumers.push(Consumer {
            tag: tag.clone(),
            conn,
            ch,
            no_ack,
            exclusive,
        });
        Ok(tag)
    }

    pub fn cancel(&mut self, conn: ConnId, ch: u16, tag: &str) {
        let Some(chan) = self.chans.get_mut(&(conn, ch)) else {
            return;
        };
        let Some(pos) = chan.consumers.iter().position(|(t, _)| t == tag) else {
            return;
        };
        let (_, qn) = chan.consumers.remove(pos);
        if let Some(q) = self.queues.get_mut(&qn) {
            q.consumers
                .retain(|c| !(c.conn == conn && c.ch == ch && c.tag == tag));
        }
        self.maybe_autodelete(&qn);
    }

    pub fn set_prefetch(&mut self, conn: ConnId, ch: u16, n: u32) {
        let queues: Vec<String> = match self.chans.get_mut(&(conn, ch)) {
            Some(c) => {
                c.prefetch = n;
                c.consumers.iter().map(|(_, q)| q.clone()).collect()
            }
            None => return,
        };
        for q in queues {
            self.dispatch(&q);
        }
    }

    /// Hand queued messages to eligible consumers, round-robin, honoring prefetch.
    pub fn dispatch(&mut self, qn: &str) {
        self.dispatch_inner(qn);
        self.expire_head(qn);
    }

    fn dispatch_inner(&mut self, qn: &str) {
        loop {
            let Some(q) = self.queues.get_mut(qn) else {
                return;
            };
            let n = q.consumers.len();
            if n == 0 || q.msgs.is_empty() {
                return;
            }
            let mut pick = None;
            for i in 0..n {
                let idx = (q.rr + i) % n;
                let c = &q.consumers[idx];
                let ok = c.no_ack
                    || self.chans.get(&(c.conn, c.ch)).is_some_and(|ch| {
                        ch.prefetch == 0 || (ch.unacked.len() as u32) < ch.prefetch
                    });
                if ok {
                    pick = Some(idx);
                    break;
                }
            }
            let Some(idx) = pick else { return };
            q.rr = (idx + 1) % n;
            let entry = q.msgs.pop_front().unwrap();
            let (msg, redelivered, count) =
                (entry.msg.clone(), entry.redelivered, entry.delivery_count);
            let c = &q.consumers[idx];
            let (conn, ch, tag, no_ack) = (c.conn, c.ch, c.tag.clone(), c.no_ack);
            let chan = self.chans.get_mut(&(conn, ch)).unwrap();
            let dtag = chan.next_tag;
            chan.next_tag += 1;
            let mut done = 0;
            if no_ack {
                done = entry.id;
            } else {
                chan.unacked.insert(dtag, (qn.to_string(), entry));
            }
            if done != 0 {
                self.persist(|| Op::Remove(vec![done]));
            }
            let frame_max = self.conns.get(&conn).map_or(131072, |c| c.frame_max);
            let mut b = method(ch, 60, 60, |w| {
                w.short(&tag);
                w.u64(dtag);
                w.u8(redelivered as u8);
                w.short(&msg.exchange);
                w.short(&msg.routing_key);
            });
            b.extend(content(ch, &out_props(&msg, count), &msg.body, frame_max));
            self.send(conn, b);
        }
    }

    pub fn get(&mut self, conn: ConnId, ch: u16, queue: &str, no_ack: bool) -> Res<()> {
        self.check_queue(conn, queue)?;
        self.expire_head(queue);
        let q = self.queues.get_mut(queue).unwrap();
        let Some(entry) = q.msgs.pop_front() else {
            self.send(conn, method(ch, 60, 72, |w| w.short("")));
            return Ok(());
        };
        let (msg, redelivered, count) =
            (entry.msg.clone(), entry.redelivered, entry.delivery_count);
        let remaining = q.msgs.len() as u32;
        let chan = self.chans.get_mut(&(conn, ch)).unwrap();
        let dtag = chan.next_tag;
        chan.next_tag += 1;
        let mut done = 0;
        if no_ack {
            done = entry.id;
        } else {
            chan.unacked.insert(dtag, (queue.to_string(), entry));
        }
        if done != 0 {
            self.persist(|| Op::Remove(vec![done]));
        }
        let frame_max = self.conns.get(&conn).map_or(131072, |c| c.frame_max);
        let mut b = method(ch, 60, 71, |w| {
            w.u64(dtag);
            w.u8(redelivered as u8);
            w.short(&msg.exchange);
            w.short(&msg.routing_key);
            w.u32(remaining);
        });
        b.extend(content(ch, &out_props(&msg, count), &msg.body, frame_max));
        self.send(conn, b);
        Ok(())
    }

    /// ack (`requeue == None`), or nack/reject with/without requeue.
    pub fn settle(
        &mut self,
        conn: ConnId,
        ch: u16,
        tag: u64,
        multiple: bool,
        requeue: Option<bool>,
    ) -> Res<()> {
        let c = self.chans.get_mut(&(conn, ch)).unwrap();
        let tags: Vec<u64> = if multiple {
            let upto = if tag == 0 { u64::MAX } else { tag };
            c.unacked.range(..=upto).map(|(k, _)| *k).collect()
        } else if c.unacked.contains_key(&tag) {
            vec![tag]
        } else {
            return Err(AmqpErr::chan(
                PRECONDITION_FAILED,
                format!("PRECONDITION_FAILED - unknown delivery tag {tag}"),
            ));
        };
        let mut touched: Vec<String> = Vec::new();
        let mut back: Vec<(String, Entry)> = Vec::new();
        let mut gone: Vec<i64> = Vec::new();
        let mut dead: Vec<(String, Arc<Msg>)> = Vec::new();
        for t in tags {
            let (qn, m) = c.unacked.remove(&t).unwrap();
            if !touched.contains(&qn) {
                touched.push(qn.clone());
            }
            if requeue == Some(true) {
                back.push((qn, m));
            } else {
                if m.id != 0 {
                    gone.push(m.id);
                }
                if requeue == Some(false) {
                    dead.push((qn, m.msg.clone()));
                }
            }
        }
        if !gone.is_empty() {
            self.persist(|| Op::Remove(gone));
        }
        for (qn, msgs) in group_by_queue(back) {
            self.requeue(&qn, msgs);
        }
        for (qn, msg) in dead {
            self.dead_letter(&qn, msg, "rejected");
        }
        // An ack frees a prefetch slot for the whole channel, so queues the channel consumes from but did not just
        // settle may now be able to deliver too. Without this, a message waiting on a second queue stays stuck.
        let mut waiting: Vec<String> = self
            .chans
            .get(&(conn, ch))
            .map(|c| c.consumers.iter().map(|(_, q)| q.clone()).collect())
            .unwrap_or_default();
        for qn in touched {
            waiting.retain(|q| *q != qn);
            self.dispatch(&qn);
        }
        for qn in waiting {
            self.dispatch(&qn);
        }
        Ok(())
    }

    /// basic.recover: requeue every unacked message on the channel.
    pub fn recover(&mut self, conn: ConnId, ch: u16) {
        let Some(c) = self.chans.get_mut(&(conn, ch)) else {
            return;
        };
        let all = std::mem::take(&mut c.unacked);
        for (qn, msgs) in group_by_queue(all.into_values().collect()) {
            self.requeue(&qn, msgs);
            self.dispatch(&qn);
        }
    }

    // ---- queue arguments: max-length, TTL, delivery limit, dead-lettering --------

    /// Append to a queue, applying `x-max-length`. Returns false if the queue
    /// rejected the message (`x-overflow=reject-publish`).
    fn enqueue(&mut self, qn: &str, msg: &Arc<Msg>) -> bool {
        let persist = msg.persistent && self.store.is_some();
        let now = Instant::now() + self.clock_skew;
        let Some(q) = self.queues.get_mut(qn) else {
            return false;
        };
        let mut victim = None;
        if let Some(max) = q.cfg.max_length {
            if q.msgs.len() >= max {
                if q.cfg.overflow == Overflow::DropHead {
                    victim = q.msgs.pop_front();
                } else {
                    let dl = q.cfg.overflow == Overflow::RejectPublishDlx;
                    if dl {
                        self.dead_letter(qn, msg.clone(), "maxlen");
                    }
                    return false;
                }
            }
        }
        let mut id = 0;
        if persist && q.durable && q.exclusive.is_none() {
            self.next_msg += 1;
            id = self.next_msg;
        }
        let ttl = match (q.cfg.ttl_ms, msg.ttl_ms) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        q.msgs.push_back(Entry {
            msg: msg.clone(),
            redelivered: false,
            id,
            delivery_count: 0,
            expires_at: ttl.map(|t| now + Duration::from_millis(t)),
        });
        if id != 0 {
            self.persist(|| Op::Enqueue {
                id,
                queue: qn.to_string(),
                msg: msg.clone(),
            });
        }
        if let Some(v) = victim {
            if v.id != 0 {
                let vid = v.id;
                self.persist(|| Op::Remove(vec![vid]));
            }
            self.dead_letter(qn, v.msg, "maxlen");
        }
        true
    }

    /// Return messages (in delivery order) to the head of a queue, counting
    /// redeliveries on quorum queues and enforcing their delivery limit.
    fn requeue(&mut self, qn: &str, entries: Vec<Entry>) {
        let Some(q) = self.queues.get_mut(qn) else {
            return;
        };
        let (quorum, limit) = (q.cfg.quorum, q.cfg.delivery_limit);
        let mut keep = Vec::new();
        let mut over = Vec::new();
        for mut e in entries {
            e.redelivered = true;
            if quorum {
                e.delivery_count += 1;
                if limit.is_some_and(|l| e.delivery_count > l) {
                    over.push(e);
                    continue;
                }
            }
            keep.push(e);
        }
        for e in keep.into_iter().rev() {
            q.msgs.push_front(e);
        }
        for e in over {
            if e.id != 0 {
                let id = e.id;
                self.persist(|| Op::Remove(vec![id]));
            }
            self.dead_letter(qn, e.msg, "delivery_limit");
        }
    }

    /// Republish `msg` to the queue's dead-letter exchange (if any), recording `x-death`.
    fn dead_letter(&mut self, qn: &str, msg: Arc<Msg>, reason: &str) {
        let Some(q) = self.queues.get(qn) else { return };
        let Some(dlx) = q.cfg.dlx.clone() else { return };
        let rk = q
            .cfg
            .dlrk
            .clone()
            .unwrap_or_else(|| msg.routing_key.clone());
        if !self.exchanges.contains_key(&dlx) {
            return;
        }
        let Ok(mut props) = parse_props(&msg.props) else {
            return;
        };
        let s = |x: &str| Value::Str(x.as_bytes().to_vec());
        let field = |t: &Table, k: &str| t.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
        let mut headers = props.headers.take().unwrap_or_default();
        let mut deaths: Vec<Table> = match headers.iter().position(|(k, _)| k == "x-death") {
            Some(i) => match headers.remove(i).1 {
                Value::Array(a) => a
                    .into_iter()
                    .filter_map(|v| match v {
                        Value::Table(t) => Some(t),
                        _ => None,
                    })
                    .collect(),
                _ => vec![],
            },
            None => vec![],
        };
        let entry = match deaths
            .iter()
            .position(|t| field(t, "queue") == Some(s(qn)) && field(t, "reason") == Some(s(reason)))
        {
            Some(i) => {
                let mut t = deaths.remove(i);
                for (k, v) in t.iter_mut() {
                    if let (true, Value::I64(c)) = (k == "count", &mut *v) {
                        *c += 1;
                    }
                }
                t
            }
            None => {
                let mut t: Table = vec![
                    ("count".into(), Value::I64(1)),
                    ("exchange".into(), s(&msg.exchange)),
                    ("queue".into(), s(qn)),
                    ("reason".into(), s(reason)),
                    (
                        "routing-keys".into(),
                        Value::Array(vec![s(&msg.routing_key)]),
                    ),
                    ("time".into(), Value::Timestamp(now_secs())),
                ];
                if let (true, Some(exp)) = (reason == "expired", &props.expiration) {
                    t.push(("original-expiration".into(), s(exp)));
                }
                t
            }
        };
        deaths.insert(0, entry);
        if !headers.iter().any(|(k, _)| k == "x-first-death-queue") {
            headers.push(("x-first-death-exchange".into(), s(&msg.exchange)));
            headers.push(("x-first-death-queue".into(), s(qn)));
            headers.push(("x-first-death-reason".into(), s(reason)));
        }
        headers.push((
            "x-death".into(),
            Value::Array(deaths.iter().cloned().map(Value::Table).collect()),
        ));
        props.expiration = None;
        props.headers = Some(headers.clone());
        let m = Arc::new(Msg {
            exchange: dlx.clone(),
            routing_key: rk.clone(),
            headers,
            props: props.encode(),
            body: msg.body.clone(),
            persistent: msg.persistent,
            ttl_ms: None,
        });
        let mut qs = Vec::new();
        self.route(&dlx, &rk, &m.headers, &mut qs, &mut HashSet::new());
        // RabbitMQ drops a dead-lettered message that would return to a queue it already
        // died in, unless it was ever rejected (this breaks expiry/overflow cycles).
        let rejected_before = deaths
            .iter()
            .any(|t| field(t, "reason") == Some(s("rejected")));
        for q2 in qs {
            if !rejected_before && deaths.iter().any(|t| field(t, "queue") == Some(s(&q2))) {
                continue;
            }
            if self.enqueue(&q2, &m) {
                self.dispatch(&q2);
            }
        }
    }

    /// Dead-letter (or drop) messages at the head of a queue whose TTL has passed.
    fn expire_head(&mut self, qn: &str) {
        loop {
            let now = Instant::now() + self.clock_skew;
            let Some(q) = self.queues.get_mut(qn) else {
                return;
            };
            let expired = q
                .msgs
                .front()
                .is_some_and(|e| e.expires_at.is_some_and(|t| t <= now));
            if !expired {
                return;
            }
            let e = q.msgs.pop_front().unwrap();
            if e.id != 0 {
                let id = e.id;
                self.persist(|| Op::Remove(vec![id]));
            }
            self.dead_letter(qn, e.msg, "expired");
        }
    }

    /// Chaos: move the TTL clock forward and expire whatever is now due.
    pub fn advance_clock(&mut self, ms: u64) {
        self.clock_skew += Duration::from_millis(ms);
        self.sweep();
    }

    /// Chaos: expire every ready message in a queue right now (dead-lettering if configured).
    /// Returns how many were expired, or None if the queue doesn't exist.
    pub fn expire_all(&mut self, name: &str) -> Option<usize> {
        let now = Instant::now() + self.clock_skew;
        let q = self.queues.get_mut(name)?;
        let before = q.msgs.len();
        for e in q.msgs.iter_mut() {
            e.expires_at = Some(now);
        }
        self.expire_head(name);
        Some(before - self.queues.get(name).map_or(0, |q| q.msgs.len()))
    }

    /// Expire due messages in every queue; called periodically.
    pub fn sweep(&mut self) {
        let names: Vec<String> = self
            .queues
            .iter()
            .filter(|(_, q)| q.msgs.front().is_some_and(|e| e.expires_at.is_some()))
            .map(|(n, _)| n.clone())
            .collect();
        for n in names {
            self.expire_head(&n);
        }
    }
}

// ---- management snapshots (JSON) -------------------------------------------------

impl Broker {
    fn unacked_by_queue(&self) -> HashMap<&str, usize> {
        let mut m = HashMap::new();
        for c in self.chans.values() {
            for (qn, _) in c.unacked.values() {
                *m.entry(qn.as_str()).or_insert(0) += 1;
            }
        }
        m
    }

    pub fn json_overview(&self) -> String {
        let unacked: usize = self.unacked_by_queue().values().sum();
        let ready: usize = self.queues.values().map(|q| q.msgs.len()).sum();
        let consumers: usize = self.queues.values().map(|q| q.consumers.len()).sum();
        format!(
            "{{\"product\":\"RabbitSim\",\"version\":{},\"storage\":{},\"queues\":{},\"exchanges\":{},\"connections\":{},\"channels\":{},\"consumers\":{},\"messages_ready\":{},\"messages_unacknowledged\":{}}}",
            esc(env!("CARGO_PKG_VERSION")),
            self.storage.as_deref().map_or("null".to_string(), esc),
            self.queues.len(),
            self.exchanges.len(),
            self.conns.len(),
            self.chans.len(),
            consumers,
            ready,
            unacked
        )
    }

    pub fn json_queues(&self) -> String {
        let unacked = self.unacked_by_queue();
        let mut names: Vec<&String> = self.queues.keys().collect();
        names.sort();
        let items: Vec<String> = names
            .into_iter()
            .map(|n| {
                let q = &self.queues[n];
                format!(
                    "{{\"name\":{},\"durable\":{},\"auto_delete\":{},\"exclusive\":{},\"messages_ready\":{},\"messages_unacknowledged\":{},\"consumers\":{}}}",
                    esc(n),
                    q.durable,
                    q.auto_delete,
                    q.exclusive.is_some(),
                    q.msgs.len(),
                    unacked.get(n.as_str()).copied().unwrap_or(0),
                    q.consumers.len()
                )
            })
            .collect();
        format!("[{}]", items.join(","))
    }

    pub fn json_exchanges(&self) -> String {
        let mut names: Vec<&String> = self.exchanges.keys().collect();
        names.sort();
        let items: Vec<String> = names
            .into_iter()
            .map(|n| {
                let e = &self.exchanges[n];
                format!(
                    "{{\"name\":{},\"type\":{},\"durable\":{},\"auto_delete\":{},\"bindings\":{}}}",
                    esc(n),
                    esc(e.kind.name()),
                    e.durable,
                    e.auto_delete,
                    e.bindings.len()
                )
            })
            .collect();
        format!("[{}]", items.join(","))
    }

    pub fn json_bindings(&self) -> String {
        let mut names: Vec<&String> = self.exchanges.keys().collect();
        names.sort();
        let mut items = Vec::new();
        for n in names {
            for b in &self.exchanges[n].bindings {
                let (d, t) = match &b.dest {
                    Dest::Queue(q) => (q, "queue"),
                    Dest::Exchange(x) => (x, "exchange"),
                };
                items.push(format!(
                    "{{\"source\":{},\"destination\":{},\"destination_type\":\"{t}\",\"routing_key\":{}}}",
                    esc(n),
                    esc(d),
                    esc(&b.key)
                ));
            }
        }
        format!("[{}]", items.join(","))
    }

    pub fn json_connections(&self) -> String {
        let mut ids: Vec<&ConnId> = self.conns.keys().collect();
        ids.sort();
        let items: Vec<String> = ids
            .into_iter()
            .map(|id| {
                let c = &self.conns[id];
                let channels = self.chans.keys().filter(|(cid, _)| cid == id).count();
                format!(
                    "{{\"id\":{id},\"peer\":{},\"channels\":{channels},\"frame_max\":{}}}",
                    esc(&c.peer),
                    c.frame_max
                )
            })
            .collect();
        format!("[{}]", items.join(","))
    }

    /// Admin purge (no connection ownership check). Returns None if the queue is missing.
    pub fn purge_admin(&mut self, name: &str) -> Option<u32> {
        self.queues
            .contains_key(name)
            .then(|| self.purge_ready(name))
    }
}
