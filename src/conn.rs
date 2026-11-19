//! Per-connection protocol handling: handshake, frame loop, method dispatch.

use crate::broker::{Broker, ConnId, Dest, Msg, Tx};
use crate::codec::*;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::{interval_at, timeout, Instant};

const MAX_FRAME: u32 = 131072;
const CHANNEL_MAX: u16 = 2047;

pub type Shared = Arc<Mutex<Broker>>;

fn bad(msg: &str) -> io::Error {
    io::Error::other(msg.to_string())
}

fn server_properties() -> Table {
    let s = |v: &str| Value::Str(v.as_bytes().to_vec());
    let caps: Table = [
        "publisher_confirms",
        "basic.nack",
        "consumer_cancel_notify",
        "exchange_exchange_bindings",
    ]
    .iter()
    .map(|k| (k.to_string(), Value::Bool(true)))
    .collect();
    vec![
        ("product".into(), s("RabbitSim")),
        ("version".into(), s(env!("CARGO_PKG_VERSION"))),
        ("platform".into(), s("Rust")),
        ("capabilities".into(), Value::Table(caps)),
    ]
}

struct Pending {
    exchange: String,
    rk: String,
    mandatory: bool,
    props: Option<Vec<u8>>,
    headers: Table,
    persistent: bool,
    size: u64,
    body: Vec<u8>,
}

struct ChState {
    pending: Option<Pending>,
    closing: bool,
}

struct Conn {
    id: ConnId,
    broker: Shared,
    tx: Tx,
    channels: HashMap<u16, ChState>,
    last_queue: String,
    cur: (u16, u16),
}

pub async fn serve(sock: TcpStream, broker: Shared, verbose: bool) -> io::Result<()> {
    let peer = sock.peer_addr().ok();
    sock.set_nodelay(true)?;
    let (rd, mut wr) = sock.into_split();
    let mut rd = BufReader::new(rd);

    let mut hdr = [0u8; 8];
    tokio::io::AsyncReadExt::read_exact(&mut rd, &mut hdr).await?;
    if &hdr != b"AMQP\x00\x00\x09\x01" {
        wr.write_all(b"AMQP\x00\x00\x09\x01").await?;
        return Ok(());
    }

    // connection.start
    wr.write_all(&method(0, 10, 10, |w| {
        w.u8(0);
        w.u8(9);
        w.table(&server_properties());
        w.long(b"PLAIN AMQPLAIN");
        w.long(b"en_US");
    }))
    .await?;

    // start-ok
    let f = read_frame(&mut rd).await?;
    let mut r = R::new(&f.payload);
    if f.ty != FRAME_METHOD
        || (
            r.u16().map_err(|_| bad("frame"))?,
            r.u16().map_err(|_| bad("frame"))?,
        ) != (10, 11)
    {
        return Err(bad("expected connection.start-ok"));
    }

    // tune
    wr.write_all(&method(0, 10, 30, |w| {
        w.u16(CHANNEL_MAX);
        w.u32(MAX_FRAME);
        w.u16(60);
    }))
    .await?;
    let f = read_frame(&mut rd).await?;
    let mut r = R::new(&f.payload);
    let (c, m) = (
        r.u16().map_err(|_| bad("frame"))?,
        r.u16().map_err(|_| bad("frame"))?,
    );
    if f.ty != FRAME_METHOD || (c, m) != (10, 31) {
        return Err(bad("expected connection.tune-ok"));
    }
    let _chmax = r.u16().map_err(|_| bad("frame"))?;
    let fm = r.u32().map_err(|_| bad("frame"))?;
    let heartbeat = r.u16().map_err(|_| bad("frame"))?;
    let frame_max = if fm == 0 || fm > MAX_FRAME {
        MAX_FRAME
    } else {
        fm.max(4096)
    } as usize;

    // connection.open
    let f = read_frame(&mut rd).await?;
    let mut r = R::new(&f.payload);
    let (c, m) = (
        r.u16().map_err(|_| bad("frame"))?,
        r.u16().map_err(|_| bad("frame"))?,
    );
    if f.ty != FRAME_METHOD || (c, m) != (10, 40) {
        return Err(bad("expected connection.open"));
    }

    let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
    let peer_s = peer.map_or("?".to_string(), |p| p.to_string());
    let id = broker
        .lock()
        .unwrap()
        .register(tx.clone(), frame_max, peer_s.clone());
    if verbose {
        eprintln!("[{id}] connected from {peer_s}");
    }

    // Writer task: owns the write half, also emits heartbeats.
    tokio::spawn(async move {
        let period = Duration::from_secs(if heartbeat > 0 {
            (heartbeat as u64 / 2).max(1)
        } else {
            3600
        });
        let mut tick = interval_at(Instant::now() + period, period);
        loop {
            tokio::select! {
                m = rx.recv() => match m {
                    Some(b) => if wr.write_all(&b).await.is_err() { break },
                    None => break,
                },
                _ = tick.tick() => {
                    if heartbeat > 0 && wr.write_all(&frame(FRAME_HEARTBEAT, 0, &[])).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = wr.shutdown().await;
    });

    let _ = tx.send(method(0, 10, 41, |w| w.short("")));

    let mut conn = Conn {
        id,
        broker: broker.clone(),
        tx,
        channels: HashMap::new(),
        last_queue: String::new(),
        cur: (0, 0),
    };

    loop {
        let f = if heartbeat > 0 {
            match timeout(
                Duration::from_secs(heartbeat as u64 * 2),
                read_frame(&mut rd),
            )
            .await
            {
                Ok(r) => r,
                Err(_) => break,
            }
        } else {
            read_frame(&mut rd).await
        };
        let Ok(f) = f else { break };
        let ch = f.ch;
        match conn.handle(f) {
            Ok(true) => break,
            Ok(false) => {}
            Err(e) => {
                if verbose {
                    eprintln!("[{id}] error: {}", e.text);
                }
                let (class, m) = conn.cur;
                if e.hard {
                    let _ = conn.tx.send(method(0, 10, 50, |w| {
                        w.u16(e.code);
                        w.short(&e.text);
                        w.u16(class);
                        w.u16(m);
                    }));
                    break;
                }
                conn.tx
                    .send(method(ch, 20, 40, |w| {
                        w.u16(e.code);
                        w.short(&e.text);
                        w.u16(class);
                        w.u16(m);
                    }))
                    .ok();
                conn.broker.lock().unwrap().close_chan(id, ch);
                if let Some(st) = conn.channels.get_mut(&ch) {
                    st.closing = true;
                    st.pending = None;
                }
            }
        }
    }
    broker.lock().unwrap().close_conn(id);
    // Let the client hang up first so it can read close/close-ok before EOF.
    let _ = timeout(Duration::from_secs(1), async {
        while read_frame(&mut rd).await.is_ok() {}
    })
    .await;
    if verbose {
        eprintln!("[{id}] disconnected");
    }
    Ok(())
}

impl Conn {
    fn send(&self, b: Vec<u8>) {
        let _ = self.tx.send(b);
    }

    fn handle(&mut self, f: Frame) -> Res<bool> {
        match f.ty {
            FRAME_HEARTBEAT => Ok(false),
            FRAME_METHOD => self.on_method(f),
            FRAME_HEADER => {
                let Some(st) = self.channels.get_mut(&f.ch) else {
                    return Err(AmqpErr::conn(
                        CHANNEL_ERROR,
                        "CHANNEL_ERROR - unexpected content header",
                    ));
                };
                if st.closing {
                    return Ok(false);
                }
                let Some(p) = st.pending.as_mut().filter(|p| p.props.is_none()) else {
                    return Err(AmqpErr::conn(
                        UNEXPECTED_FRAME,
                        "UNEXPECTED_FRAME - unexpected content header",
                    ));
                };
                let mut r = R::new(&f.payload);
                let _class = r.u16()?;
                let _weight = r.u16()?;
                p.size = r.u64()?;
                let props = r.rest().to_vec();
                let (headers, mode) = props_parse(&props)?;
                p.headers = headers;
                p.persistent = mode == 2;
                p.props = Some(props);
                if p.size == 0 {
                    self.finish_publish(f.ch)?;
                }
                Ok(false)
            }
            FRAME_BODY => {
                let Some(st) = self.channels.get_mut(&f.ch) else {
                    return Err(AmqpErr::conn(
                        CHANNEL_ERROR,
                        "CHANNEL_ERROR - unexpected content body",
                    ));
                };
                if st.closing {
                    return Ok(false);
                }
                let Some(p) = st.pending.as_mut().filter(|p| p.props.is_some()) else {
                    return Err(AmqpErr::conn(
                        UNEXPECTED_FRAME,
                        "UNEXPECTED_FRAME - unexpected content body",
                    ));
                };
                p.body.extend_from_slice(&f.payload);
                if p.body.len() as u64 > p.size {
                    return Err(AmqpErr::conn(
                        FRAME_ERROR,
                        "FRAME_ERROR - content body larger than declared",
                    ));
                }
                if p.body.len() as u64 == p.size {
                    self.finish_publish(f.ch)?;
                }
                Ok(false)
            }
            _ => Err(AmqpErr::conn(
                FRAME_ERROR,
                "FRAME_ERROR - unknown frame type",
            )),
        }
    }

    fn finish_publish(&mut self, ch: u16) -> Res<()> {
        let p = self
            .channels
            .get_mut(&ch)
            .and_then(|s| s.pending.take())
            .unwrap();
        let msg = Arc::new(Msg {
            exchange: p.exchange.clone(),
            routing_key: p.rk.clone(),
            headers: p.headers,
            props: p.props.unwrap_or_default(),
            body: p.body,
            persistent: p.persistent,
        });
        self.broker
            .lock()
            .unwrap()
            .publish(self.id, ch, &p.exchange, &p.rk, p.mandatory, msg)
    }

    fn queue_name(&self, q: String) -> String {
        if q.is_empty() {
            self.last_queue.clone()
        } else {
            q
        }
    }

    fn on_method(&mut self, f: Frame) -> Res<bool> {
        let ch = f.ch;
        let mut r = R::new(&f.payload);
        let class = r.u16()?;
        let m = r.u16()?;
        self.cur = (class, m);

        if class == 10 {
            if ch != 0 {
                return Err(AmqpErr::conn(
                    COMMAND_INVALID,
                    "COMMAND_INVALID - connection method on non-zero channel",
                ));
            }
            return match m {
                50 => {
                    self.send(method(0, 10, 51, |_| {}));
                    Ok(true)
                }
                51 => Ok(true),
                _ => Err(AmqpErr::conn(
                    COMMAND_INVALID,
                    "COMMAND_INVALID - unexpected connection method",
                )),
            };
        }
        if ch == 0 {
            return Err(AmqpErr::conn(
                CHANNEL_ERROR,
                "CHANNEL_ERROR - method on channel 0",
            ));
        }

        // channel lifecycle
        if (class, m) == (20, 10) {
            if self.channels.contains_key(&ch) || ch > CHANNEL_MAX {
                return Err(AmqpErr::conn(
                    CHANNEL_ERROR,
                    "CHANNEL_ERROR - second 'channel.open' seen",
                ));
            }
            self.channels.insert(
                ch,
                ChState {
                    pending: None,
                    closing: false,
                },
            );
            self.broker.lock().unwrap().open_chan(self.id, ch);
            self.send(method(ch, 20, 11, |w| w.long(b"")));
            return Ok(false);
        }
        let Some(st) = self.channels.get_mut(&ch) else {
            return Err(AmqpErr::conn(
                CHANNEL_ERROR,
                "CHANNEL_ERROR - expected 'channel.open'",
            ));
        };
        if st.closing {
            match (class, m) {
                (20, 41) => {
                    self.channels.remove(&ch);
                }
                (20, 40) => {
                    self.send(method(ch, 20, 41, |_| {}));
                    self.channels.remove(&ch);
                }
                _ => {}
            }
            return Ok(false);
        }

        match (class, m) {
            (20, 40) => {
                self.broker.lock().unwrap().close_chan(self.id, ch);
                self.channels.remove(&ch);
                self.send(method(ch, 20, 41, |_| {}));
            }
            (20, 20) => {
                let active = r.u8()?;
                self.send(method(ch, 20, 21, |w| w.u8(active)));
            }
            (20, 41) => {}

            // exchange
            (40, 10) => {
                r.u16()?;
                let name = r.short()?;
                let kind = r.short()?;
                let bits = r.u8()?;
                self.broker.lock().unwrap().declare_exchange(
                    &name,
                    &kind,
                    bits & 1 != 0,
                    bits & 2 != 0,
                    bits & 4 != 0,
                )?;
                if bits & 16 == 0 {
                    self.send(method(ch, 40, 11, |_| {}));
                }
            }
            (40, 20) => {
                r.u16()?;
                let name = r.short()?;
                let bits = r.u8()?;
                self.broker
                    .lock()
                    .unwrap()
                    .delete_exchange(&name, bits & 1 != 0)?;
                if bits & 2 == 0 {
                    self.send(method(ch, 40, 21, |_| {}));
                }
            }
            (40, 30) | (40, 40) => {
                r.u16()?;
                let dest = r.short()?;
                let src = r.short()?;
                let key = r.short()?;
                let bits = r.u8()?;
                let args = r.table()?;
                let mut b = self.broker.lock().unwrap();
                if m == 30 {
                    b.bind(Dest::Exchange(dest), &src, &key, args)?;
                } else {
                    b.unbind(Dest::Exchange(dest), &src, &key, &args)?;
                }
                drop(b);
                if bits & 1 == 0 {
                    self.send(method(ch, 40, m + if m == 30 { 1 } else { 11 }, |_| {}));
                }
            }

            // queue
            (50, 10) => {
                r.u16()?;
                let name = r.short()?;
                let bits = r.u8()?;
                let (name, msgs, cons) = self.broker.lock().unwrap().declare_queue(
                    self.id,
                    &name,
                    bits & 1 != 0,
                    bits & 2 != 0,
                    bits & 4 != 0,
                    bits & 8 != 0,
                )?;
                self.last_queue = name.clone();
                if bits & 16 == 0 {
                    self.send(method(ch, 50, 11, |w| {
                        w.short(&name);
                        w.u32(msgs);
                        w.u32(cons);
                    }));
                }
            }
            (50, 20) | (50, 50) => {
                r.u16()?;
                let q = self.queue_name(r.short()?);
                let ex = r.short()?;
                let key = r.short()?;
                let nowait = if m == 20 { r.u8()? & 1 != 0 } else { false };
                let args = r.table()?;
                let mut b = self.broker.lock().unwrap();
                b.check_queue_access(self.id, &q)?;
                if m == 20 {
                    b.bind(Dest::Queue(q), &ex, &key, args)?;
                } else {
                    b.unbind(Dest::Queue(q), &ex, &key, &args)?;
                }
                drop(b);
                if !nowait {
                    self.send(method(ch, 50, if m == 20 { 21 } else { 51 }, |_| {}));
                }
            }
            (50, 30) => {
                r.u16()?;
                let q = self.queue_name(r.short()?);
                let nowait = r.u8()? & 1 != 0;
                let n = self.broker.lock().unwrap().purge_queue(self.id, &q)?;
                if !nowait {
                    self.send(method(ch, 50, 31, |w| w.u32(n)));
                }
            }
            (50, 40) => {
                r.u16()?;
                let q = self.queue_name(r.short()?);
                let bits = r.u8()?;
                let n = self.broker.lock().unwrap().delete_queue(
                    self.id,
                    &q,
                    bits & 1 != 0,
                    bits & 2 != 0,
                )?;
                if bits & 4 == 0 {
                    self.send(method(ch, 50, 41, |w| w.u32(n)));
                }
            }

            // basic
            (60, 10) => {
                let _size = r.u32()?;
                let count = r.u16()?;
                self.broker
                    .lock()
                    .unwrap()
                    .set_prefetch(self.id, ch, count as u32);
                self.send(method(ch, 60, 11, |_| {}));
            }
            (60, 20) => {
                r.u16()?;
                let q = self.queue_name(r.short()?);
                let tag = r.short()?;
                let bits = r.u8()?;
                let mut b = self.broker.lock().unwrap();
                let tag = b.consume(self.id, ch, &q, &tag, bits & 2 != 0, bits & 4 != 0)?;
                if bits & 8 == 0 {
                    self.send(method(ch, 60, 21, |w| w.short(&tag)));
                }
                b.dispatch(&q);
            }
            (60, 30) => {
                let tag = r.short()?;
                let nowait = r.u8()? & 1 != 0;
                self.broker.lock().unwrap().cancel(self.id, ch, &tag);
                if !nowait {
                    self.send(method(ch, 60, 31, |w| w.short(&tag)));
                }
            }
            (60, 40) => {
                r.u16()?;
                let exchange = r.short()?;
                let rk = r.short()?;
                let bits = r.u8()?;
                self.channels.get_mut(&ch).unwrap().pending = Some(Pending {
                    exchange,
                    rk,
                    mandatory: bits & 1 != 0,
                    props: None,
                    headers: Vec::new(),
                    persistent: false,
                    size: 0,
                    body: Vec::new(),
                });
            }
            (60, 70) => {
                r.u16()?;
                let q = self.queue_name(r.short()?);
                let no_ack = r.u8()? & 1 != 0;
                self.broker.lock().unwrap().get(self.id, ch, &q, no_ack)?;
            }
            (60, 80) => {
                let tag = r.u64()?;
                let multiple = r.u8()? & 1 != 0;
                self.broker
                    .lock()
                    .unwrap()
                    .settle(self.id, ch, tag, multiple, None)?;
            }
            (60, 90) => {
                let tag = r.u64()?;
                let requeue = r.u8()? & 1 != 0;
                self.broker
                    .lock()
                    .unwrap()
                    .settle(self.id, ch, tag, false, Some(requeue))?;
            }
            (60, 120) => {
                let tag = r.u64()?;
                let bits = r.u8()?;
                self.broker.lock().unwrap().settle(
                    self.id,
                    ch,
                    tag,
                    bits & 1 != 0,
                    Some(bits & 2 != 0),
                )?;
            }
            (60, 100) => {
                self.broker.lock().unwrap().recover(self.id, ch);
            }
            (60, 110) => {
                self.broker.lock().unwrap().recover(self.id, ch);
                self.send(method(ch, 60, 111, |_| {}));
            }

            // confirm
            (85, 10) => {
                let nowait = r.u8()? & 1 != 0;
                self.broker.lock().unwrap().confirm_select(self.id, ch);
                if !nowait {
                    self.send(method(ch, 85, 11, |_| {}));
                }
            }

            (90, _) => {
                return Err(AmqpErr::conn(
                    NOT_IMPLEMENTED,
                    "NOT_IMPLEMENTED - transactions (tx.*) are not supported",
                ));
            }
            _ => {
                return Err(AmqpErr::conn(
                    COMMAND_INVALID,
                    format!("COMMAND_INVALID - unimplemented method {class}.{m}"),
                ));
            }
        }
        Ok(false)
    }
}
