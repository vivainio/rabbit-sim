//! Optional SQLite persistence.
//!
//! The broker stays authoritative in memory; every durable change is sent as an `Op`
//! to a background thread which applies them in batched transactions. On startup the
//! tables are loaded back into the broker.

use crate::broker::Msg;
use crate::codec::{Table, R, W};
use rusqlite::{params, Connection, Transaction};
use std::sync::{mpsc, Arc};
use std::time::Duration;

const SCHEMA: &str = "
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS exchanges(name TEXT PRIMARY KEY, kind TEXT NOT NULL, auto_delete INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS queues(name TEXT PRIMARY KEY, auto_delete INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS bindings(
    source TEXT NOT NULL, dest TEXT NOT NULL, dest_is_queue INTEGER NOT NULL,
    key TEXT NOT NULL, args BLOB NOT NULL,
    PRIMARY KEY(source, dest, dest_is_queue, key, args));
CREATE TABLE IF NOT EXISTS messages(
    id INTEGER PRIMARY KEY, queue TEXT NOT NULL, exchange TEXT NOT NULL,
    routing_key TEXT NOT NULL, props BLOB NOT NULL, body BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS messages_queue ON messages(queue);
";

pub struct BindRow {
    pub source: String,
    pub dest: String,
    pub dest_is_queue: bool,
    pub key: String,
    pub args: Vec<u8>,
}

pub enum Op {
    Exchange {
        name: String,
        kind: String,
        auto_delete: bool,
    },
    DeleteExchange(String),
    Queue {
        name: String,
        auto_delete: bool,
    },
    DeleteQueue(String),
    Bind(BindRow),
    Unbind(BindRow),
    Enqueue {
        id: i64,
        queue: String,
        msg: Arc<Msg>,
    },
    Remove(Vec<i64>),
    Flush(mpsc::Sender<()>),
}

pub struct StoredMsg {
    pub id: i64,
    pub queue: String,
    pub exchange: String,
    pub routing_key: String,
    pub props: Vec<u8>,
    pub body: Vec<u8>,
}

#[derive(Default)]
pub struct Loaded {
    pub exchanges: Vec<(String, String, bool)>,
    pub queues: Vec<(String, bool)>,
    pub bindings: Vec<BindRow>,
    pub messages: Vec<StoredMsg>,
}

#[derive(Clone)]
pub struct Store {
    tx: mpsc::Sender<Op>,
}

pub fn encode_args(t: &Table) -> Vec<u8> {
    let mut w = W::default();
    w.table(t);
    w.0
}

pub fn decode_args(b: &[u8]) -> Table {
    R::new(b).table().unwrap_or_default()
}

fn err(e: rusqlite::Error) -> String {
    e.to_string()
}

impl Store {
    pub fn open(path: &str) -> Result<(Store, Loaded), String> {
        let db = Connection::open(path).map_err(|e| format!("cannot open {path}: {e}"))?;
        db.execute_batch(SCHEMA).map_err(err)?;
        let loaded = load(&db).map_err(err)?;
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("rabbit-sim-store".into())
            .spawn(move || writer(db, rx))
            .map_err(|e| e.to_string())?;
        Ok((Store { tx }, loaded))
    }

    pub fn send(&self, op: Op) {
        let _ = self.tx.send(op);
    }

    /// Block until everything queued so far is committed to disk.
    pub fn flush(&self) {
        let (t, r) = mpsc::channel();
        if self.tx.send(Op::Flush(t)).is_ok() {
            let _ = r.recv_timeout(Duration::from_secs(10));
        }
    }
}

fn load(db: &Connection) -> rusqlite::Result<Loaded> {
    let exchanges = db
        .prepare("SELECT name, kind, auto_delete FROM exchanges")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)? != 0)))?
        .collect::<Result<_, _>>()?;
    let queues = db
        .prepare("SELECT name, auto_delete FROM queues")?
        .query_map([], |r| Ok((r.get(0)?, r.get::<_, i64>(1)? != 0)))?
        .collect::<Result<_, _>>()?;
    let bindings = db
        .prepare("SELECT source, dest, dest_is_queue, key, args FROM bindings")?
        .query_map([], |r| {
            Ok(BindRow {
                source: r.get(0)?,
                dest: r.get(1)?,
                dest_is_queue: r.get::<_, i64>(2)? != 0,
                key: r.get(3)?,
                args: r.get(4)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    let messages = db
        .prepare("SELECT id, queue, exchange, routing_key, props, body FROM messages ORDER BY id")?
        .query_map([], |r| {
            Ok(StoredMsg {
                id: r.get(0)?,
                queue: r.get(1)?,
                exchange: r.get(2)?,
                routing_key: r.get(3)?,
                props: r.get(4)?,
                body: r.get(5)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(Loaded {
        exchanges,
        queues,
        bindings,
        messages,
    })
}

fn writer(mut db: Connection, rx: mpsc::Receiver<Op>) {
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        while batch.len() < 2000 {
            match rx.try_recv() {
                Ok(op) => batch.push(op),
                Err(_) => break,
            }
        }
        let mut flushes = Vec::new();
        let ops: Vec<Op> = batch
            .into_iter()
            .filter_map(|op| match op {
                Op::Flush(t) => {
                    flushes.push(t);
                    None
                }
                op => Some(op),
            })
            .collect();
        if !ops.is_empty() {
            match db.transaction() {
                Ok(tx) => {
                    for op in ops {
                        if let Err(e) = apply(&tx, op) {
                            eprintln!("store error: {e}");
                        }
                    }
                    if let Err(e) = tx.commit() {
                        eprintln!("store commit error: {e}");
                    }
                }
                Err(e) => eprintln!("store error: {e}"),
            }
        }
        for f in flushes {
            let _ = f.send(());
        }
    }
}

fn apply(tx: &Transaction, op: Op) -> rusqlite::Result<()> {
    match op {
        Op::Exchange {
            name,
            kind,
            auto_delete,
        } => {
            tx.prepare_cached(
                "INSERT OR REPLACE INTO exchanges(name, kind, auto_delete) VALUES(?1, ?2, ?3)",
            )?
            .execute(params![name, kind, auto_delete])?;
        }
        Op::DeleteExchange(name) => {
            tx.prepare_cached("DELETE FROM exchanges WHERE name = ?1")?
                .execute([&name])?;
            tx.prepare_cached(
                "DELETE FROM bindings WHERE source = ?1 OR (dest = ?1 AND dest_is_queue = 0)",
            )?
            .execute([&name])?;
        }
        Op::Queue { name, auto_delete } => {
            tx.prepare_cached("INSERT OR REPLACE INTO queues(name, auto_delete) VALUES(?1, ?2)")?
                .execute(params![name, auto_delete])?;
        }
        Op::DeleteQueue(name) => {
            tx.prepare_cached("DELETE FROM queues WHERE name = ?1")?
                .execute([&name])?;
            tx.prepare_cached("DELETE FROM messages WHERE queue = ?1")?
                .execute([&name])?;
            tx.prepare_cached("DELETE FROM bindings WHERE dest = ?1 AND dest_is_queue = 1")?
                .execute([&name])?;
        }
        Op::Bind(b) => {
            tx.prepare_cached("INSERT OR IGNORE INTO bindings(source, dest, dest_is_queue, key, args) VALUES(?1, ?2, ?3, ?4, ?5)")?
                .execute(params![b.source, b.dest, b.dest_is_queue, b.key, b.args])?;
        }
        Op::Unbind(b) => {
            tx.prepare_cached("DELETE FROM bindings WHERE source = ?1 AND dest = ?2 AND dest_is_queue = ?3 AND key = ?4 AND args = ?5")?
                .execute(params![b.source, b.dest, b.dest_is_queue, b.key, b.args])?;
        }
        Op::Enqueue { id, queue, msg } => {
            tx.prepare_cached("INSERT OR REPLACE INTO messages(id, queue, exchange, routing_key, props, body) VALUES(?1, ?2, ?3, ?4, ?5, ?6)")?
                .execute(params![id, queue, msg.exchange, msg.routing_key, msg.props, msg.body])?;
        }
        Op::Remove(ids) => {
            let mut st = tx.prepare_cached("DELETE FROM messages WHERE id = ?1")?;
            for id in ids {
                st.execute([id])?;
            }
        }
        Op::Flush(_) => {}
    }
    Ok(())
}
