//! AMQP 0-9-1 wire primitives: field tables, frames, content encoding.

use std::io;
use tokio::io::{AsyncRead, AsyncReadExt};

pub type Table = Vec<(String, Value)>;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    Decimal(u8, u32),
    Str(Vec<u8>),
    Array(Vec<Value>),
    Timestamp(u64),
    Table(Table),
    Bytes(Vec<u8>),
    Void,
}

pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;
pub const FRAME_END: u8 = 0xCE;

pub const ACCESS_REFUSED: u16 = 403;
pub const NOT_FOUND: u16 = 404;
pub const RESOURCE_LOCKED: u16 = 405;
pub const PRECONDITION_FAILED: u16 = 406;
pub const FRAME_ERROR: u16 = 501;
pub const SYNTAX_ERROR: u16 = 502;
pub const COMMAND_INVALID: u16 = 503;
pub const CHANNEL_ERROR: u16 = 504;
pub const UNEXPECTED_FRAME: u16 = 505;
pub const NOT_ALLOWED: u16 = 530;
pub const NOT_IMPLEMENTED: u16 = 540;

/// An AMQP exception. `hard` errors close the connection, others only the channel.
#[derive(Debug)]
pub struct AmqpErr {
    pub code: u16,
    pub text: String,
    pub hard: bool,
}

impl AmqpErr {
    pub fn chan(code: u16, text: impl Into<String>) -> Self {
        AmqpErr {
            code,
            text: text.into(),
            hard: false,
        }
    }
    pub fn conn(code: u16, text: impl Into<String>) -> Self {
        AmqpErr {
            code,
            text: text.into(),
            hard: true,
        }
    }
}

pub type Res<T> = Result<T, AmqpErr>;

fn syntax() -> AmqpErr {
    AmqpErr::conn(SYNTAX_ERROR, "SYNTAX_ERROR - malformed frame payload")
}

pub struct R<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> R<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        R { b, p: 0 }
    }
    fn take(&mut self, n: usize) -> Res<&'a [u8]> {
        if self.p + n > self.b.len() {
            return Err(syntax());
        }
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        Ok(s)
    }
    pub fn u8(&mut self) -> Res<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Res<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> Res<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Res<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn short(&mut self) -> Res<String> {
        let n = self.u8()? as usize;
        Ok(String::from_utf8_lossy(self.take(n)?).into_owned())
    }
    pub fn long(&mut self) -> Res<Vec<u8>> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    pub fn rest(&mut self) -> &'a [u8] {
        let s = &self.b[self.p..];
        self.p = self.b.len();
        s
    }
    pub fn table(&mut self) -> Res<Table> {
        let n = self.u32()? as usize;
        let mut r = R::new(self.take(n)?);
        let mut t = Vec::new();
        while r.p < r.b.len() {
            let k = r.short()?;
            let v = r.value()?;
            t.push((k, v));
        }
        Ok(t)
    }
    pub fn value(&mut self) -> Res<Value> {
        Ok(match self.u8()? {
            b't' => Value::Bool(self.u8()? != 0),
            b'b' => Value::I8(self.u8()? as i8),
            b'B' => Value::U8(self.u8()?),
            b's' => Value::I16(self.u16()? as i16),
            b'u' => Value::U16(self.u16()?),
            b'I' => Value::I32(self.u32()? as i32),
            b'i' => Value::U32(self.u32()?),
            b'l' => Value::I64(self.u64()? as i64),
            b'f' => Value::F32(f32::from_bits(self.u32()?)),
            b'd' => Value::F64(f64::from_bits(self.u64()?)),
            b'D' => {
                let s = self.u8()?;
                Value::Decimal(s, self.u32()?)
            }
            b'S' => Value::Str(self.long()?),
            b'A' => {
                let n = self.u32()? as usize;
                let mut r = R::new(self.take(n)?);
                let mut v = Vec::new();
                while r.p < r.b.len() {
                    v.push(r.value()?);
                }
                Value::Array(v)
            }
            b'T' => Value::Timestamp(self.u64()?),
            b'F' => Value::Table(self.table()?),
            b'V' => Value::Void,
            b'x' => Value::Bytes(self.long()?),
            _ => return Err(syntax()),
        })
    }
}

#[derive(Default)]
pub struct W(pub Vec<u8>);

impl W {
    pub fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    pub fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    pub fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    pub fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    pub fn short(&mut self, s: &str) {
        let b = s.as_bytes();
        let n = b.len().min(255);
        self.u8(n as u8);
        self.0.extend_from_slice(&b[..n]);
    }
    pub fn long(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.0.extend_from_slice(b);
    }
    pub fn table(&mut self, t: &Table) {
        let at = self.0.len();
        self.u32(0);
        for (k, v) in t {
            self.short(k);
            self.value(v);
        }
        let n = (self.0.len() - at - 4) as u32;
        self.0[at..at + 4].copy_from_slice(&n.to_be_bytes());
    }
    pub fn value(&mut self, v: &Value) {
        match v {
            Value::Bool(b) => {
                self.u8(b't');
                self.u8(*b as u8)
            }
            Value::I8(x) => {
                self.u8(b'b');
                self.u8(*x as u8)
            }
            Value::U8(x) => {
                self.u8(b'B');
                self.u8(*x)
            }
            Value::I16(x) => {
                self.u8(b's');
                self.u16(*x as u16)
            }
            Value::U16(x) => {
                self.u8(b'u');
                self.u16(*x)
            }
            Value::I32(x) => {
                self.u8(b'I');
                self.u32(*x as u32)
            }
            Value::U32(x) => {
                self.u8(b'i');
                self.u32(*x)
            }
            Value::I64(x) => {
                self.u8(b'l');
                self.u64(*x as u64)
            }
            Value::F32(x) => {
                self.u8(b'f');
                self.u32(x.to_bits())
            }
            Value::F64(x) => {
                self.u8(b'd');
                self.u64(x.to_bits())
            }
            Value::Decimal(s, x) => {
                self.u8(b'D');
                self.u8(*s);
                self.u32(*x)
            }
            Value::Str(b) => {
                self.u8(b'S');
                self.long(b)
            }
            Value::Array(a) => {
                self.u8(b'A');
                let at = self.0.len();
                self.u32(0);
                for x in a {
                    self.value(x);
                }
                let n = (self.0.len() - at - 4) as u32;
                self.0[at..at + 4].copy_from_slice(&n.to_be_bytes());
            }
            Value::Timestamp(x) => {
                self.u8(b'T');
                self.u64(*x)
            }
            Value::Table(t) => {
                self.u8(b'F');
                self.table(t)
            }
            Value::Bytes(b) => {
                self.u8(b'x');
                self.long(b)
            }
            Value::Void => self.u8(b'V'),
        }
    }
}

pub struct Frame {
    pub ty: u8,
    pub ch: u16,
    pub payload: Vec<u8>,
}

pub async fn read_frame<T: AsyncRead + Unpin>(r: &mut T) -> io::Result<Frame> {
    let mut h = [0u8; 7];
    r.read_exact(&mut h).await?;
    let ty = h[0];
    let ch = u16::from_be_bytes([h[1], h[2]]);
    let size = u32::from_be_bytes([h[3], h[4], h[5], h[6]]) as usize;
    if size > 1 << 24 {
        return Err(io::Error::other("frame too large"));
    }
    let mut p = vec![0u8; size + 1];
    r.read_exact(&mut p).await?;
    if p[size] != FRAME_END {
        return Err(io::Error::other("bad frame end"));
    }
    p.truncate(size);
    Ok(Frame { ty, ch, payload: p })
}

pub fn frame(ty: u8, ch: u16, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(payload.len() + 8);
    v.push(ty);
    v.extend_from_slice(&ch.to_be_bytes());
    v.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    v.extend_from_slice(payload);
    v.push(FRAME_END);
    v
}

/// Build a complete method frame.
pub fn method(ch: u16, class: u16, m: u16, f: impl FnOnce(&mut W)) -> Vec<u8> {
    let mut w = W::default();
    w.u16(class);
    w.u16(m);
    f(&mut w);
    frame(FRAME_METHOD, ch, &w.0)
}

/// Content header frame followed by body frames (basic class).
pub fn content(ch: u16, props: &[u8], body: &[u8], frame_max: usize) -> Vec<u8> {
    let mut w = W::default();
    w.u16(60);
    w.u16(0);
    w.u64(body.len() as u64);
    w.0.extend_from_slice(props);
    let mut out = frame(FRAME_HEADER, ch, &w.0);
    let chunk = frame_max.saturating_sub(8).max(1);
    for c in body.chunks(chunk) {
        out.extend(frame(FRAME_BODY, ch, c));
    }
    out
}

/// Decoded basic properties. Re-encode with [`Props::encode`] after modifying.
#[derive(Debug, Default, Clone)]
pub struct Props {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: Option<Table>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub kind: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub cluster_id: Option<String>,
}

pub fn parse_props(b: &[u8]) -> Res<Props> {
    let mut r = R::new(b);
    let f = r.u16()?;
    let mut p = Props::default();
    let short = |bit: u16, r: &mut R| -> Res<Option<String>> {
        if f & bit != 0 {
            Ok(Some(r.short()?))
        } else {
            Ok(None)
        }
    };
    p.content_type = short(0x8000, &mut r)?;
    p.content_encoding = short(0x4000, &mut r)?;
    if f & 0x2000 != 0 {
        p.headers = Some(r.table()?);
    }
    if f & 0x1000 != 0 {
        p.delivery_mode = Some(r.u8()?);
    }
    if f & 0x0800 != 0 {
        p.priority = Some(r.u8()?);
    }
    p.correlation_id = short(0x0400, &mut r)?;
    p.reply_to = short(0x0200, &mut r)?;
    p.expiration = short(0x0100, &mut r)?;
    p.message_id = short(0x0080, &mut r)?;
    if f & 0x0040 != 0 {
        p.timestamp = Some(r.u64()?);
    }
    p.kind = short(0x0020, &mut r)?;
    p.user_id = short(0x0010, &mut r)?;
    p.app_id = short(0x0008, &mut r)?;
    p.cluster_id = short(0x0004, &mut r)?;
    Ok(p)
}

impl Props {
    pub fn encode(&self) -> Vec<u8> {
        let mut flags = 0u16;
        let mut w = W::default();
        w.u16(0);
        fn short(flags: &mut u16, bit: u16, v: &Option<String>, w: &mut W) {
            if let Some(s) = v {
                *flags |= bit;
                w.short(s);
            }
        }
        short(&mut flags, 0x8000, &self.content_type, &mut w);
        short(&mut flags, 0x4000, &self.content_encoding, &mut w);
        if let Some(h) = &self.headers {
            flags |= 0x2000;
            w.table(h);
        }
        if let Some(v) = self.delivery_mode {
            flags |= 0x1000;
            w.u8(v);
        }
        if let Some(v) = self.priority {
            flags |= 0x0800;
            w.u8(v);
        }
        short(&mut flags, 0x0400, &self.correlation_id, &mut w);
        short(&mut flags, 0x0200, &self.reply_to, &mut w);
        short(&mut flags, 0x0100, &self.expiration, &mut w);
        short(&mut flags, 0x0080, &self.message_id, &mut w);
        if let Some(v) = self.timestamp {
            flags |= 0x0040;
            w.u64(v);
        }
        short(&mut flags, 0x0020, &self.kind, &mut w);
        short(&mut flags, 0x0010, &self.user_id, &mut w);
        short(&mut flags, 0x0008, &self.app_id, &mut w);
        short(&mut flags, 0x0004, &self.cluster_id, &mut w);
        w.0[0..2].copy_from_slice(&flags.to_be_bytes());
        w.0
    }
}
