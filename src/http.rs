//! Minimal management HTTP server: JSON API plus a single embedded HTML page.

use crate::conn::Shared;
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const UI: &str = include_str!("ui.html");

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let hex = std::str::from_utf8(&b[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok());
            if let Some(v) = hex {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn respond(status: &str, ctype: &str, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn json(body: &str) -> Vec<u8> {
    respond("200 OK", "application/json", body)
}

fn route(method: &str, path: &str, broker: &Shared) -> Vec<u8> {
    let path = path.split('?').next().unwrap_or("");
    let path = path.trim_end_matches('/');
    match (method, path) {
        ("GET", "") | ("GET", "/index.html") => respond("200 OK", "text/html; charset=utf-8", UI),
        ("GET", "/api/overview") => json(&broker.lock().unwrap().json_overview()),
        ("GET", "/api/queues") => json(&broker.lock().unwrap().json_queues()),
        ("GET", "/api/exchanges") => json(&broker.lock().unwrap().json_exchanges()),
        ("GET", "/api/bindings") => json(&broker.lock().unwrap().json_bindings()),
        ("GET", "/api/connections") => json(&broker.lock().unwrap().json_connections()),
        // DELETE /api/queues/{vhost}/{name}/contents  -> purge
        ("DELETE", p) if p.starts_with("/api/queues/") && p.ends_with("/contents") => {
            let mid = &p["/api/queues/".len()..p.len() - "/contents".len()];
            let name = percent_decode(mid.split_once('/').map_or(mid, |(_, n)| n));
            match broker.lock().unwrap().purge_admin(&name) {
                Some(n) => json(&format!("{{\"purged\":{n}}}")),
                None => respond("404 Not Found", "application/json", "{\"error\":\"no such queue\"}"),
            }
        }
        _ => respond("404 Not Found", "application/json", "{\"error\":\"not found\"}"),
    }
}

pub async fn serve(sock: TcpStream, broker: Shared) -> io::Result<()> {
    let (rd, mut wr) = sock.into_split();
    let mut rd = BufReader::new(rd);
    let mut line = String::new();
    rd.read_line(&mut line).await?;
    let mut parts = line.split_whitespace();
    let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let (method, path) = (method.to_string(), path.to_string());
    // drain headers
    loop {
        let mut h = String::new();
        if rd.read_line(&mut h).await? == 0 || h == "\r\n" || h == "\n" {
            break;
        }
    }
    wr.write_all(&route(&method, &path, &broker)).await?;
    wr.shutdown().await
}
