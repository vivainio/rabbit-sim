pub mod broker;
pub mod codec;
pub mod conn;
pub mod http;
pub mod store;

use conn::Shared;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

pub fn new_broker() -> Shared {
    Arc::new(Mutex::new(broker::Broker::new()))
}

/// Broker persisted to the SQLite file at `path` (state is restored if the file exists).
pub fn open_broker(path: &str) -> Result<Shared, String> {
    Ok(Arc::new(Mutex::new(broker::Broker::open(path)?)))
}

/// Block until all pending writes are committed (no-op for in-memory brokers).
pub fn flush(broker: &Shared) {
    let store = broker.lock().unwrap().store();
    if let Some(s) = store {
        s.flush();
    }
}

/// Accept AMQP connections on `listener` forever.
pub async fn serve_amqp(listener: TcpListener, broker: Shared, verbose: bool) {
    loop {
        let Ok((sock, _)) = listener.accept().await else { continue };
        let b = broker.clone();
        tokio::spawn(async move {
            let _ = conn::serve(sock, b, verbose).await;
        });
    }
}

/// Serve the management API and web UI on `listener` forever.
pub async fn serve_http(listener: TcpListener, broker: Shared) {
    loop {
        let Ok((sock, _)) = listener.accept().await else { continue };
        let b = broker.clone();
        tokio::spawn(async move {
            let _ = http::serve(sock, b).await;
        });
    }
}

/// AMQP only, with a fresh broker (handy for tests).
pub async fn serve(listener: TcpListener, verbose: bool) {
    serve_amqp(listener, new_broker(), verbose).await
}
