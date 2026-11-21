use tokio::net::TcpListener;

const USAGE: &str = "rabbit-sim - in-memory AMQP 0-9-1 broker for local development

USAGE: rabbit-sim [--bind ADDR] [--port PORT] [--http-port PORT] [--data FILE] [--rabbitmq 3|4] [-v]

  -b, --bind ADDR   address to listen on (default 127.0.0.1)
  -p, --port PORT   AMQP port (default 5672)
  -m, --http-port PORT  management UI/API port, 0 to disable (default 15672)
  -d, --data FILE   persist durable state to a SQLite file (default: in-memory only)
  -r, --rabbitmq N  emulate RabbitMQ N's stricter behaviour: 4 refuses transient queues that are not
                    exclusive, as RabbitMQ 4 does by default (default 3, which accepts them)
  -v, --verbose     log connections and protocol errors
";

#[tokio::main]
async fn main() {
    let mut bind = "127.0.0.1".to_string();
    let mut port = "5672".to_string();
    let mut http_port = "15672".to_string();
    let mut data: Option<String> = None;
    let mut verbose = false;
    let mut rabbitmq = 3u8;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "-b" | "--bind" => bind = args.next().unwrap_or_default(),
            "-p" | "--port" => port = args.next().unwrap_or_default(),
            "-m" | "--http-port" => http_port = args.next().unwrap_or_default(),
            "-d" | "--data" => data = args.next(),
            "-r" | "--rabbitmq" => match args.next().as_deref() {
                Some("3") => rabbitmq = 3,
                Some("4") => rabbitmq = 4,
                _ => {
                    eprint!("{USAGE}");
                    std::process::exit(2);
                }
            },
            "-v" | "--verbose" => verbose = true,
            _ => {
                eprint!("{USAGE}");
                std::process::exit(if a == "-h" || a == "--help" { 0 } else { 2 });
            }
        }
    }
    let addr = format!("{bind}:{port}");
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("cannot bind {addr}: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("rabbit-sim listening on amqp://{addr}");
    let broker = match &data {
        None => rabbit_sim::new_broker(),
        Some(path) => match rabbit_sim::open_broker(path) {
            Ok(b) => {
                let (q, m) = b.lock().unwrap().counts();
                eprintln!("persistence: {path} ({q} queues, {m} messages restored)");
                b
            }
            Err(e) => {
                eprintln!("cannot open data file: {e}");
                std::process::exit(1);
            }
        },
    };
    broker.lock().unwrap().set_rabbitmq_major(rabbitmq);
    if http_port != "0" {
        let haddr = format!("{bind}:{http_port}");
        match TcpListener::bind(&haddr).await {
            Ok(l) => {
                eprintln!("management UI on http://{haddr}/");
                tokio::spawn(rabbit_sim::serve_http(l, broker.clone()));
            }
            Err(e) => eprintln!("management UI disabled, cannot bind {haddr}: {e}"),
        }
    }
    let shutdown = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = signal(SignalKind::terminate()).expect("signal");
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    };
    tokio::select! {
        _ = rabbit_sim::serve_amqp(listener, broker.clone(), verbose) => {}
        _ = shutdown => eprintln!("shutting down"),
    }
    rabbit_sim::flush(&broker);
}
