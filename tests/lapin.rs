use futures_lite::StreamExt;
use lapin::{options::*, types::*, BasicProperties, Connection, ConnectionProperties, ExchangeKind};
use std::time::Duration;
use tokio::net::TcpListener;

async fn start() -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(rabbitsim::serve(l, false));
    format!("amqp://127.0.0.1:{port}/%2f")
}

async fn connect(url: &str) -> Connection {
    Connection::connect(url, ConnectionProperties::default()).await.unwrap()
}

#[tokio::test]
async fn default_exchange_publish_consume_ack() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare("q1".into(), QueueDeclareOptions::default(), FieldTable::default()).await.unwrap();
    ch.basic_publish("".into(), "q1".into(), BasicPublishOptions::default(), b"hello", BasicProperties::default().with_content_type("text/plain".into()))
        .await.unwrap().await.unwrap();

    let mut c = ch.basic_consume("q1".into(), "me".into(), BasicConsumeOptions::default(), FieldTable::default()).await.unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), c.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(d.data, b"hello");
    assert_eq!(d.routing_key.as_str(), "q1");
    assert_eq!(d.properties.content_type().as_ref().unwrap().as_str(), "text/plain");
    d.ack(BasicAckOptions::default()).await.unwrap();
}

#[tokio::test]
async fn topic_and_fanout_routing() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.exchange_declare("logs".into(), ExchangeKind::Topic, ExchangeDeclareOptions::default(), FieldTable::default()).await.unwrap();
    ch.exchange_declare("fan".into(), ExchangeKind::Fanout, ExchangeDeclareOptions::default(), FieldTable::default()).await.unwrap();
    for (q, key) in [("errs", "*.error"), ("all", "#"), ("kern", "kern.*")] {
        ch.queue_declare(q.into(), QueueDeclareOptions::default(), FieldTable::default()).await.unwrap();
        ch.queue_bind(q.into(), "logs".into(), key.into(), QueueBindOptions::default(), FieldTable::default()).await.unwrap();
    }
    ch.queue_declare("f1".into(), QueueDeclareOptions::default(), FieldTable::default()).await.unwrap();
    ch.queue_bind("f1".into(), "fan".into(), "".into(), QueueBindOptions::default(), FieldTable::default()).await.unwrap();
    // exchange -> exchange binding
    ch.exchange_bind("fan".into(), "logs".into(), "kern.#".into(), ExchangeBindOptions::default(), FieldTable::default()).await.unwrap();

    ch.basic_publish("logs".into(), "kern.error".into(), BasicPublishOptions::default(), b"x", BasicProperties::default()).await.unwrap().await.unwrap();
    ch.basic_publish("logs".into(), "app.info".into(), BasicPublishOptions::default(), b"y", BasicProperties::default()).await.unwrap().await.unwrap();

    let count = |q: &'static str| {
        let ch = ch.clone();
        async move {
            ch.queue_declare(q.into(), QueueDeclareOptions { passive: true, ..Default::default() }, FieldTable::default())
                .await.unwrap().message_count()
        }
    };
    assert_eq!(count("errs").await, 1);
    assert_eq!(count("all").await, 2);
    assert_eq!(count("kern").await, 1);
    assert_eq!(count("f1").await, 1);
}

#[tokio::test]
async fn confirms_mandatory_get_and_nack_requeue() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default()).await.unwrap();
    ch.queue_declare("work".into(), QueueDeclareOptions::default(), FieldTable::default()).await.unwrap();

    let confirm = ch.basic_publish("".into(), "work".into(), BasicPublishOptions::default(), b"job", BasicProperties::default()).await.unwrap().await.unwrap();
    assert!(matches!(confirm, lapin::publisher_confirm::Confirmation::Ack(_)));

    // mandatory publish with no route comes back
    let confirm = ch.basic_publish("".into(), "nowhere".into(), BasicPublishOptions { mandatory: true, ..Default::default() }, b"lost", BasicProperties::default()).await.unwrap().await.unwrap();
    match confirm {
        lapin::publisher_confirm::Confirmation::Ack(Some(ret)) => assert_eq!(ret.data, b"lost"),
        other => panic!("expected returned message, got {other:?}"),
    }

    let got = ch.basic_get("work".into(), BasicGetOptions::default()).await.unwrap().unwrap();
    assert_eq!(got.data, b"job");
    got.nack(BasicNackOptions { requeue: true, ..Default::default() }).await.unwrap();
    let again = ch.basic_get("work".into(), BasicGetOptions::default()).await.unwrap().unwrap();
    assert!(again.delivery.redelivered);
    again.ack(BasicAckOptions::default()).await.unwrap();
    assert!(ch.basic_get("work".into(), BasicGetOptions::default()).await.unwrap().is_none());
}

#[tokio::test]
async fn prefetch_and_requeue_on_disconnect() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare("pq".into(), QueueDeclareOptions::default(), FieldTable::default()).await.unwrap();
    for i in 0..5u8 {
        ch.basic_publish("".into(), "pq".into(), BasicPublishOptions::default(), &[i], BasicProperties::default()).await.unwrap().await.unwrap();
    }
    let c2 = connect(&url).await;
    let ch2 = c2.create_channel().await.unwrap();
    ch2.basic_qos(2, BasicQosOptions::default()).await.unwrap();
    let mut cons = ch2.basic_consume("pq".into(), "".into(), BasicConsumeOptions::default(), FieldTable::default()).await.unwrap();
    let d1 = cons.next().await.unwrap().unwrap();
    let _d2 = cons.next().await.unwrap().unwrap();
    // third must not arrive until an ack
    assert!(tokio::time::timeout(Duration::from_millis(300), cons.next()).await.is_err());
    d1.ack(BasicAckOptions::default()).await.unwrap();
    let d3 = tokio::time::timeout(Duration::from_secs(2), cons.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(d3.data, vec![2]);

    // drop consumer connection: unacked (d2, d3) are requeued -> 4 ready
    drop(cons);
    c2.close(200, "bye".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let q = ch.queue_declare("pq".into(), QueueDeclareOptions { passive: true, ..Default::default() }, FieldTable::default()).await.unwrap();
    assert_eq!(q.message_count(), 4);
}

#[tokio::test]
async fn headers_exchange_and_errors() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.exchange_declare("h".into(), ExchangeKind::Headers, ExchangeDeclareOptions::default(), FieldTable::default()).await.unwrap();
    ch.queue_declare("hq".into(), QueueDeclareOptions::default(), FieldTable::default()).await.unwrap();
    let mut args = FieldTable::default();
    args.insert("x-match".into(), AMQPValue::LongString("all".into()));
    args.insert("fmt".into(), AMQPValue::LongString("pdf".into()));
    ch.queue_bind("hq".into(), "h".into(), "".into(), QueueBindOptions::default(), args).await.unwrap();

    let mut hdr = FieldTable::default();
    hdr.insert("fmt".into(), AMQPValue::LongString("pdf".into()));
    ch.basic_publish("h".into(), "".into(), BasicPublishOptions::default(), b"a", BasicProperties::default().with_headers(hdr)).await.unwrap().await.unwrap();
    let mut hdr = FieldTable::default();
    hdr.insert("fmt".into(), AMQPValue::LongString("png".into()));
    ch.basic_publish("h".into(), "".into(), BasicPublishOptions::default(), b"b", BasicProperties::default().with_headers(hdr)).await.unwrap().await.unwrap();
    let m = ch.basic_get("hq".into(), BasicGetOptions::default()).await.unwrap().unwrap();
    assert_eq!(m.data, b"a");
    assert!(ch.basic_get("hq".into(), BasicGetOptions::default()).await.unwrap().is_none());

    // channel-level error: passive declare of a missing queue closes only that channel
    let bad = conn.create_channel().await.unwrap();
    assert!(bad.queue_declare("missing".into(), QueueDeclareOptions { passive: true, ..Default::default() }, FieldTable::default()).await.is_err());
    assert!(ch.queue_declare("hq".into(), QueueDeclareOptions { passive: true, ..Default::default() }, FieldTable::default()).await.is_ok());
}

async fn http(port: u16, method: &str, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.write_all(format!("{method} {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes()).await.unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    out
}

#[tokio::test]
async fn management_api() {
    let amqp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (ap, hp) = (amqp.local_addr().unwrap().port(), hl.local_addr().unwrap().port());
    let broker = rabbitsim::new_broker();
    tokio::spawn(rabbitsim::serve_amqp(amqp, broker.clone(), false));
    tokio::spawn(rabbitsim::serve_http(hl, broker));

    let conn = connect(&format!("amqp://127.0.0.1:{ap}/%2f")).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare("my/q \"1\"".into(), QueueDeclareOptions::default(), FieldTable::default()).await.unwrap();
    for _ in 0..3 {
        ch.basic_publish("".into(), "my/q \"1\"".into(), BasicPublishOptions::default(), b"m", BasicProperties::default()).await.unwrap().await.unwrap();
    }
    let _held = ch.basic_get("my/q \"1\"".into(), BasicGetOptions::default()).await.unwrap().unwrap();

    let q = http(hp, "GET", "/api/queues").await;
    assert!(q.starts_with("HTTP/1.1 200"), "{q}");
    assert!(q.contains(r#""name":"my/q \"1\"""#) && q.contains(r#""messages_ready":2"#) && q.contains(r#""messages_unacknowledged":1"#), "{q}");
    assert!(http(hp, "GET", "/api/overview").await.contains(r#""connections":1"#));
    assert!(http(hp, "GET", "/api/connections").await.contains("127.0.0.1"));
    assert!(http(hp, "GET", "/").await.contains("<title>RabbitSim</title>"));
    assert!(http(hp, "GET", "/nope").await.starts_with("HTTP/1.1 404"));

    let r = http(hp, "DELETE", "/api/queues/%2F/my%2Fq%20%221%22/contents").await;
    assert!(r.contains(r#""purged":2"#), "{r}");
    assert!(http(hp, "DELETE", "/api/queues/%2F/missing/contents").await.starts_with("HTTP/1.1 404"));
}

#[tokio::test]
async fn persistence_survives_restart() {
    let path = format!("{}/persist-{}.db", env!("CARGO_TARGET_TMPDIR"), std::process::id());
    let _ = std::fs::remove_file(&path);

    let start_on = |broker| async move {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let h = tokio::spawn(rabbitsim::serve_amqp(l, broker, false));
        (format!("amqp://127.0.0.1:{port}/%2f"), h)
    };

    // --- first life
    let b1 = rabbitsim::open_broker(&path).unwrap();
    let (url, h1) = start_on(b1.clone()).await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    let durable = QueueDeclareOptions { durable: true, ..Default::default() };
    ch.exchange_declare("dx".into(), ExchangeKind::Topic, ExchangeDeclareOptions { durable: true, ..Default::default() }, FieldTable::default()).await.unwrap();
    ch.queue_declare("dq".into(), durable, FieldTable::default()).await.unwrap();
    ch.queue_bind("dq".into(), "dx".into(), "a.*".into(), QueueBindOptions::default(), FieldTable::default()).await.unwrap();
    ch.queue_declare("tq".into(), QueueDeclareOptions::default(), FieldTable::default()).await.unwrap(); // transient queue
    let persistent = BasicProperties::default().with_delivery_mode(2);
    for body in [&b"one"[..], b"two", b"three"] {
        ch.basic_publish("dx".into(), "a.b".into(), BasicPublishOptions::default(), body, persistent.clone()).await.unwrap().await.unwrap();
    }
    // transient message (delivery_mode 1) into a durable queue, and persistent into a transient queue
    ch.basic_publish("".into(), "dq".into(), BasicPublishOptions::default(), b"transient", BasicProperties::default()).await.unwrap().await.unwrap();
    ch.basic_publish("".into(), "tq".into(), BasicPublishOptions::default(), b"x", persistent.clone()).await.unwrap().await.unwrap();
    // consume + ack "one", leave "two" unacked-then-requeued, "three" ready
    let m = ch.basic_get("dq".into(), BasicGetOptions::default()).await.unwrap().unwrap();
    assert_eq!(m.data, b"one");
    m.ack(BasicAckOptions::default()).await.unwrap();
    let _held = ch.basic_get("dq".into(), BasicGetOptions::default()).await.unwrap().unwrap(); // "two", never acked
    rabbitsim::flush(&b1);
    h1.abort();
    drop(conn);

    // --- second life, same file
    let b2 = rabbitsim::open_broker(&path).unwrap();
    let (url, _h2) = start_on(b2.clone()).await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    // transient queue is gone; durable queue and exchange are back
    assert!(ch.queue_declare("tq".into(), QueueDeclareOptions { passive: true, ..Default::default() }, FieldTable::default()).await.is_err());
    let ch = conn.create_channel().await.unwrap();
    let got = |ch: lapin::Channel| async move {
        ch.basic_get("dq".into(), BasicGetOptions::default()).await.unwrap().map(|m| m.data.clone())
    };
    assert_eq!(got(ch.clone()).await.as_deref(), Some(&b"two"[..])); // unacked at "crash" -> back
    assert_eq!(got(ch.clone()).await.as_deref(), Some(&b"three"[..]));
    assert_eq!(got(ch.clone()).await, None); // acked + transient ones are gone
    // binding survived: publishing through the durable exchange reaches the durable queue
    ch.basic_publish("dx".into(), "a.z".into(), BasicPublishOptions::default(), b"fresh", BasicProperties::default()).await.unwrap().await.unwrap();
    assert_eq!(got(ch.clone()).await.as_deref(), Some(&b"fresh"[..]));
    let _ = std::fs::remove_file(&path);
}
