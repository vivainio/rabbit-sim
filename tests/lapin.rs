#![allow(clippy::useless_conversion)] // lapin takes &str; the .into() calls keep the tests portable across lapin majors

use futures_lite::StreamExt;
use lapin::{
    options::*, types::*, BasicProperties, Connection, ConnectionProperties, ExchangeKind,
};
use std::time::Duration;
use tokio::net::TcpListener;

async fn start() -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(rabbit_sim::serve(l, false));
    format!("amqp://127.0.0.1:{port}/%2f")
}

async fn connect(url: &str) -> Connection {
    Connection::connect(url, ConnectionProperties::default())
        .await
        .unwrap()
}

#[tokio::test]
async fn default_exchange_publish_consume_ack() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "q1".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "".into(),
        "q1".into(),
        BasicPublishOptions::default(),
        b"hello",
        BasicProperties::default().with_content_type("text/plain".into()),
    )
    .await
    .unwrap()
    .await
    .unwrap();

    let mut c = ch
        .basic_consume(
            "q1".into(),
            "me".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), c.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(d.data, b"hello");
    assert_eq!(d.routing_key.as_str(), "q1");
    assert_eq!(
        d.properties.content_type().as_ref().unwrap().as_str(),
        "text/plain"
    );
    d.ack(BasicAckOptions::default()).await.unwrap();
}

#[tokio::test]
async fn topic_and_fanout_routing() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.exchange_declare(
        "logs".into(),
        ExchangeKind::Topic,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.exchange_declare(
        "fan".into(),
        ExchangeKind::Fanout,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    for (q, key) in [("errs", "*.error"), ("all", "#"), ("kern", "kern.*")] {
        ch.queue_declare(
            q.into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
        ch.queue_bind(
            q.into(),
            "logs".into(),
            key.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    }
    ch.queue_declare(
        "f1".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "f1".into(),
        "fan".into(),
        "".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    // exchange -> exchange binding
    ch.exchange_bind(
        "fan".into(),
        "logs".into(),
        "kern.#".into(),
        ExchangeBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();

    ch.basic_publish(
        "logs".into(),
        "kern.error".into(),
        BasicPublishOptions::default(),
        b"x",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    ch.basic_publish(
        "logs".into(),
        "app.info".into(),
        BasicPublishOptions::default(),
        b"y",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap();

    let count = |q: &'static str| {
        let ch = ch.clone();
        async move {
            ch.queue_declare(
                q.into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap()
            .message_count()
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
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    ch.queue_declare(
        "work".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();

    let confirm = ch
        .basic_publish(
            "".into(),
            "work".into(),
            BasicPublishOptions::default(),
            b"job",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(matches!(
        confirm,
        lapin::publisher_confirm::Confirmation::Ack(_)
    ));

    // mandatory publish with no route comes back
    let confirm = ch
        .basic_publish(
            "".into(),
            "nowhere".into(),
            BasicPublishOptions {
                mandatory: true,
                ..Default::default()
            },
            b"lost",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    match confirm {
        lapin::publisher_confirm::Confirmation::Ack(Some(ret)) => assert_eq!(ret.data, b"lost"),
        other => panic!("expected returned message, got {other:?}"),
    }

    let got = ch
        .basic_get("work".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.data, b"job");
    got.nack(BasicNackOptions {
        requeue: true,
        ..Default::default()
    })
    .await
    .unwrap();
    let again = ch
        .basic_get("work".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert!(again.delivery.redelivered);
    again.ack(BasicAckOptions::default()).await.unwrap();
    assert!(ch
        .basic_get("work".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn prefetch_and_requeue_on_disconnect() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "pq".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    for i in 0..5u8 {
        ch.basic_publish(
            "".into(),
            "pq".into(),
            BasicPublishOptions::default(),
            &[i],
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    let c2 = connect(&url).await;
    let ch2 = c2.create_channel().await.unwrap();
    ch2.basic_qos(2, BasicQosOptions::default()).await.unwrap();
    let mut cons = ch2
        .basic_consume(
            "pq".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let d1 = cons.next().await.unwrap().unwrap();
    let _d2 = cons.next().await.unwrap().unwrap();
    // third must not arrive until an ack
    assert!(
        tokio::time::timeout(Duration::from_millis(300), cons.next())
            .await
            .is_err()
    );
    d1.ack(BasicAckOptions::default()).await.unwrap();
    let d3 = tokio::time::timeout(Duration::from_secs(2), cons.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(d3.data, vec![2]);

    // drop consumer connection: unacked (d2, d3) are requeued -> 4 ready
    drop(cons);
    c2.close(200, "bye".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let q = ch
        .queue_declare(
            "pq".into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    assert_eq!(q.message_count(), 4);
}

#[tokio::test]
async fn ack_redelivers_waiting_messages_of_other_queues_on_the_channel() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    for name in ["first", "second"] {
        ch.queue_declare(
            name.into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    }
    ch.basic_qos(1, BasicQosOptions::default()).await.unwrap();
    let mut first = ch
        .basic_consume(
            "first".into(),
            "c1".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut second = ch
        .basic_consume(
            "second".into(),
            "c2".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    for (queue, body) in [("first", b"a"), ("second", b"b")] {
        ch.basic_publish(
            "".into(),
            queue.into(),
            BasicPublishOptions::default(),
            body,
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    // The single prefetch slot goes to the first queue; the second queue's message must wait.
    let held = first.next().await.unwrap().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), second.next())
            .await
            .is_err()
    );
    // Acking frees the slot, and the other queue's message must be delivered without any further publish.
    held.ack(BasicAckOptions::default()).await.unwrap();
    let delivered = tokio::time::timeout(Duration::from_secs(2), second.next())
        .await
        .expect("the waiting message of the other queue is delivered after the ack")
        .unwrap()
        .unwrap();
    assert_eq!(delivered.data, b"b".to_vec());
}

#[tokio::test]
async fn headers_exchange_and_errors() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.exchange_declare(
        "h".into(),
        ExchangeKind::Headers,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare(
        "hq".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let mut args = FieldTable::default();
    args.insert("x-match".into(), AMQPValue::LongString("all".into()));
    args.insert("fmt".into(), AMQPValue::LongString("pdf".into()));
    ch.queue_bind(
        "hq".into(),
        "h".into(),
        "".into(),
        QueueBindOptions::default(),
        args,
    )
    .await
    .unwrap();

    let mut hdr = FieldTable::default();
    hdr.insert("fmt".into(), AMQPValue::LongString("pdf".into()));
    ch.basic_publish(
        "h".into(),
        "".into(),
        BasicPublishOptions::default(),
        b"a",
        BasicProperties::default().with_headers(hdr),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    let mut hdr = FieldTable::default();
    hdr.insert("fmt".into(), AMQPValue::LongString("png".into()));
    ch.basic_publish(
        "h".into(),
        "".into(),
        BasicPublishOptions::default(),
        b"b",
        BasicProperties::default().with_headers(hdr),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    let m = ch
        .basic_get("hq".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.data, b"a");
    assert!(ch
        .basic_get("hq".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());

    // channel-level error: passive declare of a missing queue closes only that channel
    let bad = conn.create_channel().await.unwrap();
    assert!(bad
        .queue_declare(
            "missing".into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default()
        )
        .await
        .is_err());
    assert!(ch
        .queue_declare(
            "hq".into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default()
        )
        .await
        .is_ok());
}

async fn http(port: u16, method: &str, path: &str) -> String {
    http_body(port, method, path, "").await
}

async fn http_body(port: u16, method: &str, path: &str, body: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    s.write_all(
        format!(
            "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    out
}

#[tokio::test]
async fn management_api() {
    let amqp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (ap, hp) = (
        amqp.local_addr().unwrap().port(),
        hl.local_addr().unwrap().port(),
    );
    let broker = rabbit_sim::new_broker();
    tokio::spawn(rabbit_sim::serve_amqp(amqp, broker.clone(), false));
    tokio::spawn(rabbit_sim::serve_http(hl, broker));

    let conn = connect(&format!("amqp://127.0.0.1:{ap}/%2f")).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "my/q \"1\"".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    for _ in 0..3 {
        ch.basic_publish(
            "".into(),
            "my/q \"1\"".into(),
            BasicPublishOptions::default(),
            b"m",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    let _held = ch
        .basic_get("my/q \"1\"".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();

    let q = http(hp, "GET", "/api/queues").await;
    assert!(q.starts_with("HTTP/1.1 200"), "{q}");
    assert!(
        q.contains(r#""name":"my/q \"1\"""#)
            && q.contains(r#""messages_ready":2"#)
            && q.contains(r#""messages_unacknowledged":1"#),
        "{q}"
    );
    assert!(http(hp, "GET", "/api/overview")
        .await
        .contains(r#""connections":1"#));
    assert!(http(hp, "GET", "/api/connections")
        .await
        .contains("127.0.0.1"));
    assert!(http(hp, "GET", "/")
        .await
        .contains("<title>RabbitSim</title>"));
    assert!(http(hp, "GET", "/nope").await.starts_with("HTTP/1.1 404"));

    let r = http(hp, "DELETE", "/api/queues/%2F/my%2Fq%20%221%22/contents").await;
    assert!(r.contains(r#""purged":2"#), "{r}");
    assert!(http(hp, "DELETE", "/api/queues/%2F/missing/contents")
        .await
        .starts_with("HTTP/1.1 404"));
}

#[tokio::test]
async fn persistence_survives_restart() {
    let path = format!(
        "{}/persist-{}.db",
        env!("CARGO_TARGET_TMPDIR"),
        std::process::id()
    );
    let _ = std::fs::remove_file(&path);

    let start_on = |broker| async move {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let h = tokio::spawn(rabbit_sim::serve_amqp(l, broker, false));
        (format!("amqp://127.0.0.1:{port}/%2f"), h)
    };

    // --- first life
    let b1 = rabbit_sim::open_broker(&path).unwrap();
    let (url, h1) = start_on(b1.clone()).await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    let durable = QueueDeclareOptions {
        durable: true,
        ..Default::default()
    };
    ch.exchange_declare(
        "dx".into(),
        ExchangeKind::Topic,
        ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("dq".into(), durable, FieldTable::default())
        .await
        .unwrap();
    ch.queue_bind(
        "dq".into(),
        "dx".into(),
        "a.*".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare(
        "tq".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap(); // transient queue
    let persistent = BasicProperties::default().with_delivery_mode(2);
    for body in [&b"one"[..], b"two", b"three"] {
        ch.basic_publish(
            "dx".into(),
            "a.b".into(),
            BasicPublishOptions::default(),
            body,
            persistent.clone(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    // transient message (delivery_mode 1) into a durable queue, and persistent into a transient queue
    ch.basic_publish(
        "".into(),
        "dq".into(),
        BasicPublishOptions::default(),
        b"transient",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    ch.basic_publish(
        "".into(),
        "tq".into(),
        BasicPublishOptions::default(),
        b"x",
        persistent.clone(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    // consume + ack "one", leave "two" unacked-then-requeued, "three" ready
    let m = ch
        .basic_get("dq".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.data, b"one");
    m.ack(BasicAckOptions::default()).await.unwrap();
    let _held = ch
        .basic_get("dq".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap(); // "two", never acked
    rabbit_sim::flush(&b1);
    h1.abort();
    drop(conn);

    // --- second life, same file
    let b2 = rabbit_sim::open_broker(&path).unwrap();
    let (url, _h2) = start_on(b2.clone()).await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    // transient queue is gone; durable queue and exchange are back
    assert!(ch
        .queue_declare(
            "tq".into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default()
        )
        .await
        .is_err());
    let ch = conn.create_channel().await.unwrap();
    let got = |ch: lapin::Channel| async move {
        ch.basic_get("dq".into(), BasicGetOptions::default())
            .await
            .unwrap()
            .map(|m| m.data.clone())
    };
    assert_eq!(got(ch.clone()).await.as_deref(), Some(&b"two"[..])); // unacked at "crash" -> back
    assert_eq!(got(ch.clone()).await.as_deref(), Some(&b"three"[..]));
    assert_eq!(got(ch.clone()).await, None); // acked + transient ones are gone
                                             // binding survived: publishing through the durable exchange reaches the durable queue
    ch.basic_publish(
        "dx".into(),
        "a.z".into(),
        BasicPublishOptions::default(),
        b"fresh",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    assert_eq!(got(ch.clone()).await.as_deref(), Some(&b"fresh"[..]));
    let _ = std::fs::remove_file(&path);
}

// ---- queue arguments: dead-lettering, TTL, max-length, delivery limit ----------------

fn s(x: &str) -> AMQPValue {
    AMQPValue::LongString(x.into())
}

fn args(pairs: &[(&str, AMQPValue)]) -> FieldTable {
    let mut t = FieldTable::default();
    for (k, v) in pairs {
        t.insert((*k).into(), v.clone());
    }
    t
}

/// (queue, reason, count) of the most recent x-death entry, plus x-delivery-count if present.
fn death_info(m: &lapin::message::BasicGetMessage) -> (String, String, i64, Option<i64>) {
    let h = m.properties.headers().clone().expect("headers");
    let text = |v: &AMQPValue| match v {
        AMQPValue::LongString(x) => String::from_utf8_lossy(x.as_bytes()).into_owned(),
        other => panic!("not a string: {other:?}"),
    };
    let int = |v: &AMQPValue| match v {
        AMQPValue::LongLongInt(n) => *n,
        other => panic!("not an int: {other:?}"),
    };
    let (mut q, mut r, mut c) = (String::new(), String::new(), 0);
    if let Some(AMQPValue::FieldArray(a)) = h.inner().get("x-death") {
        if let Some(AMQPValue::FieldTable(t)) = a.as_slice().first() {
            q = text(t.inner().get("queue").unwrap());
            r = text(t.inner().get("reason").unwrap());
            c = int(t.inner().get("count").unwrap());
        }
    }
    (q, r, c, h.inner().get("x-delivery-count").map(int))
}

async fn get_eventually(
    ch: &lapin::Channel,
    queue: &str,
) -> Option<lapin::message::BasicGetMessage> {
    for _ in 0..20 {
        if let Some(m) = ch
            .basic_get(queue.into(), BasicGetOptions::default())
            .await
            .unwrap()
        {
            return Some(m);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

async fn dlx_setup(ch: &lapin::Channel) {
    ch.exchange_declare(
        "dlx".into(),
        ExchangeKind::Direct,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare(
        "dead".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "dead".into(),
        "dlx".into(),
        "dead".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
}

fn dl_args(extra: &[(&str, AMQPValue)]) -> FieldTable {
    let mut v = vec![
        ("x-dead-letter-exchange", s("dlx")),
        ("x-dead-letter-routing-key", s("dead")),
    ];
    v.extend_from_slice(extra);
    args(&v)
}

#[tokio::test]
async fn dead_letter_on_reject_records_x_death() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    dlx_setup(&ch).await;
    ch.queue_declare("work".into(), QueueDeclareOptions::default(), dl_args(&[]))
        .await
        .unwrap();
    ch.basic_publish(
        "".into(),
        "work".into(),
        BasicPublishOptions::default(),
        b"poison",
        BasicProperties::default().with_expiration("60000".into()),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    let m = ch
        .basic_get("work".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    m.nack(BasicNackOptions {
        requeue: false,
        ..Default::default()
    })
    .await
    .unwrap();

    let d = get_eventually(&ch, "dead")
        .await
        .expect("dead-lettered message");
    assert_eq!(d.data, b"poison");
    assert_eq!(d.routing_key.as_str(), "dead");
    assert_eq!(death_info(&d), ("work".into(), "rejected".into(), 1, None));
    assert!(
        d.properties.expiration().is_none(),
        "expiration is dropped on dead-lettering"
    );
    let h = d.properties.headers().clone().unwrap();
    assert!(h.inner().contains_key("x-first-death-reason"));

    // redeclaring with different arguments is inequivalent; the same arguments are fine
    assert!(ch
        .queue_declare("work".into(), QueueDeclareOptions::default(), dl_args(&[]))
        .await
        .is_ok());
    let ch2 = conn.create_channel().await.unwrap();
    let e = ch2
        .queue_declare(
            "work".into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await;
    assert!(
        e.is_err(),
        "redeclare without the x-dead-letter args must fail"
    );
}

#[tokio::test]
async fn message_and_queue_ttl_dead_letter() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    dlx_setup(&ch).await;
    ch.queue_declare(
        "qttl".into(),
        QueueDeclareOptions::default(),
        dl_args(&[("x-message-ttl", AMQPValue::LongLongInt(150))]),
    )
    .await
    .unwrap();
    ch.queue_declare("mttl".into(), QueueDeclareOptions::default(), dl_args(&[]))
        .await
        .unwrap();
    ch.basic_publish(
        "".into(),
        "qttl".into(),
        BasicPublishOptions::default(),
        b"a",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    ch.basic_publish(
        "".into(),
        "mttl".into(),
        BasicPublishOptions::default(),
        b"b",
        BasicProperties::default().with_expiration("100".into()),
    )
    .await
    .unwrap()
    .await
    .unwrap();

    let first = get_eventually(&ch, "dead").await.expect("first expired");
    let second = get_eventually(&ch, "dead").await.expect("second expired");
    let mut got = [
        (
            String::from_utf8(first.data.clone()).unwrap(),
            death_info(&first),
        ),
        (
            String::from_utf8(second.data.clone()).unwrap(),
            death_info(&second),
        ),
    ];
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got[0].1, ("qttl".into(), "expired".into(), 1, None));
    assert_eq!(got[1].1, ("mttl".into(), "expired".into(), 1, None));
    // gone from the source queues
    assert!(ch
        .basic_get("qttl".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    assert!(ch
        .basic_get("mttl".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());

    // without a DLX, expired messages are simply dropped
    ch.queue_declare(
        "plain".into(),
        QueueDeclareOptions::default(),
        args(&[("x-message-ttl", AMQPValue::LongLongInt(50))]),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "".into(),
        "plain".into(),
        BasicPublishOptions::default(),
        b"x",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(ch
        .basic_get("plain".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn max_length_overflow_modes() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    dlx_setup(&ch).await;

    // default drop-head: oldest is dead-lettered, newest two stay
    ch.queue_declare(
        "ml".into(),
        QueueDeclareOptions::default(),
        dl_args(&[("x-max-length", AMQPValue::LongLongInt(2))]),
    )
    .await
    .unwrap();
    for b in [&b"m1"[..], b"m2", b"m3"] {
        ch.basic_publish(
            "".into(),
            "ml".into(),
            BasicPublishOptions::default(),
            b,
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    let dead = get_eventually(&ch, "dead").await.expect("m1 dead-lettered");
    assert_eq!(dead.data, b"m1");
    assert_eq!(death_info(&dead).1, "maxlen");
    assert_eq!(
        ch.basic_get("ml".into(), BasicGetOptions::default())
            .await
            .unwrap()
            .unwrap()
            .data,
        b"m2"
    );

    // reject-publish: the publisher confirm becomes a nack and nothing is enqueued
    let cch = conn.create_channel().await.unwrap();
    cch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    cch.queue_declare(
        "rp".into(),
        QueueDeclareOptions::default(),
        args(&[
            ("x-max-length", AMQPValue::LongLongInt(1)),
            ("x-overflow", s("reject-publish")),
        ]),
    )
    .await
    .unwrap();
    let c1 = cch
        .basic_publish(
            "".into(),
            "rp".into(),
            BasicPublishOptions::default(),
            b"keep",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    let c2 = cch
        .basic_publish(
            "".into(),
            "rp".into(),
            BasicPublishOptions::default(),
            b"refuse",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(
        matches!(c1, lapin::publisher_confirm::Confirmation::Ack(_)),
        "{c1:?}"
    );
    assert!(
        matches!(c2, lapin::publisher_confirm::Confirmation::Nack(_)),
        "{c2:?}"
    );
    assert_eq!(
        cch.basic_get("rp".into(), BasicGetOptions::default())
            .await
            .unwrap()
            .unwrap()
            .data,
        b"keep"
    );
    assert!(cch
        .basic_get("rp".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn quorum_queue_delivery_count_and_limit() {
    let url = start().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    dlx_setup(&ch).await;
    let durable = QueueDeclareOptions {
        durable: true,
        ..Default::default()
    };

    // quorum queues must be durable, non-exclusive, non-auto-delete
    let bad = conn.create_channel().await.unwrap();
    assert!(bad
        .queue_declare(
            "nq".into(),
            QueueDeclareOptions::default(),
            args(&[("x-queue-type", s("quorum"))])
        )
        .await
        .is_err());

    ch.queue_declare(
        "qq".into(),
        durable,
        dl_args(&[
            ("x-queue-type", s("quorum")),
            ("x-delivery-limit", AMQPValue::LongLongInt(2)),
        ]),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "".into(),
        "qq".into(),
        BasicPublishOptions::default(),
        b"retry-me",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap();

    let mut seen = Vec::new();
    for _ in 0..3 {
        let m = ch
            .basic_get("qq".into(), BasicGetOptions::default())
            .await
            .unwrap()
            .expect("redelivery");
        seen.push((m.delivery.redelivered, death_info_count(&m)));
        m.nack(BasicNackOptions {
            requeue: true,
            ..Default::default()
        })
        .await
        .unwrap();
    }
    // first delivery has no header; each return bumps x-delivery-count
    assert_eq!(seen, vec![(false, None), (true, Some(1)), (true, Some(2))]);
    // the third return exceeds the limit of 2 -> dead-lettered, not requeued
    assert!(ch
        .basic_get("qq".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    let d = get_eventually(&ch, "dead")
        .await
        .expect("delivery_limit dead-letter");
    assert_eq!(d.data, b"retry-me");
    assert_eq!(death_info(&d).1, "delivery_limit");

    // classic queues don't count deliveries
    ch.queue_declare(
        "cq".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "".into(),
        "cq".into(),
        BasicPublishOptions::default(),
        b"c",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    for _ in 0..3 {
        let m = ch
            .basic_get("cq".into(), BasicGetOptions::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(death_info_count(&m), None);
        m.nack(BasicNackOptions {
            requeue: true,
            ..Default::default()
        })
        .await
        .unwrap();
    }
}

fn death_info_count(m: &lapin::message::BasicGetMessage) -> Option<i64> {
    let h = m.properties.headers().clone()?;
    match h.inner().get("x-delivery-count") {
        Some(AMQPValue::LongLongInt(n)) => Some(*n),
        _ => None,
    }
}

#[tokio::test]
async fn chaos_clock_and_expire_via_http() {
    let amqp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (ap, hp) = (
        amqp.local_addr().unwrap().port(),
        hl.local_addr().unwrap().port(),
    );
    let broker = rabbit_sim::new_broker();
    tokio::spawn(rabbit_sim::serve_amqp(amqp, broker.clone(), false));
    tokio::spawn(rabbit_sim::serve_http(hl, broker));
    let conn = connect(&format!("amqp://127.0.0.1:{ap}/%2f")).await;
    let ch = conn.create_channel().await.unwrap();
    dlx_setup(&ch).await;
    // a one-hour TTL would never fire in a test without the chaos API
    ch.queue_declare(
        "slow".into(),
        QueueDeclareOptions::default(),
        dl_args(&[("x-message-ttl", AMQPValue::LongLongInt(3_600_000))]),
    )
    .await
    .unwrap();
    for b in [&b"a"[..], b"b"] {
        ch.basic_publish(
            "".into(),
            "slow".into(),
            BasicPublishOptions::default(),
            b,
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(ch
        .basic_get("dead".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());

    // 30 minutes: nothing due yet; another 31: both expire into the DLX
    assert!(http(hp, "POST", "/chaos/clock/advance?ms=1800000")
        .await
        .contains(r#""advanced_ms":1800000"#));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(ch
        .basic_get("dead".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    http(hp, "POST", "/chaos/clock/advance?ms=1860000").await;
    let m = get_eventually(&ch, "dead")
        .await
        .expect("expired after clock advance");
    assert_eq!(death_info(&m).1, "expired");
    m.ack(BasicAckOptions::default()).await.unwrap();
    get_eventually(&ch, "dead").await.expect("second expired");

    // expire-now on a queue, and bad requests
    ch.queue_declare(
        "later".into(),
        QueueDeclareOptions::default(),
        dl_args(&[("x-message-ttl", AMQPValue::LongLongInt(3_600_000))]),
    )
    .await
    .unwrap();
    for _ in 0..3 {
        ch.basic_publish(
            "".into(),
            "later".into(),
            BasicPublishOptions::default(),
            b"z",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    assert!(http(hp, "POST", "/chaos/queues/%2F/later/expire")
        .await
        .contains(r#""expired":3"#));
    assert!(ch
        .basic_get("later".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    assert!(http(hp, "POST", "/chaos/queues/%2F/nope/expire")
        .await
        .starts_with("HTTP/1.1 404"));
    assert!(http(hp, "POST", "/chaos/clock/advance")
        .await
        .starts_with("HTTP/1.1 400"));
    assert!(http(hp, "GET", "/chaos/clock/advance?ms=1")
        .await
        .starts_with("HTTP/1.1 404"));
}

async fn start_emulating(major: u8) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let broker = rabbit_sim::new_broker();
    broker.lock().unwrap().set_rabbitmq_major(major);
    tokio::spawn(rabbit_sim::serve_amqp(l, broker, false));
    format!("amqp://127.0.0.1:{port}/%2f")
}

#[tokio::test]
async fn rabbitmq_4_refuses_transient_queues_that_are_not_exclusive() {
    let url = start_emulating(4).await;

    // Durable and exclusive queues are fine.
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "durable".into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare(
        "exclusive".into(),
        QueueDeclareOptions {
            exclusive: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    // A passive check never creates a queue, so it is not refused for its flags.
    ch.queue_declare(
        "durable".into(),
        QueueDeclareOptions {
            passive: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();

    // A transient queue that anyone can use is refused, and the broker closes the connection.
    let err = ch
        .queue_declare(
            "transient".into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect_err("RabbitMQ 4 refuses transient non-exclusive queues");
    assert!(
        err.to_string().contains("transient_nonexcl_queues"),
        "the error names the deprecated feature: {err}"
    );
}

#[tokio::test]
async fn rabbitmq_3_still_accepts_transient_queues() {
    let url = start_emulating(3).await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "transient".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
}

async fn start_with_http() -> (String, u16) {
    let amqp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (ap, hp) = (
        amqp.local_addr().unwrap().port(),
        hl.local_addr().unwrap().port(),
    );
    let broker = rabbit_sim::new_broker();
    tokio::spawn(rabbit_sim::serve_amqp(amqp, broker.clone(), false));
    tokio::spawn(rabbit_sim::serve_http(hl, broker));
    (format!("amqp://127.0.0.1:{ap}/%2f"), hp)
}

async fn publish_confirmed(
    ch: &lapin::Channel,
    queue: &str,
    body: &[u8],
) -> lapin::publisher_confirm::Confirmation {
    ch.basic_publish(
        "".into(),
        queue.into(),
        BasicPublishOptions::default(),
        body,
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap()
}

#[tokio::test]
async fn confirm_delay_holds_confirms_back_in_order() {
    let (url, hp) = start_with_http().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    ch.queue_declare(
        "q".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();

    // no delay: a confirm is immediate
    let started = std::time::Instant::now();
    publish_confirmed(&ch, "q", b"fast").await;
    assert!(started.elapsed() < Duration::from_millis(100));

    assert!(
        http_body(hp, "POST", "/chaos", r#"{"confirm_delay_ms":200}"#)
            .await
            .contains(r#""confirm_delay_ms":200"#)
    );
    let started = std::time::Instant::now();
    publish_confirmed(&ch, "q", b"slow").await;
    assert!(
        started.elapsed() >= Duration::from_millis(180),
        "the confirm was held back: {:?}",
        started.elapsed()
    );

    // several publishes made together are all confirmed, in order, after about one delay
    let mut pending = Vec::new();
    let started = std::time::Instant::now();
    for i in 0..5 {
        pending.push(
            ch.basic_publish(
                "".into(),
                "q".into(),
                BasicPublishOptions::default(),
                format!("m{i}").as_bytes(),
                BasicProperties::default(),
            )
            .await
            .unwrap(),
        );
    }
    for confirm in pending {
        assert!(matches!(
            confirm.await.unwrap(),
            lapin::publisher_confirm::Confirmation::Ack(_)
        ));
    }
    assert!(
        started.elapsed() < Duration::from_millis(800),
        "five publishes waited together, not one after another: {:?}",
        started.elapsed()
    );

    // 0 turns it off again, and a bad request is refused
    http_body(hp, "POST", "/chaos", r#"{}"#).await;
    let started = std::time::Instant::now();
    publish_confirmed(&ch, "q", b"fast again").await;
    assert!(started.elapsed() < Duration::from_millis(100));
    assert!(
        http_body(hp, "POST", "/chaos", r#"{"confirm_delay_ms":"soon"}"#)
            .await
            .contains("non-negative")
    );
}

#[tokio::test]
async fn nack_next_declines_publishes_without_enqueuing_them() {
    let (url, hp) = start_with_http().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    ch.queue_declare(
        "q".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();

    assert!(http_body(hp, "POST", "/chaos", r#"{"nack_next":2}"#)
        .await
        .contains(r#""nack_next":2"#));
    for body in [&b"declined 1"[..], b"declined 2"] {
        assert!(matches!(
            publish_confirmed(&ch, "q", body).await,
            lapin::publisher_confirm::Confirmation::Nack(_)
        ));
    }
    assert!(ch
        .basic_get("q".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());

    // after the count runs out, publishes are accepted again
    assert!(matches!(
        publish_confirmed(&ch, "q", b"accepted").await,
        lapin::publisher_confirm::Confirmation::Ack(_)
    ));
    let got = get_eventually(&ch, "q")
        .await
        .expect("the accepted message");
    assert_eq!(got.data, b"accepted");
    assert!(http_body(hp, "POST", "/chaos", r#"{"nack_next":-1}"#)
        .await
        .contains("non-negative"));
}

async fn declare_and_fill(ch: &lapin::Channel, q: &str, n: usize) {
    ch.queue_declare(
        q.into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    for i in 0..n {
        ch.basic_publish(
            "".into(),
            q.into(),
            BasicPublishOptions::default(),
            format!("m{i}").as_bytes(),
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn deliver_delay_holds_deliveries_back_in_order() {
    let (url, hp) = start_with_http().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    declare_and_fill(&ch, "dq", 3).await;
    assert!(
        http_body(hp, "POST", "/chaos", r#"{"deliver_delay_ms":200}"#)
            .await
            .contains(r#""deliver_delay_ms":200"#)
    );
    let started = std::time::Instant::now();
    let mut cons = ch
        .basic_consume(
            "dq".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    for i in 0..3 {
        let d = cons.next().await.unwrap().unwrap();
        assert_eq!(d.data, format!("m{i}").as_bytes());
        if i == 0 {
            assert!(
                started.elapsed() >= Duration::from_millis(180),
                "{:?}",
                started.elapsed()
            );
        }
        d.ack(BasicAckOptions::default()).await.unwrap();
    }
    // the three were handed out together, so they arrive after about one delay, not three
    assert!(
        started.elapsed() < Duration::from_millis(600),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn ack_delay_keeps_the_message_unacked_meanwhile() {
    let (url, hp) = start_with_http().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    declare_and_fill(&ch, "aq", 2).await;
    ch.basic_qos(1, BasicQosOptions::default()).await.unwrap();
    assert!(http_body(hp, "POST", "/chaos", r#"{"ack_delay_ms":300}"#)
        .await
        .contains(r#""ack_delay_ms":300"#));
    let mut cons = ch
        .basic_consume(
            "aq".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let d1 = cons.next().await.unwrap().unwrap();
    assert_eq!(d1.data, b"m0");
    let acked = std::time::Instant::now();
    d1.ack(BasicAckOptions::default()).await.unwrap();
    // prefetch 1: the next message waits for the broker to act on the ack
    let d2 = tokio::time::timeout(Duration::from_secs(5), cons.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(d2.data, b"m1");
    assert!(
        acked.elapsed() >= Duration::from_millis(270),
        "{:?}",
        acked.elapsed()
    );
    d2.ack(BasicAckOptions::default()).await.unwrap();
    // an ack still pending when the channel closes is dropped quietly
    ch.close(200, "bye".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let ch2 = conn.create_channel().await.unwrap();
    ch2.queue_declare(
        "aq".into(),
        QueueDeclareOptions {
            passive: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn chaos_endpoint_replaces_all_settings_and_rejects_bad_bodies() {
    let (_url, hp) = start_with_http().await;
    let set = http_body(
        hp,
        "POST",
        "/chaos",
        r#"{"confirm_delay_ms": 5, "deliver_delay_ms": 6, "ack_delay_ms": 7, "nack_next": 8}"#,
    )
    .await;
    assert!(set
        .contains(r#"{"confirm_delay_ms":5,"deliver_delay_ms":6,"ack_delay_ms":7,"nack_next":8}"#));
    assert!(http(hp, "GET", "/chaos")
        .await
        .contains(r#""ack_delay_ms":7"#));
    // the body is the whole behaviour: what it leaves out goes back to 0
    assert!(http_body(hp, "POST", "/chaos", r#"{"ack_delay_ms":9}"#)
        .await
        .contains(r#"{"confirm_delay_ms":0,"deliver_delay_ms":0,"ack_delay_ms":9,"nack_next":0}"#));
    assert!(http_body(hp, "POST", "/chaos", "{}")
        .await
        .contains(r#""ack_delay_ms":0"#));
    // bad bodies are refused and change nothing
    http_body(hp, "POST", "/chaos", r#"{"ack_delay_ms":9}"#).await;
    assert!(http(hp, "POST", "/chaos").await.contains("400"));
    assert!(http_body(hp, "POST", "/chaos", r#"{"ack_delay":5}"#)
        .await
        .contains("unknown field"));
    assert!(http(hp, "GET", "/chaos")
        .await
        .contains(r#""ack_delay_ms":9"#));
}

// The same URL on another vhost.
fn on_vhost(url: &str, vhost: &str) -> String {
    format!("{}/{vhost}", url.trim_end_matches("/%2f"))
}

async fn declare_durable(ch: &lapin::Channel, q: &str) {
    ch.queue_declare(
        q.into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
}

async fn publish_to(ch: &lapin::Channel, exchange: &str, key: &str, body: &[u8]) {
    ch.basic_publish(
        exchange.into(),
        key.into(),
        BasicPublishOptions::default(),
        body,
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
}

#[tokio::test]
async fn vhosts_keep_queues_and_exchanges_apart() {
    let (url, _hp) = start_with_http().await;
    let a = connect(&on_vhost(&url, "tenant-a")).await;
    let b = connect(&on_vhost(&url, "tenant-b")).await;
    let root = connect(&url).await;
    let (ca, cb, cr) = (
        a.create_channel().await.unwrap(),
        b.create_channel().await.unwrap(),
        root.create_channel().await.unwrap(),
    );

    // the same names in three vhosts are three separate sets, and the default and amq.* exchanges exist in each
    for ch in [&ca, &cb, &cr] {
        declare_durable(ch, "orders").await;
        ch.exchange_declare(
            "ex".into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
        ch.queue_bind(
            "orders".into(),
            "ex".into(),
            "k".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    }
    publish_to(&ca, "", "orders", b"via default in a").await;
    publish_to(&cb, "ex", "k", b"via ex in b").await;
    publish_to(&cb, "amq.direct", "nobody", b"unrouted").await;

    assert_eq!(
        get_eventually(&ca, "orders").await.unwrap().data,
        b"via default in a"
    );
    assert!(ca
        .basic_get("orders".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        get_eventually(&cb, "orders").await.unwrap().data,
        b"via ex in b"
    );
    assert!(cb
        .basic_get("orders".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    // nothing reached the root vhost
    assert!(cr
        .basic_get("orders".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());

    // the same name may be declared differently in another vhost, and a queue of one vhost is not found from another
    cb.queue_declare(
        "other".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let missing = ca
        .queue_declare(
            "other".into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await;
    assert!(missing.is_err(), "tenant-a has no queue 'other'");
}

#[tokio::test]
async fn vhosts_have_their_own_server_named_queues_dead_letters_and_ttl_clock() {
    let (url, hp) = start_with_http().await;
    let conn = connect(&on_vhost(&url, "dl")).await;
    let ch = conn.create_channel().await.unwrap();

    // a server-named queue comes back with the name the client sees, and can be used by that name
    let q = ch
        .queue_declare(
            "".into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    assert!(q.name().as_str().starts_with("amq.gen-"), "{}", q.name());
    publish_to(&ch, "", q.name().as_str(), b"named by the server").await;
    assert_eq!(
        get_eventually(&ch, q.name().as_str()).await.unwrap().data,
        b"named by the server"
    );

    // a dead-letter exchange named in the queue arguments is looked up in the queue's own vhost
    ch.queue_declare(
        "dead".into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let mut args = FieldTable::default();
    args.insert(
        "x-message-ttl".into(),
        lapin::types::AMQPValue::LongUInt(1_000),
    );
    args.insert(
        "x-dead-letter-exchange".into(),
        lapin::types::AMQPValue::LongString("".into()),
    );
    args.insert(
        "x-dead-letter-routing-key".into(),
        lapin::types::AMQPValue::LongString("dead".into()),
    );
    ch.queue_declare("live".into(), QueueDeclareOptions::default(), args)
        .await
        .unwrap();
    publish_to(&ch, "", "live", b"expires").await;
    http(hp, "POST", "/chaos/queues/dl/live/expire").await;
    let got = get_eventually(&ch, "dead")
        .await
        .expect("dead-lettered in the same vhost");
    assert_eq!(got.data, b"expires");
}

#[tokio::test]
async fn management_api_names_the_vhost_and_purges_by_it() {
    let (url, hp) = start_with_http().await;
    let conn = connect(&on_vhost(&url, "ops")).await;
    let ch = conn.create_channel().await.unwrap();
    declare_durable(&ch, "work").await;
    publish_to(&ch, "", "work", b"x").await;

    let listing = http(hp, "GET", "/api/queues").await;
    assert!(
        listing.contains(r#""name":"work","vhost":"ops""#),
        "{listing}"
    );
    assert!(http(hp, "GET", "/api/connections")
        .await
        .contains(r#""vhost":"ops""#));
    assert!(http(hp, "GET", "/api/exchanges")
        .await
        .contains(r#""vhost":"ops""#));
    // the wrong vhost does not find it
    assert!(http(hp, "DELETE", "/api/queues/%2F/work/contents")
        .await
        .contains("404"));
    assert!(http(hp, "DELETE", "/api/queues/ops/work/contents")
        .await
        .contains(r#""purged":1"#));
}

#[tokio::test]
async fn vhosts_survive_a_restart_with_persistence() {
    let path = format!(
        "{}/persist-vhost-{}.db",
        env!("CARGO_TARGET_TMPDIR"),
        std::process::id()
    );
    let _ = std::fs::remove_file(&path);
    let start_on = |broker| async move {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let h = tokio::spawn(rabbit_sim::serve_amqp(l, broker, false));
        (format!("amqp://127.0.0.1:{port}/%2f"), h)
    };

    let b1 = rabbit_sim::open_broker(&path).unwrap();
    let (url, h1) = start_on(b1.clone()).await;
    for (vhost, body) in [("%2f", &b"root"[..]), ("t1", b"one"), ("t2", b"two")] {
        let conn = connect(&on_vhost(&url, vhost)).await;
        let ch = conn.create_channel().await.unwrap();
        declare_durable(&ch, "q").await;
        ch.basic_publish(
            "".into(),
            "q".into(),
            BasicPublishOptions::default(),
            body,
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    h1.abort();
    drop(b1);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let b2 = rabbit_sim::open_broker(&path).unwrap();
    let (url, _h2) = start_on(b2).await;
    for (vhost, body) in [("%2f", &b"root"[..]), ("t1", b"one"), ("t2", b"two")] {
        let conn = connect(&on_vhost(&url, vhost)).await;
        let ch = conn.create_channel().await.unwrap();
        let got = get_eventually(&ch, "q").await.expect(vhost);
        assert_eq!(got.data, body, "vhost {vhost}");
        assert!(ch
            .basic_get("q".into(), BasicGetOptions::default())
            .await
            .unwrap()
            .is_none());
    }
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn consumers_receive_deliveries_on_a_vhost() {
    let (url, _hp) = start_with_http().await;
    let conn = connect(&on_vhost(&url, "cv")).await;
    let ch = conn.create_channel().await.unwrap();
    declare_durable(&ch, "work").await;
    ch.basic_qos(4, BasicQosOptions::default()).await.unwrap();
    let mut cons = ch
        .basic_consume(
            "work".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    publish_to(&ch, "", "work", b"pushed").await;
    let d = tokio::time::timeout(Duration::from_secs(3), cons.next())
        .await
        .expect("a delivery on the vhost")
        .unwrap()
        .unwrap();
    assert_eq!(d.data, b"pushed");
    d.ack(BasicAckOptions::default()).await.unwrap();
    // a message published before the consumer exists is delivered when it arrives
    publish_to(&ch, "", "work", b"waiting").await;
    let d = tokio::time::timeout(Duration::from_secs(3), cons.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(d.data, b"waiting");
}

async fn start_emulating_with_http(major: u8) -> (String, u16) {
    let amqp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (ap, hp) = (
        amqp.local_addr().unwrap().port(),
        hl.local_addr().unwrap().port(),
    );
    let broker = rabbit_sim::new_broker();
    broker.lock().unwrap().set_rabbitmq_major(major);
    tokio::spawn(rabbit_sim::serve_amqp(amqp, broker.clone(), false));
    tokio::spawn(rabbit_sim::serve_http(hl, broker));
    (format!("amqp://127.0.0.1:{ap}/%2f"), hp)
}

fn epoch_ms_in(millis: i64) -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + millis
}

async fn publish_due(ch: &lapin::Channel, queue: &str, body: &[u8], due_epoch_ms: i64) {
    let mut headers = FieldTable::default();
    headers.insert(
        "x-opt-delivery-time".into(),
        lapin::types::AMQPValue::LongLongInt(due_epoch_ms),
    );
    ch.basic_publish(
        "".into(),
        queue.into(),
        BasicPublishOptions::default(),
        body,
        BasicProperties::default()
            .with_delivery_mode(2)
            .with_headers(headers),
    )
    .await
    .unwrap()
    .await
    .unwrap();
}

async fn declare_typed(ch: &lapin::Channel, q: &str, queue_type: &str) {
    let mut args = FieldTable::default();
    args.insert(
        "x-queue-type".into(),
        lapin::types::AMQPValue::LongString(queue_type.into()),
    );
    ch.queue_declare(
        q.into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        args,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn delayed_messages_wait_on_quorum_queues_until_the_clock_reaches_them() {
    let (url, hp) = start_emulating_with_http(4).await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    declare_typed(&ch, "later", "quorum").await;
    declare_typed(&ch, "classic-later", "classic").await;

    publish_due(&ch, "later", b"in a minute", epoch_ms_in(60_000)).await;
    publish_due(&ch, "later", b"in two minutes", epoch_ms_in(120_000)).await;
    publish_due(&ch, "later", b"already due", epoch_ms_in(-1_000)).await;
    publish_due(
        &ch,
        "classic-later",
        b"classic ignores it",
        epoch_ms_in(60_000),
    )
    .await;

    // routed and confirmed at once, but only the due one is ready
    assert_eq!(
        get_eventually(&ch, "later").await.unwrap().data,
        b"already due"
    );
    assert!(ch
        .basic_get("later".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    // a classic queue does not hold it, as on a real broker
    assert_eq!(
        get_eventually(&ch, "classic-later").await.unwrap().data,
        b"classic ignores it"
    );
    // the held ones show in the management API
    let listing = http(hp, "GET", "/api/queues").await;
    assert!(
        listing.contains(r#""name":"later""#) && listing.contains(r#""messages_delayed":2"#),
        "{listing}"
    );

    http(hp, "POST", "/chaos/clock/advance?ms=61000").await;
    assert_eq!(
        get_eventually(&ch, "later").await.unwrap().data,
        b"in a minute"
    );
    assert!(ch
        .basic_get("later".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    http(hp, "POST", "/chaos/clock/advance?ms=60000").await;
    assert_eq!(
        get_eventually(&ch, "later").await.unwrap().data,
        b"in two minutes"
    );
}

#[tokio::test]
async fn delayed_messages_are_delivered_to_a_waiting_consumer_and_older_brokers_ignore_the_header()
{
    let (url, hp) = start_emulating_with_http(4).await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    declare_typed(&ch, "pushed", "quorum").await;
    let mut cons = ch
        .basic_consume(
            "pushed".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    publish_due(&ch, "pushed", b"timed", epoch_ms_in(30_000)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), cons.next())
            .await
            .is_err()
    );
    http(hp, "POST", "/chaos/clock/advance?ms=31000").await;
    let d = tokio::time::timeout(Duration::from_secs(3), cons.next())
        .await
        .expect("delivered once due")
        .unwrap()
        .unwrap();
    assert_eq!(d.data, b"timed");

    // RabbitMQ 3.x and 4.0-4.3 have no such feature: the header is just a header and the message is ready at once
    let old = start_emulating(3).await;
    let conn = connect(&old).await;
    let ch = conn.create_channel().await.unwrap();
    declare_typed(&ch, "q", "quorum").await;
    publish_due(&ch, "q", b"no delay here", epoch_ms_in(60_000)).await;
    assert_eq!(
        get_eventually(&ch, "q").await.unwrap().data,
        b"no delay here"
    );
}

// A chain of queues with fixed TTLs and dead-letter exchanges, the way a delay-level topology is built.
async fn declare_hop(
    ch: &lapin::Channel,
    queue: &str,
    ttl_ms: u32,
    next_exchange: &str,
    next_key: &str,
) {
    let mut args = FieldTable::default();
    args.insert(
        "x-message-ttl".into(),
        lapin::types::AMQPValue::LongUInt(ttl_ms),
    );
    args.insert(
        "x-dead-letter-exchange".into(),
        lapin::types::AMQPValue::LongString(next_exchange.into()),
    );
    args.insert(
        "x-dead-letter-routing-key".into(),
        lapin::types::AMQPValue::LongString(next_key.into()),
    );
    ch.queue_declare(
        queue.into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        args,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn one_clock_advance_carries_a_message_through_every_hop_of_a_delay_chain() {
    let (url, hp) = start_with_http().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    // hop4 (4 s) -> hop2 (2 s) -> hop1 (1 s) -> final, each handing on by the default exchange
    declare_durable(&ch, "final").await;
    declare_hop(&ch, "hop1", 1000, "", "final").await;
    declare_hop(&ch, "hop2", 2000, "", "hop1").await;
    declare_hop(&ch, "hop4", 4000, "", "hop2").await;
    publish_to(&ch, "", "hop4", b"seven seconds").await;

    // 3.5 s is not yet hop4's 4 s, so nothing has moved on
    http(hp, "POST", "/chaos/clock/advance?ms=3500").await;
    assert!(ch
        .basic_get("final".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    // another 3.5 s makes 7 s in all: 4 + 2 + 1, with each hop starting when the one before it expired
    http(hp, "POST", "/chaos/clock/advance?ms=3500").await;
    assert_eq!(
        get_eventually(&ch, "final").await.unwrap().data,
        b"seven seconds"
    );
}

#[tokio::test]
async fn a_single_large_advance_passes_all_the_hops_at_once() {
    let (url, hp) = start_with_http().await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    declare_durable(&ch, "final").await;
    declare_hop(&ch, "hop1", 1000, "", "final").await;
    declare_hop(&ch, "hop2", 2000, "", "hop1").await;
    declare_hop(&ch, "hop4", 4000, "", "hop2").await;
    publish_to(&ch, "", "hop4", b"at once").await;
    http(hp, "POST", "/chaos/clock/advance?ms=7000").await;
    assert_eq!(get_eventually(&ch, "final").await.unwrap().data, b"at once");
}

#[tokio::test]
async fn advancing_the_clock_moves_delivery_times_too_and_reset_puts_it_back() {
    let (url, hp) = start_emulating_with_http(4).await;
    let conn = connect(&url).await;
    let ch = conn.create_channel().await.unwrap();
    declare_typed(&ch, "dq", "quorum").await;

    // after an advance, a time that is still ahead on the real clock is already past on the broker's
    http(hp, "POST", "/chaos/clock/advance?ms=10000").await;
    publish_due(&ch, "dq", b"looks due", epoch_ms_in(5_000)).await;
    assert_eq!(get_eventually(&ch, "dq").await.unwrap().data, b"looks due");

    // reset undoes it, so the same time is in the future again and the message waits
    assert!(http(hp, "POST", "/chaos/clock/reset")
        .await
        .contains(r#""advanced_ms":0"#));
    publish_due(&ch, "dq", b"waits", epoch_ms_in(60_000)).await;
    assert!(ch
        .basic_get("dq".into(), BasicGetOptions::default())
        .await
        .unwrap()
        .is_none());
    http(hp, "POST", "/chaos/clock/advance?ms=61000").await;
    assert_eq!(get_eventually(&ch, "dq").await.unwrap().data, b"waits");
}
