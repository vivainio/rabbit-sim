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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    s.write_all(format!("{method} {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
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
