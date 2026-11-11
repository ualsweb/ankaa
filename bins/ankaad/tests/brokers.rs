//! Generic sources against real brokers (opt-in): set `ANKAA_BROKER_TESTS=1` with a NATS server on
//! 127.0.0.1:4222 and a Kafka broker on 127.0.0.1:9092, e.g.
//!
//! ```sh
//! docker run -d --name ankaa-nats -p 4222:4222 nats:2.11-alpine
//! docker run -d --name ankaa-kafka -p 9092:9092 -e KAFKA_NODE_ID=1 \
//!   -e KAFKA_PROCESS_ROLES=broker,controller \
//!   -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
//!   -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
//!   -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
//!   -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
//!   -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
//!   -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 apache/kafka:3.9.1
//! docker run -d --name ankaa-mqtt -p 1883:1883 eclipse-mosquitto:2 \
//!   mosquitto -c /mosquitto-no-auth.conf
//! ANKAA_BROKER_TESTS=1 cargo test -p ankaad --test brokers
//! ```
//!
//! The server is started with one NATS source and one Kafka source feeding the same user table
//! plus a continuous query; messages are published with the real clients; counts, dictionary
//! names and the continuous-query output are checked over Flight.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn enabled() -> bool {
    std::env::var("ANKAA_BROKER_TESTS").is_ok_and(|v| v == "1")
}

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_ankaad"))
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_for(log: &Path, needle: &str, timeout: Duration) {
    let t0 = Instant::now();
    loop {
        if std::fs::read_to_string(log).is_ok_and(|s| s.contains(needle)) {
            return;
        }
        assert!(
            t0.elapsed() < timeout,
            "timeout waiting for {needle:?}:\n{}",
            std::fs::read_to_string(log).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn query(addr: &str, sql: &str) -> String {
    let out = Command::new(bin())
        .args(["query", "--addr", addr, "--sql", sql])
        .env("ANKAA_TOKEN", "t0k3n")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn first_cell(table: &str) -> String {
    let row = table
        .lines()
        .filter(|l| l.starts_with('|'))
        .nth(1)
        .expect("a data row");
    row.trim_matches('|')
        .split('|')
        .next()
        .unwrap()
        .trim()
        .to_owned()
}

fn count_until(addr: &str, sql: &str, want: &str, timeout: Duration) {
    let t0 = Instant::now();
    loop {
        let got = first_cell(&query(addr, sql));
        if got == want {
            return;
        }
        assert!(t0.elapsed() < timeout, "{sql}: got {got}, want {want}");
        std::thread::sleep(Duration::from_millis(300));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn nats_and_kafka_sources_feed_a_user_table() {
    if !enabled() {
        eprintln!("skipped: set ANKAA_BROKER_TESTS=1 with NATS on :4222 and Kafka on :9092");
        return;
    }
    let run = std::process::id();
    let subject = format!("ankaa.test.{run}.sensors");
    let topic = format!("ankaa-test-{run}");

    // Kafka topic must exist before the source starts reading partitions
    let kafka = rskafka::client::ClientBuilder::new(vec!["127.0.0.1:9092".to_owned()])
        .build()
        .await
        .expect("kafka on 127.0.0.1:9092");
    kafka
        .controller_client()
        .unwrap()
        .create_topic(&topic, 2, 1, 10_000)
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let (flight, metrics) = (free_port(), free_port());
    let capture =
        std::fs::canonicalize("../../bench/data/binance-btcusdt-trade-depth-20s.ankc").unwrap();
    let config = format!(
        r#"
[feed]
url = "file://{capture}"
depth_snapshots = false
[server]
flight_addr = "127.0.0.1:{flight}"
metrics_addr = "127.0.0.1:{metrics}"
[storage]
snapshot_ms = 100

[[tables]]
name = "sensors"
partition_by = "device"
columns = [
  {{ name = "ts", type = "timestamp_ns" }},
  {{ name = "device", type = "dict" }},
  {{ name = "value", type = "fixed8" }},
]

[[sources]]
name = "nats-json"
kind = "nats"
url = "nats://127.0.0.1:4222"
subject = "{subject}"
format = "json"
table = "sensors"
ts_unit = "millis"
map = {{ ts = "t", device = "dev", value = "v" }}

[[sources]]
name = "kafka-csv"
kind = "kafka"
url = "127.0.0.1:9092"
subject = "{topic}"
format = "csv"
table = "sensors"
ts_unit = "millis"
map = {{ ts = "0", device = "1", value = "2" }}

[[continuous]]
name = "sensor_1m"
kind = "ohlcv"
table = "sensors"
key = "device"
value = "value"
width_ns = 60000000000
"#,
        capture = capture.display()
    );
    let cfg_path = dir.path().join("brokers.toml");
    std::fs::write(&cfg_path, config).unwrap();
    let log_path = dir.path().join("brokers.log");
    let _server = Server(
        Command::new(bin())
            .args(["serve", "--config"])
            .arg(&cfg_path)
            .args(["--duration-secs", "60"])
            .env("ANKAA_TOKEN", "t0k3n")
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log_path).unwrap())
            .spawn()
            .unwrap(),
    );
    wait_for(&log_path, "nats subscribed", Duration::from_secs(20));
    wait_for(&log_path, "kafka consuming", Duration::from_secs(30));
    // let the Kafka source fetch its starting offsets before producing
    tokio::time::sleep(Duration::from_secs(1)).await;

    let base_ms: i64 = 1_762_700_000_000;
    // NATS: 300 JSON messages from 3 devices
    let nats = async_nats::connect("nats://127.0.0.1:4222").await.unwrap();
    for i in 0..300i64 {
        let msg = format!(
            r#"{{"t":{},"dev":"nats-{}","v":"{}.25"}}"#,
            base_ms + i * 1000,
            i % 3,
            20 + i % 5
        );
        nats.publish(subject.clone(), msg.into()).await.unwrap();
    }
    nats.flush().await.unwrap();

    // Kafka: 200 CSV records from 2 devices, spread over 2 partitions
    for p in 0..2i32 {
        let pc = kafka
            .partition_client(
                &topic,
                p,
                rskafka::client::partition::UnknownTopicHandling::Retry,
            )
            .await
            .unwrap();
        let records: Vec<rskafka::record::Record> = (0..100i64)
            .map(|i| rskafka::record::Record {
                key: None,
                value: Some(
                    format!("{},kafka-{p},{}.5", base_ms + i * 1000, 10 + i % 3).into_bytes(),
                ),
                headers: std::collections::BTreeMap::new(),
                timestamp: chrono::Utc::now(),
            })
            .collect();
        pc.produce(
            records,
            rskafka::client::partition::Compression::NoCompression,
        )
        .await
        .unwrap();
    }

    let addr = format!("127.0.0.1:{flight}");
    let wait = Duration::from_secs(20);
    count_until(&addr, "SELECT count(*) FROM sensors", "500", wait);
    count_until(
        &addr,
        "SELECT count(*) FROM dictionaries WHERE \"table\" = 'sensors' AND \"column\" = 'device'",
        "5",
        wait,
    );
    // exact decimals survive both paths
    let nats_sum = first_cell(&query(
        &addr,
        "SELECT sum(s.value) FROM sensors s JOIN dictionaries d ON d.\"table\" = 'sensors' AND d.id = s.device WHERE d.name LIKE 'nats-%'",
    ));
    // Σ (20 + i%5 + 0.25) for i in 0..300 = 300*20.25 + 60*(0+1+2+3+4) = 6075 + 600 = 6675 → ×1e8
    assert_eq!(nats_sum, "667500000000");
    let kafka_sum = first_cell(&query(
        &addr,
        "SELECT sum(s.value) FROM sensors s JOIN dictionaries d ON d.\"table\" = 'sensors' AND d.id = s.device WHERE d.name LIKE 'kafka-%'",
    ));
    // per partition Σ (10 + i%3 + 0.5) for i in 0..100 = 100*10.5 + (33*1 + 33*2) = 1050 + 99 = 1149; ×2 partitions
    assert_eq!(kafka_sum, "229800000000");
    // the continuous query sees rows from both brokers: 5 devices have a live bar
    count_until(&addr, "SELECT count(*) FROM cq_sensor_1m_live", "5", wait);
}

/// SIGTERM and wait for the process to exit.
fn stop(mut server: Server) {
    let pid = server.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let t0 = Instant::now();
    while server.0.try_wait().unwrap().is_none() {
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "server did not stop"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A Kafka source continues from its saved offsets: messages published while the server was
/// down are ingested after a clean restart and after a crash, none is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn kafka_source_resumes_where_it_stopped() {
    if !enabled() {
        eprintln!("skipped: set ANKAA_BROKER_TESTS=1 with Kafka on :9092");
        return;
    }
    let topic = format!("ankaa-resume-{}", std::process::id());
    let kafka = rskafka::client::ClientBuilder::new(vec!["127.0.0.1:9092".to_owned()])
        .build()
        .await
        .expect("kafka on 127.0.0.1:9092");
    kafka
        .controller_client()
        .unwrap()
        .create_topic(&topic, 2, 1, 10_000)
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (flight, metrics) = (free_port(), free_port());
    let config = format!(
        r#"
[feed]
enabled = false
streams = []
[server]
flight_addr = "127.0.0.1:{flight}"
metrics_addr = "127.0.0.1:{metrics}"
[storage]
snapshot_ms = 100
wal_dir = "{wal}"
checkpoint_dir = "{ckpt}"

[[tables]]
name = "sensors"
partition_by = "device"
columns = [
  {{ name = "ts", type = "timestamp_ns" }},
  {{ name = "device", type = "dict" }},
  {{ name = "value", type = "fixed8" }},
]

[[sources]]
name = "kafka-csv"
kind = "kafka"
url = "127.0.0.1:9092"
subject = "{topic}"
format = "csv"
table = "sensors"
ts_unit = "millis"
map = {{ ts = "0", device = "1", value = "2" }}
"#,
        wal = dir.path().join("wal").display(),
        ckpt = dir.path().join("ckpt").display(),
    );
    let cfg_path = dir.path().join("resume.toml");
    std::fs::write(&cfg_path, config).unwrap();
    let start = |name: &str| -> (Server, PathBuf) {
        let log = dir.path().join(format!("{name}.log"));
        let child = Command::new(bin())
            .args(["serve", "--config"])
            .arg(&cfg_path)
            .env("ANKAA_TOKEN", "t0k3n")
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .unwrap();
        (Server(child), log)
    };
    // `from..to` records per partition, timestamps increasing with the record number
    let base_ms: i64 = 1_762_700_000_000;
    let produce = |from: i64, to: i64| {
        let kafka = &kafka;
        let topic = topic.clone();
        async move {
            for p in 0..2i32 {
                let pc = kafka
                    .partition_client(
                        &topic,
                        p,
                        rskafka::client::partition::UnknownTopicHandling::Retry,
                    )
                    .await
                    .unwrap();
                let records: Vec<rskafka::record::Record> = (from..to)
                    .map(|i| rskafka::record::Record {
                        key: None,
                        value: Some(format!("{},dev-{p},{i}.5", base_ms + i * 1000).into_bytes()),
                        headers: std::collections::BTreeMap::new(),
                        timestamp: chrono::Utc::now(),
                    })
                    .collect();
                pc.produce(
                    records,
                    rskafka::client::partition::Compression::NoCompression,
                )
                .await
                .unwrap();
            }
        }
    };
    let addr = format!("127.0.0.1:{flight}");
    let wait = Duration::from_secs(30);
    let count = "SELECT count(*) FROM sensors";
    // sum of the record numbers ingested: a lost or repeated record changes it
    let checksum = "SELECT sum(value) FROM sensors";
    let sum_of = |n: i64| -> String {
        // per partition Σ (i + 0.5) for i in 0..n, two partitions, ×1e8
        (2 * (n * (n - 1) / 2 * 100_000_000 + n * 50_000_000)).to_string()
    };

    // 1. first run: starts at the latest offset, ingests what is published from then on
    let (server, log) = start("run1");
    wait_for(&log, "kafka consuming", wait);
    tokio::time::sleep(Duration::from_secs(1)).await;
    produce(0, 100).await;
    count_until(&addr, count, "200", wait);
    // let the positions become durable, then stop cleanly
    tokio::time::sleep(Duration::from_millis(2500)).await;
    stop(server);

    // 2. published while the server is down
    produce(100, 150).await;
    let (server, log) = start("run2");
    wait_for(&log, "kafka resumes from saved positions", wait);
    count_until(&addr, count, "300", wait);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(first_cell(&query(&addr, count)), "300");
    assert_eq!(first_cell(&query(&addr, checksum)), sum_of(150));

    // 3. crash (SIGKILL) after more data, more data while down, restart
    produce(150, 200).await;
    count_until(&addr, count, "400", wait);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    drop(server);
    produce(200, 230).await;
    let (server, log) = start("run3");
    wait_for(&log, "kafka resumes from saved positions", wait);
    count_until(&addr, count, "460", wait);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(first_cell(&query(&addr, count)), "460");
    assert_eq!(first_cell(&query(&addr, checksum)), sum_of(230));
    stop(server);
}

/// An MQTT source: device and line come from the topic, a missing reading is NULL, and messages
/// published while the server is down are delivered by the broker's persistent session on the
/// next start (`QoS` 1), none lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn mqtt_source_maps_topics_and_keeps_its_session() {
    if !enabled() {
        eprintln!("skipped: set ANKAA_BROKER_TESTS=1 with an MQTT broker on :1883");
        return;
    }
    let run = std::process::id();
    let dir = tempfile::tempdir().unwrap();
    let (flight, metrics) = (free_port(), free_port());
    let config = format!(
        r#"
[feed]
enabled = false
streams = []
[server]
flight_addr = "127.0.0.1:{flight}"
metrics_addr = "127.0.0.1:{metrics}"
[storage]
snapshot_ms = 100
wal_dir = "{wal}"
checkpoint_dir = "{ckpt}"

[[tables]]
name = "power"
partition_by = "machine"
max_lateness_ms = 60000
columns = [
  {{ name = "ts", type = "timestamp_ns" }},
  {{ name = "line", type = "dict" }},
  {{ name = "machine", type = "dict" }},
  {{ name = "kw", type = "float64", nullable = true }},
]

[[sources]]
name = "plant-{run}"
kind = "mqtt"
url = "mqtt://127.0.0.1:1883"
subject = "ankaa-{run}/+/+/power"
format = "json"
table = "power"
ts_unit = "millis"
map = {{ ts = "t", line = "$topic.1", machine = "$topic.2", kw = "kw" }}
"#,
        wal = dir.path().join("wal").display(),
        ckpt = dir.path().join("ckpt").display(),
    );
    let cfg_path = dir.path().join("mqtt.toml");
    std::fs::write(&cfg_path, config).unwrap();
    let start = |name: &str| {
        let log = dir.path().join(format!("{name}.log"));
        let server = Server(
            Command::new(bin())
                .args(["serve", "--config"])
                .arg(&cfg_path)
                .args(["--duration-secs", "120"])
                .env("ANKAA_TOKEN", "t0k3n")
                .stdout(Stdio::null())
                .stderr(std::fs::File::create(&log).unwrap())
                .spawn()
                .unwrap(),
        );
        (server, log)
    };
    let mut opts = rumqttc::MqttOptions::new(format!("pub-{run}"), "127.0.0.1", 1883);
    opts.set_keep_alive(Duration::from_secs(10));
    let (client, mut events) = rumqttc::AsyncClient::new(opts, 1_000);
    let pump = tokio::spawn(async move { while events.poll().await.is_ok() {} });
    let base_ms: i64 = 1_762_700_000_000;
    let publish = |from: i64, to: i64| {
        let client = client.clone();
        async move {
            for i in from..to {
                // every tenth reading has no kw value
                let kw = if i % 10 == 0 {
                    "null".to_owned()
                } else {
                    format!("{}.5", i % 7)
                };
                let topic = format!("ankaa-{run}/line{}/m{}/power", i % 2, i % 4);
                let msg = format!(r#"{{"t":{},"kw":{kw}}}"#, base_ms + i * 100);
                client
                    .publish(topic, rumqttc::QoS::AtLeastOnce, false, msg)
                    .await
                    .unwrap();
            }
        }
    };

    let (server, log) = start("run1");
    wait_for(&log, "mqtt connected", Duration::from_secs(20));
    tokio::time::sleep(Duration::from_millis(500)).await; // subscription in place
    publish(0, 400).await;
    let addr = format!("127.0.0.1:{flight}");
    let wait = Duration::from_secs(20);
    count_until(&addr, "SELECT count(*) FROM power", "400", wait);
    count_until(&addr, "SELECT count(kw) FROM power", "360", wait);
    count_until(
        &addr,
        "SELECT count(*) FROM dictionaries WHERE \"table\" = 'power' AND \"column\" = 'machine'",
        "4",
        wait,
    );
    let line1 = first_cell(&query(
        &addr,
        "SELECT count(*) FROM power p JOIN dictionaries d ON d.\"table\" = 'power' AND d.\"column\" = 'line' AND d.id = p.line WHERE d.name = 'line1'",
    ));
    assert_eq!(line1, "200");
    stop(server);

    // published while the server is down: the broker keeps them for the session
    publish(400, 700).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (_server, log) = start("run2");
    wait_for(&log, "mqtt connected", Duration::from_secs(20));
    count_until(&addr, "SELECT count(*) FROM power", "700", wait);
    count_until(&addr, "SELECT count(kw) FROM power", "630", wait);
    pump.abort();
}

/// Minimal Sparkplug B encoder for the test (protobuf by hand: the server must not depend on a
/// code generator, and neither does its test).
mod spb {
    pub fn varint(mut v: u64, out: &mut Vec<u8>) {
        while v >= 0x80 {
            out.push((v as u8) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
    fn len(tag: u8, body: &[u8], out: &mut Vec<u8>) {
        out.push(tag);
        varint(body.len() as u64, out);
        out.extend_from_slice(body);
    }
    pub enum V<'a> {
        Double(f64),
        /// `int_value`: the two's complement of a 32-bit integer
        Int(i32),
        Str(&'a str),
    }
    /// One metric: name (BIRTH) or alias only (DATA, `datatype` 0 = left out as the
    /// specification asks).
    pub fn metric(name: Option<&str>, alias: u64, ts: u64, datatype: u32, v: &V) -> Vec<u8> {
        let mut m = Vec::new();
        if let Some(n) = name {
            len(0x0a, n.as_bytes(), &mut m);
        }
        m.push(0x10);
        varint(alias, &mut m);
        m.push(0x18);
        varint(ts, &mut m);
        if datatype != 0 {
            m.push(0x20);
            varint(u64::from(datatype), &mut m);
        }
        match v {
            V::Double(x) => {
                m.push(0x69);
                m.extend_from_slice(&x.to_le_bytes());
            }
            V::Int(i) => {
                m.push(0x50);
                varint(u64::from(*i as u32), &mut m);
            }
            V::Str(s) => len(0x7a, s.as_bytes(), &mut m),
        }
        m
    }
    pub fn payload(ts: u64, seq: u64, metrics: &[Vec<u8>]) -> Vec<u8> {
        let mut p = vec![0x08];
        varint(ts, &mut p);
        for m in metrics {
            len(0x12, m, &mut p);
        }
        p.push(0x18);
        varint(seq, &mut p);
        p
    }
}

/// Sparkplug B through Mosquitto: DATA that arrives before any BIRTH makes the server ask the
/// edge node for a rebirth (`NCMD`); BIRTHs define names, datatypes and aliases; DATA by alias
/// without a datatype is read with the BIRTH's (an `Int32` keeps its sign); after a restart the
/// aliases still resolve without a new BIRTH (replayed from the log).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn sparkplug_source_resolves_aliases_and_asks_for_rebirth() {
    use spb::{V, metric, payload};
    if !enabled() {
        eprintln!("skipped: set ANKAA_BROKER_TESTS=1 with an MQTT broker on :1883");
        return;
    }
    let run = std::process::id();
    let group = format!("g{run}");
    let dir = tempfile::tempdir().unwrap();
    let (flight, metrics) = (free_port(), free_port());
    let config = format!(
        r#"
[feed]
enabled = false
streams = []
[server]
flight_addr = "127.0.0.1:{flight}"
metrics_addr = "127.0.0.1:{metrics}"
[storage]
snapshot_ms = 100
wal_dir = "{wal}"
checkpoint_dir = "{ckpt}"

[[tables]]
name = "scada"
partition_by = "metric"
columns = [
  {{ name = "ts", type = "timestamp_ns" }},
  {{ name = "node", type = "dict" }},
  {{ name = "device", type = "dict" }},
  {{ name = "metric", type = "dict" }},
  {{ name = "value", type = "float64", nullable = true }},
  {{ name = "text", type = "dict" }},
]

[[sources]]
name = "scada-{run}"
kind = "mqtt"
url = "mqtt://127.0.0.1:1883"
subject = "spBv1.0/{group}/#"
format = "sparkplug"
table = "scada"
sparkplug_rebirth = true
map = {{ ts = "$timestamp", node = "$node", device = "$device", metric = "$metric", value = "$value", text = "$string" }}
"#,
        wal = dir.path().join("wal").display(),
        ckpt = dir.path().join("ckpt").display(),
    );
    let cfg_path = dir.path().join("sparkplug.toml");
    std::fs::write(&cfg_path, config).unwrap();
    let start = |name: &str| {
        let log = dir.path().join(format!("{name}.log"));
        let server = Server(
            Command::new(bin())
                .args(["serve", "--config"])
                .arg(&cfg_path)
                .args(["--duration-secs", "120"])
                .env("ANKAA_TOKEN", "t0k3n")
                .stdout(Stdio::null())
                .stderr(std::fs::File::create(&log).unwrap())
                .spawn()
                .unwrap(),
        );
        (server, log)
    };
    // the edge node: publishes, and listens for commands addressed to it
    let mut opts = rumqttc::MqttOptions::new(format!("edge-{run}"), "127.0.0.1", 1883);
    opts.set_keep_alive(Duration::from_secs(10));
    let (client, mut events) = rumqttc::AsyncClient::new(opts, 1_000);
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let pump = tokio::spawn(async move {
        while let Ok(e) = events.poll().await {
            if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(p)) = e {
                let _ = cmd_tx.send(p.topic);
            }
        }
    });
    client
        .subscribe(format!("spBv1.0/{group}/NCMD/#"), rumqttc::QoS::AtLeastOnce)
        .await
        .unwrap();
    let send = |kind: &str, device: Option<&str>, p: Vec<u8>| {
        let topic = match device {
            Some(d) => format!("spBv1.0/{group}/{kind}/gw1/{d}"),
            None => format!("spBv1.0/{group}/{kind}/gw1"),
        };
        let client = client.clone();
        async move {
            client
                .publish(topic, rumqttc::QoS::AtLeastOnce, false, p)
                .await
                .unwrap();
        }
    };
    let base: u64 = 1_762_700_000_000;
    let data = |from: u64, to: u64| {
        (from..to)
            .map(|i| {
                payload(
                    base + 10 + i,
                    i % 256,
                    &[
                        metric(None, 1, base + 10 + i, 0, &V::Double(20.0 + i as f64)),
                        metric(None, 3, base + 10 + i, 0, &V::Int(-(i as i32))),
                    ],
                )
            })
            .collect::<Vec<_>>()
    };

    let (server, log) = start("run1");
    wait_for(&log, "mqtt connected", Duration::from_secs(20));
    tokio::time::sleep(Duration::from_millis(500)).await;
    // DATA before any BIRTH: refused rows, and a rebirth request to the node
    send("DDATA", Some("press"), data(0, 1).remove(0)).await;
    let cmd = tokio::time::timeout(Duration::from_secs(10), cmd_rx.recv())
        .await
        .expect("a rebirth request")
        .unwrap();
    assert_eq!(cmd, format!("spBv1.0/{group}/NCMD/gw1"));

    // the edge node answers with its BIRTHs, then sends DATA by alias only
    send(
        "NBIRTH",
        None,
        payload(base, 0, &[metric(Some("Uptime"), 0, base, 4, &V::Int(7))]),
    )
    .await;
    send(
        "DBIRTH",
        Some("press"),
        payload(
            base + 1,
            1,
            &[
                metric(Some("Temp"), 1, base + 1, 10, &V::Double(19.5)),
                metric(Some("State"), 2, base + 1, 12, &V::Str("IDLE")),
                metric(Some("Offset"), 3, base + 1, 3, &V::Int(0)),
            ],
        ),
    )
    .await;
    for p in data(0, 50) {
        send("DDATA", Some("press"), p).await;
    }
    let addr = format!("127.0.0.1:{flight}");
    let wait = Duration::from_secs(20);
    // 1 + 3 birth rows, 2 per DATA message
    count_until(&addr, "SELECT count(*) FROM scada", "104", wait);
    // dict columns hold "" where the selector does not apply: only State has text
    count_until(
        &addr,
        "SELECT count(*) FROM scada s JOIN dictionaries d ON d.\"table\" = 'scada' AND d.\"column\" = 'text' AND d.id = s.text WHERE d.name = 'IDLE'",
        "1",
        wait,
    );
    // an Int32 sent without its datatype keeps its sign
    count_until(
        &addr,
        "SELECT min(s.value) FROM scada s JOIN dictionaries d ON d.\"table\" = 'scada' AND d.\"column\" = 'metric' AND d.id = s.metric WHERE d.name = 'Offset'",
        "-49.0",
        wait,
    );
    stop(server);

    // after a restart the aliases resolve from the log, without a new BIRTH
    let (_server, log) = start("run2");
    wait_for(&log, "mqtt connected", Duration::from_secs(20));
    tokio::time::sleep(Duration::from_millis(500)).await;
    for p in data(50, 60) {
        send("DDATA", Some("press"), p).await;
    }
    count_until(&addr, "SELECT count(*) FROM scada", "124", wait);
    pump.abort();
}

/// MQTT over TLS against a real broker (`scripts/mqtt-tls-broker.sh`): a source that trusts the
/// broker's private CA connects, one that also presents a client certificate connects to the
/// mutual-TLS listener, and one that trusts only the public roots is refused and stores nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn mqtt_tls_sources_verify_the_broker_and_present_a_client_certificate() {
    let Ok(certs) = std::env::var("ANKAA_MQTT_TLS_DIR") else {
        eprintln!("skipped: run scripts/mqtt-tls-broker.sh <dir> and set ANKAA_MQTT_TLS_DIR=<dir>");
        return;
    };
    let certs = std::path::PathBuf::from(certs);
    let run = std::process::id();
    let dir = tempfile::tempdir().unwrap();
    let (flight, metrics) = (free_port(), free_port());
    let p = |f: &str| certs.join(f).display().to_string();
    let config = format!(
        r#"
[feed]
enabled = false
streams = []
[server]
flight_addr = "127.0.0.1:{flight}"
metrics_addr = "127.0.0.1:{metrics}"

[[tables]]
name = "readings"
partition_by = "dev"
columns = [
  {{ name = "ts", type = "timestamp_ns" }},
  {{ name = "dev", type = "dict" }},
  {{ name = "v", type = "float64" }},
]

[[sources]]
name = "tls-{run}"
kind = "mqtt"
url = "mqtts://127.0.0.1:18883"
subject = "tls-{run}/#"
tls_ca = "{ca}"
format = "json"
table = "readings"
ts_unit = "millis"
map = {{ ts = "t", dev = "$topic.1", v = "v" }}

[[sources]]
name = "mtls-{run}"
kind = "mqtt"
url = "mqtts://127.0.0.1:18884"
subject = "mtls-{run}/#"
tls_ca = "{ca}"
tls_cert = "{cert}"
tls_key = "{key}"
format = "json"
table = "readings"
ts_unit = "millis"
map = {{ ts = "t", dev = "$topic.1", v = "v" }}

[[sources]]
name = "public-roots-{run}"
kind = "mqtt"
url = "mqtts://127.0.0.1:18883"
subject = "nothing-{run}/#"
format = "json"
table = "readings"
ts_unit = "millis"
map = {{ ts = "t", dev = "$topic.1", v = "v" }}
"#,
        ca = p("ca.pem"),
        cert = p("client.pem"),
        key = p("client.key"),
    );
    let cfg_path = dir.path().join("tls.toml");
    std::fs::write(&cfg_path, config).unwrap();
    let log = dir.path().join("tls.log");
    let _server = Server(
        Command::new(bin())
            .args(["serve", "--config"])
            .arg(&cfg_path)
            .args(["--duration-secs", "60"])
            .env("ANKAA_TOKEN", "t0k3n")
            .env("RUST_LOG", "info")
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .unwrap(),
    );
    // the log is coloured: drop the escape sequences before reading it
    let plain = |path: &std::path::Path| {
        let raw = std::fs::read_to_string(path).unwrap_or_default();
        let mut out = String::with_capacity(raw.len());
        let mut chars = raw.chars();
        while let Some(c) = chars.next() {
            if c == '\u{1b}' {
                for d in chars.by_ref() {
                    if d == 'm' {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    };
    // both verified sources connect (the log line carries tls=true)
    let t0 = Instant::now();
    loop {
        let text = plain(&log);
        if text.matches("mqtt connected").count() >= 2 {
            assert!(text.contains("tls=true"), "{text}");
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(20),
            "TLS sources did not connect:\n{text}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut opts = rumqttc::MqttOptions::new(format!("tls-pub-{run}"), "127.0.0.1", 11883);
    opts.set_keep_alive(Duration::from_secs(10));
    let (client, mut events) = rumqttc::AsyncClient::new(opts, 1_000);
    let pump = tokio::spawn(async move { while events.poll().await.is_ok() {} });
    let base_ms: i64 = 1_762_700_000_000;
    for i in 0..100i64 {
        for prefix in ["tls", "mtls", "nothing"] {
            let msg = format!(r#"{{"t":{},"v":{i}.5}}"#, base_ms + i);
            client
                .publish(
                    format!("{prefix}-{run}/dev-{prefix}"),
                    rumqttc::QoS::AtLeastOnce,
                    false,
                    msg,
                )
                .await
                .unwrap();
        }
    }
    let addr = format!("127.0.0.1:{flight}");
    count_until(
        &addr,
        "SELECT count(*) FROM readings",
        "200",
        Duration::from_secs(20),
    );
    // the source that trusts only the public roots never got in: nothing of it is stored
    let text = plain(&log);
    assert_eq!(text.matches("mqtt connected").count(), 2, "{text}");
    assert!(
        text.contains("mqtt connection lost"),
        "the untrusted connection must fail:\n{text}"
    );
    let devices = first_cell(&query(
        &addr,
        "SELECT count(*) FROM dictionaries WHERE \"table\" = 'readings'",
    ));
    assert_eq!(devices, "2");
    pump.abort();
}
