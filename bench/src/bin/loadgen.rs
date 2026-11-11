//! Telemetry load generator for end-to-end profiling (`scripts/e2e-profile.sh`).
//!
//! Publishes realistic device readings to an MQTT broker as fast as the broker takes them, or
//! writes the same messages to a file (NDJSON) so the engine can be measured without a broker in
//! the path. Every reading carries a distinct, increasing timestamp per device, so the receiving
//! table accepts every one and the row count is an exact check.
//!
//! ```text
//! loadgen --messages 1000000 --devices 1000 --format json        --mqtt mqtt://127.0.0.1:1883
//! loadgen --messages  100000 --devices 1000 --format json-array --batch 10 --file /tmp/x.ndjson
//! ```

use std::io::Write as _;
use std::time::{Duration, Instant};

use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    /// One reading per message.
    Json,
    /// `--batch` readings per message, in a `samples` array.
    JsonArray,
}

struct Args {
    messages: u64,
    devices: u64,
    format: Format,
    batch: u64,
    mqtt: Option<String>,
    file: Option<String>,
    qos: QoS,
    prefix: String,
}

fn usage() -> ! {
    eprintln!(
        "usage: loadgen --messages N [--devices D] [--format json|json-array] [--batch B]\n\
         \x20              (--mqtt mqtt://host:port | --file path) [--qos 0|1] [--prefix bench]"
    );
    std::process::exit(2)
}

fn parse_args() -> Args {
    let mut a = Args {
        messages: 0,
        devices: 1000,
        format: Format::Json,
        batch: 10,
        mqtt: None,
        file: None,
        qos: QoS::AtLeastOnce,
        prefix: "bench".into(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut v = || it.next().unwrap_or_else(|| usage());
        match k.as_str() {
            "--messages" => a.messages = v().parse().unwrap_or_else(|_| usage()),
            "--devices" => a.devices = v().parse().unwrap_or_else(|_| usage()),
            "--batch" => a.batch = v().parse().unwrap_or_else(|_| usage()),
            "--format" => {
                a.format = match v().as_str() {
                    "json" => Format::Json,
                    "json-array" => Format::JsonArray,
                    _ => usage(),
                }
            }
            "--mqtt" => a.mqtt = Some(v()),
            "--file" => a.file = Some(v()),
            "--qos" => {
                a.qos = match v().as_str() {
                    "0" => QoS::AtMostOnce,
                    "1" => QoS::AtLeastOnce,
                    _ => usage(),
                }
            }
            "--prefix" => a.prefix = v(),
            _ => usage(),
        }
    }
    if a.messages == 0 || a.devices == 0 || a.batch == 0 || a.mqtt.is_some() == a.file.is_some() {
        usage();
    }
    a
}

/// Deterministic xorshift: the same run produces the same values.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// One reading of device `d` at sample `i`: append its JSON object (no surrounding braces
/// for the batched form's shared fields) to `out`.
fn reading(out: &mut Vec<u8>, rng: &mut Rng, ts_ms: u64, with_device: bool, d: u64) {
    let temp = 20.0 + 15.0 * rng.unit();
    let kw = 5.0 + 50.0 * rng.unit();
    out.push(b'{');
    if with_device {
        write!(out, "\"dev\":\"dev-{d:05}\",").unwrap();
    }
    write!(
        out,
        "\"t\":{ts_ms},\"temp\":{temp:.2},\"kw\":{kw:.3},\"ok\":{}}}",
        u8::from(rng.next() % 50 != 0)
    )
    .unwrap();
}

/// Message `m` (device `m % devices`): topic and payload.
fn message(a: &Args, m: u64, rng: &mut Rng, payload: &mut Vec<u8>) -> String {
    let d = m % a.devices;
    let round = m / a.devices;
    // 1 ms apart per reading of a device; batches cover consecutive milliseconds
    let base_ms = 1_762_000_000_000 + round * a.batch.max(1);
    payload.clear();
    match a.format {
        Format::Json => reading(payload, rng, base_ms, true, d),
        Format::JsonArray => {
            write!(payload, "{{\"dev\":\"dev-{d:05}\",\"samples\":[").unwrap();
            for k in 0..a.batch {
                if k > 0 {
                    payload.push(b',');
                }
                reading(payload, rng, base_ms + k, false, d);
            }
            payload.extend_from_slice(b"]}");
        }
    }
    format!("{}/line{}/dev-{d:05}/telemetry", a.prefix, d % 8)
}

fn readings_per_message(a: &Args) -> u64 {
    match a.format {
        Format::Json => 1,
        Format::JsonArray => a.batch,
    }
}

fn to_file(a: &Args, path: &str) {
    let f = std::fs::File::create(path).expect("create output file");
    let mut w = std::io::BufWriter::with_capacity(1 << 20, f);
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut payload = Vec::with_capacity(4096);
    let t0 = Instant::now();
    for m in 0..a.messages {
        message(a, m, &mut rng, &mut payload);
        w.write_all(&payload).unwrap();
        w.write_all(b"\n").unwrap();
    }
    w.flush().unwrap();
    eprintln!(
        "loadgen: wrote {} messages ({} readings) to {path} in {:?}",
        a.messages,
        a.messages * readings_per_message(a),
        t0.elapsed()
    );
}

async fn to_mqtt(a: &Args, url: &str) {
    let rest = url.strip_prefix("mqtt://").expect("mqtt:// URL");
    let (host, port) = rest.rsplit_once(':').map_or((rest, 1883), |(h, p)| (h, p.parse().unwrap()));
    let mut o = MqttOptions::new(format!("loadgen-{}", std::process::id()), host, port);
    o.set_keep_alive(Duration::from_secs(30));
    o.set_inflight(1000);
    o.set_max_packet_size(1 << 20, 1 << 20);
    let (client, mut events) = AsyncClient::new(o, 10_000);
    let target = a.messages;
    let qos = a.qos;
    // the event loop counts acknowledgements (QoS 1) or sent publishes (QoS 0)
    let done = tokio::spawn(async move {
        let mut acked = 0u64;
        loop {
            match events.poll().await {
                Ok(Event::Incoming(Packet::PubAck(_))) if qos == QoS::AtLeastOnce => acked += 1,
                Ok(Event::Outgoing(Outgoing::Publish(_))) if qos == QoS::AtMostOnce => acked += 1,
                Ok(_) => {}
                Err(e) => {
                    eprintln!("loadgen: connection error: {e}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            if acked >= target {
                return acked;
            }
        }
    });
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut payload = Vec::with_capacity(4096);
    let t0 = Instant::now();
    for m in 0..a.messages {
        let topic = message(a, m, &mut rng, &mut payload);
        client.publish(topic, qos, false, payload.clone()).await.unwrap();
    }
    let acked = done.await.unwrap();
    let secs = t0.elapsed().as_secs_f64();
    eprintln!(
        "loadgen: published {acked} messages ({} readings) in {secs:.2} s: {:.0} msg/s",
        acked * readings_per_message(a),
        acked as f64 / secs
    );
    let _ = client.disconnect().await;
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let a = parse_args();
    if let Some(path) = a.file.clone() {
        to_file(&a, &path);
    } else if let Some(url) = a.mqtt.clone() {
        to_mqtt(&a, &url).await;
    }
}
