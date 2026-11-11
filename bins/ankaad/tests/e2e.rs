//! End-to-end tests of the server binary: generic ingest + continuous queries from a file
//! source, and WAL-shipping replication between a primary and a standby. Loopback only.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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

struct Server(Child, PathBuf);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(config: &str, dir: &Path, name: &str, extra: &[&str]) -> Server {
    let path = dir.join(format!("{name}.toml"));
    std::fs::write(&path, config).unwrap();
    let log = std::fs::File::create(dir.join(format!("{name}.log"))).unwrap();
    let child = Command::new(bin())
        .args(["serve", "--config"])
        .arg(&path)
        .args(extra)
        .env("ANKAA_TOKEN", "t0k3n")
        .env("ANKAA_REPL_TOKEN", "r3pl-t0k3n")
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap();
    Server(child, dir.join(format!("{name}.log")))
}

/// SIGTERM and wait for an orderly exit (WAL committed, segments flushed).
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
    loop {
        if server.0.try_wait().unwrap().is_some() {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "server did not stop:\n{}",
            std::fs::read_to_string(&server.1).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let log = std::fs::read_to_string(&server.1).unwrap_or_default();
    assert!(log.contains("serve done"), "{log}");
}

/// Log text without terminal colour sequences.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for d in chars.by_ref() {
                if d.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Poll a single-cell query until it returns `want`.
fn until(addr: &str, sql: &str, want: &str, timeout: Duration) {
    let t0 = Instant::now();
    loop {
        let got = first_cell(&query(addr, sql));
        if got == want {
            return;
        }
        assert!(t0.elapsed() < timeout, "{sql}: got {got}, want {want}");
        std::thread::sleep(Duration::from_millis(200));
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
            "timeout waiting for {needle:?} in {}:\n{}",
            log.display(),
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
        "query failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// First cell of the first data row of a pretty-printed result.
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

#[test]
#[allow(clippy::too_many_lines)]
fn generic_ingest_and_continuous_queries_from_a_file_source() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("telemetry.ndjson");
    let mut f = std::fs::File::create(&data).unwrap();
    // 3 racks × 600 samples, 1 s apart, temperature random walk
    let base = 1_762_700_000_000i64; // ms
    for i in 0..600i64 {
        for (r, rack) in ["rack-1", "rack-2", "rack-3"].iter().enumerate() {
            let temp = 20.0 + f64::from(i32::try_from(i % 37).unwrap()) * 0.1 + r as f64;
            writeln!(
                f,
                r#"{{"meta":{{"time":{},"id":"{rack}"}},"temp":"{temp:.2}","ok":true}}"#,
                base + i * 1000
            )
            .unwrap();
        }
    }
    drop(f);
    let flight = free_port();
    let metrics = free_port();
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
name = "telemetry"
partition_by = "rack"
columns = [
  {{ name = "ts", type = "timestamp_ns" }},
  {{ name = "rack", type = "dict" }},
  {{ name = "temp", type = "fixed8" }},
  {{ name = "ok", type = "u8" }},
]

[[sources]]
name = "racks"
kind = "file"
url = "{data}"
format = "json"
table = "telemetry"
ts_unit = "millis"
map = {{ ts = "meta.time", rack = "meta.id", temp = "temp", ok = "ok" }}

[[continuous]]
name = "temp_1m"
kind = "ohlcv"
table = "telemetry"
key = "rack"
value = "temp"
width_ns = 60000000000

[[continuous]]
name = "temp_p"
kind = "percentiles"
table = "telemetry"
key = "rack"
value = "temp"
width_ns = 300000000000
quantiles = [0.5, 0.99]

[[continuous]]
name = "trades_q"
kind = "asof"
table = "trades"
right_table = "quotes"
"#,
        capture = std::fs::canonicalize("../../bench/data/binance-btcusdt-trade-depth-20s.ankc")
            .unwrap()
            .display(),
        data = data.display(),
    );
    let server = start(&config, dir.path(), "cq", &["--duration-secs", "25"]);
    wait_for(&server.1, "file source consumed", Duration::from_secs(20));
    std::thread::sleep(Duration::from_millis(600));
    let addr = format!("127.0.0.1:{flight}");
    assert_eq!(
        first_cell(&query(&addr, "SELECT count(*) FROM telemetry")),
        "1800"
    );
    // dictionary names are queryable
    let names = query(
        &addr,
        "SELECT count(*) FROM dictionaries WHERE \"table\" = 'telemetry' AND \"column\" = 'rack'",
    );
    assert_eq!(first_cell(&names), "3");
    // the data starts 20 s into a minute, so 600 s touch 11 one-minute buckets per rack:
    // 10 closed bars + 1 live
    assert_eq!(
        first_cell(&query(&addr, "SELECT count(*) FROM cq_temp_1m")),
        "30"
    );
    assert_eq!(
        first_cell(&query(&addr, "SELECT count(*) FROM cq_temp_1m_live")),
        "3"
    );
    // 5-minute windows: 3 touched per rack → 2 closed + 1 live
    assert_eq!(
        first_cell(&query(&addr, "SELECT count(*) FROM cq_temp_p")),
        "6"
    );
    let p = query(&addr, "SELECT min(p50) FROM cq_temp_p");
    let p50: f64 = first_cell(&p).parse().unwrap();
    assert!(
        p50 > 2_000_000_000.0 && p50 < 2_500_000_000.0,
        "p50 of fixed8 temps: {p50}"
    );
    // asof over the market tables: every trade after the first quote has a quote attached
    let n = first_cell(&query(&addr, "SELECT count(*) FROM cq_trades_q"))
        .parse::<i64>()
        .unwrap();
    assert!(n > 100, "asof rows: {n}");
    let m = ureq_get(&format!("http://127.0.0.1:{metrics}/metrics"));
    assert!(m.contains("ankaa_user_rows_total 1800"), "{m}");
    assert!(m.contains("ankaa_cq_late_rows_total 0"), "{m}");
}

#[test]
#[allow(clippy::too_many_lines)]
fn standby_replicates_the_primary_wal_and_serves_the_same_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (p_flight, p_metrics, s_flight, s_metrics) =
        (free_port(), free_port(), free_port(), free_port());
    let capture =
        std::fs::canonicalize("../../bench/data/binance-btcusdt-trade-depth-20s.ankc").unwrap();
    let primary_cfg = format!(
        r#"
[feed]
url = "file://{capture}"
depth_snapshots = false
[server]
flight_addr = "127.0.0.1:{p_flight}"
metrics_addr = "127.0.0.1:{p_metrics}"
[storage]
wal_dir = "{wal}"
snapshot_ms = 100
[replication]
serve_wal = true
token_env = "ANKAA_REPL_TOKEN"
"#,
        capture = capture.display(),
        wal = dir.path().join("primary-wal").display()
    );
    let standby_cfg = format!(
        r#"
[feed]
url = "file://{capture}"
depth_snapshots = false
[server]
flight_addr = "127.0.0.1:{s_flight}"
metrics_addr = "127.0.0.1:{s_metrics}"
[storage]
wal_dir = "{wal}"
snapshot_ms = 100
[replication]
standby_of = "127.0.0.1:{p_flight}"
primary_token_env = "ANKAA_REPL_TOKEN"
"#,
        capture = capture.display(),
        wal = dir.path().join("standby-wal").display()
    );
    // a standby that presents the query token instead of the replication token is refused
    let wrong_token_cfg = standby_cfg
        .replace("primary_token_env = \"ANKAA_REPL_TOKEN\"\n", "")
        .replace("standby-wal", "refused-wal")
        .replace(&format!(":{s_flight}"), &format!(":{}", free_port()))
        .replace(&format!(":{s_metrics}"), &format!(":{}", free_port()));
    let primary = start(
        &primary_cfg,
        dir.path(),
        "primary",
        &["--duration-secs", "30"],
    );
    wait_for(
        &primary.1,
        "serving the WAL to standbys",
        Duration::from_secs(20),
    );
    wait_for(&primary.1, "capture served", Duration::from_secs(20));
    let refused = start(
        &wrong_token_cfg,
        dir.path(),
        "refused",
        &["--duration-secs", "30"],
    );
    wait_for(&refused.1, "WAL stream refused", Duration::from_secs(20));
    drop(refused);
    let standby = start(
        &standby_cfg,
        dir.path(),
        "standby",
        &["--duration-secs", "30"],
    );
    wait_for(
        &standby.1,
        "replication: streaming primary WAL",
        Duration::from_secs(20),
    );
    // let the stream drain
    let p_addr = format!("127.0.0.1:{p_flight}");
    let s_addr = format!("127.0.0.1:{s_flight}");
    let want = first_cell(&query(&p_addr, "SELECT count(*) FROM trades"));
    let t0 = Instant::now();
    loop {
        let got = first_cell(&query(&s_addr, "SELECT count(*) FROM trades"));
        if got == want {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(20),
            "standby has {got} trades, primary {want}"
        );
        std::thread::sleep(Duration::from_millis(300));
    }
    assert_eq!(
        first_cell(&query(&s_addr, "SELECT count(*) FROM book_deltas")),
        first_cell(&query(&p_addr, "SELECT count(*) FROM book_deltas"))
    );
    let m = ureq_get(&format!("http://127.0.0.1:{s_metrics}/metrics"));
    assert!(m.contains("ankaa_replication_connected 1"), "{m}");
    let lsn_line = m
        .lines()
        .find(|l| l.starts_with("ankaa_replication_received_lsn "))
        .unwrap();
    let lsn: u64 = lsn_line.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(lsn >= 400, "received lsn {lsn}");
    // the standby wrote the same records into its own WAL
    let wal_files: Vec<_> = std::fs::read_dir(dir.path().join("standby-wal"))
        .unwrap()
        .flatten()
        .collect();
    assert!(!wal_files.is_empty());
    assert!(
        m.contains(&format!("ankaa_replication_applied_lsn {lsn}")),
        "{m}"
    );
    let pm = ureq_get(&format!("http://127.0.0.1:{p_metrics}/metrics"));
    assert!(pm.contains("ankaa_replication_wal_streams 1"), "{pm}");
    assert!(
        !pm.contains("ankaa_replication_wal_refused_total 0"),
        "the wrong-token standby was counted: {pm}"
    );
    // restart the standby: it continues at the end of its own log (same LSNs as the primary)
    // and neither loses nor repeats a row
    let trades = first_cell(&query(&s_addr, "SELECT count(*) FROM trades"));
    let deltas = first_cell(&query(&s_addr, "SELECT count(*) FROM book_deltas"));
    stop(standby);
    let standby = start(
        &standby_cfg,
        dir.path(),
        "standby2",
        &["--duration-secs", "30"],
    );
    wait_for(
        &standby.1,
        "replication: streaming primary WAL",
        Duration::from_secs(20),
    );
    let log = strip_ansi(&std::fs::read_to_string(&standby.1).unwrap());
    assert!(
        log.contains(&format!("from_lsn={}", lsn + 1)),
        "resumes after lsn {lsn}: {log}"
    );
    until(
        &s_addr,
        "SELECT count(*) FROM trades",
        &trades,
        Duration::from_secs(20),
    );
    until(
        &s_addr,
        "SELECT count(*) FROM book_deltas",
        &deltas,
        Duration::from_secs(20),
    );
    assert_eq!(
        first_cell(&query(&s_addr, "SELECT count(*) FROM trades")),
        first_cell(&query(&p_addr, "SELECT count(*) FROM trades"))
    );
}

/// Restarts (clean and crash) of a server with a file source, a dictionary column, segments
/// and continuous queries: no row is lost or ingested twice, dictionary names keep their ids,
/// and the continuous-query results are the same before and after.
#[test]
#[allow(clippy::too_many_lines)]
fn restarts_keep_rows_dictionaries_positions_and_continuous_results() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("telemetry.csv");
    let mut f = std::fs::File::create(&data).unwrap();
    let base = 1_762_700_000_000i64; // ms
    // first sight order rack-2, rack-0, rack-1: dictionary ids are not alphabetical
    let racks = ["rack-2", "rack-0", "rack-1"];
    // the devices send in blocks of seven seconds, newest first (out of time order, within the
    // table's allowed lateness of ten minutes); now and then a reading an hour old turns up,
    // beyond the lateness: it is dropped and counted, whatever its value
    let mut written = 0;
    for block in 0..(30_000i64 + 6) / 7 {
        for j in (block * 7..(block * 7 + 7).min(30_000)).rev() {
            // humidity is missing from every ninth reading (an empty field: NULL)
            let hum = if j % 9 == 0 {
                String::new()
            } else {
                format!("{}", 40 + j % 20)
            };
            for (r, rack) in racks.iter().enumerate() {
                writeln!(
                    f,
                    "{},{rack},{}.{:02},{hum}",
                    base + j * 1000,
                    20 + r,
                    j % 100
                )
                .unwrap();
            }
            written += 1;
            if written % 5_000 == 0 {
                for rack in &racks {
                    writeln!(f, "{},{rack},99.99,", base + j * 1000 - 3_600_000).unwrap();
                }
            }
        }
    }
    drop(f);
    let empty = dir.path().join("empty.csv");
    std::fs::write(&empty, "").unwrap();
    let flight = free_port();
    let metrics = free_port();
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
hot_sealed_chunks = 1
checkpoint_interval_s = 1

[[tables]]
name = "telemetry"
partition_by = "rack"
max_lateness_ms = 600000
columns = [
  {{ name = "ts", type = "timestamp_ns" }},
  {{ name = "rack", type = "dict" }},
  {{ name = "temp", type = "fixed8" }},
  {{ name = "humidity", type = "float64", nullable = true }},
]

[[sources]]
name = "racks"
kind = "file"
url = "{data}"
format = "csv"
table = "telemetry"
ts_unit = "millis"
map = {{ ts = "0", rack = "1", temp = "2", humidity = "3" }}

[[continuous]]
name = "temp_1m"
kind = "ohlcv"
table = "telemetry"
key = "rack"
value = "temp"
width_ns = 60000000000

[[continuous]]
name = "temp_p"
kind = "percentiles"
table = "telemetry"
key = "rack"
value = "temp"
width_ns = 300000000000
quantiles = [0.5]
"#,
        wal = dir.path().join("wal").display(),
        ckpt = dir.path().join("ckpt").display(),
        data = data.display(),
    );
    let addr = format!("127.0.0.1:{flight}");
    let named_rows = "SELECT count(*) FROM telemetry t JOIN dictionaries d ON d.id = t.rack \
                 WHERE d.\"table\" = 'telemetry' AND d.name = 'rack-2'";
    let fingerprint = |addr: &str| -> Vec<String> {
        [
            "SELECT count(*) FROM telemetry",
            "SELECT count(*) FROM cq_temp_1m",
            "SELECT sum(close) + sum(open) + sum(high) + sum(low) + sum(count) FROM cq_temp_1m",
            "SELECT count(*) FROM cq_temp_1m_live",
            "SELECT sum(close) + sum(count) FROM cq_temp_1m_live",
            "SELECT count(*) FROM cq_temp_p",
            "SELECT sum(count) FROM cq_temp_p",
            "SELECT count(*) FROM dictionaries",
            named_rows,
            "SELECT min(temp) FROM telemetry t JOIN dictionaries d ON d.id = t.rack WHERE d.name = 'rack-0'",
            "SELECT max(temp) FROM telemetry",
            "SELECT count(*) FROM (SELECT ts, rack FROM telemetry GROUP BY ts, rack) x",
            "SELECT count(humidity) FROM telemetry",
            "SELECT min(humidity) + max(humidity) FROM telemetry",
        ]
        .iter()
        .map(|q| first_cell(&query(addr, q)))
        .collect()
    };
    let long = Duration::from_secs(30);

    // 1. first run: everything is ingested once
    let s = start(&config, dir.path(), "run1", &[]);
    wait_for(&s.1, "file source consumed", long);
    until(&addr, "SELECT count(*) FROM telemetry", "90000", long);
    let before = fingerprint(&addr);
    assert_eq!(before[8], "30000");
    assert_eq!(before[7], "3");
    assert_eq!(before[9], "2100000000", "rack-0 is the second rack seen");
    assert_eq!(
        before[10], "2299000000",
        "no reading beyond the lateness was stored"
    );
    assert_eq!(before[11], "90000", "every reading once");
    assert_eq!(before[12], "79998", "missing humidity readings are NULL");
    assert_eq!(before[13], "99.0");
    let m = ureq_get(&format!("http://127.0.0.1:{metrics}/metrics"));
    let late: u64 = m
        .lines()
        .find_map(|l| l.strip_prefix("ankaa_late_rows_total "))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    assert!(late > 50_000, "out-of-order readings accepted: {late}\n{m}");
    assert!(m.contains("ankaa_cq_late_rows_total 0"), "{m}");
    stop(s);
    assert!(dir.path().join("ckpt").join("dictionaries.json").exists());
    assert!(dir.path().join("wal").join("sources.json").exists());

    // 2. clean restart: the file is not ingested again, results are identical
    let s = start(&config, dir.path(), "run2", &[]);
    wait_for(&s.1, "file source resumes after", long);
    wait_for(&s.1, "file source consumed", long);
    wait_for(&s.1, "continuous queries rebuilt", long);
    until(&addr, "SELECT count(*) FROM telemetry", "90000", long);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(fingerprint(&addr), before, "after a clean restart");
    stop(s);

    // 3. lines appended while the server was down are picked up, once
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&data)
        .unwrap();
    for (r, rack) in racks.iter().enumerate() {
        writeln!(f, "{},{rack},{}.50,", base + 30_000 * 1000, 20 + r).unwrap();
    }
    drop(f);
    let s = start(&config, dir.path(), "run3", &[]);
    wait_for(&s.1, "file source consumed", long);
    until(&addr, "SELECT count(*) FROM telemetry", "90003", long);
    // let the position of the new lines become durable, then crash (SIGKILL)
    std::thread::sleep(Duration::from_millis(1500));
    let after_append = fingerprint(&addr);
    assert_eq!(after_append[8], "30001");
    drop(s);

    // 4. crash recovery: WAL replay brings the rows back, nothing is repeated
    let s = start(&config, dir.path(), "run4", &[]);
    wait_for(&s.1, "wal recovery done", long);
    wait_for(&s.1, "file source consumed", long);
    until(&addr, "SELECT count(*) FROM telemetry", "90003", long);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(fingerprint(&addr), after_append, "after a crash");
    stop(s);

    // 5. the WAL is gone (as after truncation): rows come from segments and the dictionary
    //    from its own file, so names still resolve to the right rows
    std::fs::remove_dir_all(dir.path().join("wal")).unwrap();
    let config5 = config.replace(&data.display().to_string(), &empty.display().to_string());
    let s = start(&config5, dir.path(), "run5", &[]);
    wait_for(&s.1, "file source consumed", long);
    until(&addr, "SELECT count(*) FROM telemetry", "90003", long);
    assert_eq!(fingerprint(&addr), after_append, "without the WAL");
    let m = ureq_get(&format!("http://127.0.0.1:{metrics}/metrics"));
    assert!(m.contains("ankaa_dictionary_entries 3"), "{m}");
    assert!(m.contains("ankaa_cq_rejected_rows_total 0"), "{m}");
    stop(s);
}

/// Minimal HTTP GET with the bearer token (no extra dependency).
fn ureq_get(url: &str) -> String {
    use std::io::{Read, Write};
    let rest = url.strip_prefix("http://").unwrap();
    let (hostport, path) = rest.split_once('/').unwrap();
    let mut s = std::net::TcpStream::connect(hostport).unwrap();
    write!(s, "GET /{path} HTTP/1.1\r\nHost: {hostport}\r\nAuthorization: Bearer t0k3n\r\nConnection: close\r\n\r\n").unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    out
}

/// Messages that carry many readings (`rows`): one message is one WAL record holding up to a
/// thousand rows, so records straddle the 65,536-row chunk boundaries. Every reading must be
/// stored once: after a clean restart, after a crash, and with the WAL gone.
#[test]
#[allow(clippy::too_many_lines)]
fn rows_messages_are_stored_exactly_once_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("batches.ndjson");
    let mut f = std::fs::File::create(&data).unwrap();
    let base = 1_762_700_000_000i64; // ms
    let per_msg = 1000i64;
    // 150 messages of 1,000 readings per device, the two devices interleaved: 150,000 rows each,
    // two chunk boundaries inside messages for both
    for m in 0..150i64 {
        for dev in ["m1", "m2"] {
            write!(f, "{{\"dev\":\"{dev}\",\"samples\":[").unwrap();
            for k in 0..per_msg {
                let i = m * per_msg + k;
                if k > 0 {
                    write!(f, ",").unwrap();
                }
                if i % 7 == 0 {
                    write!(f, "{{\"t\":{}}}", base + i).unwrap(); // no value: NULL
                } else {
                    write!(f, "{{\"t\":{},\"v\":{}.5}}", base + i, i % 100).unwrap();
                }
            }
            writeln!(f, "]}}").unwrap();
        }
    }
    drop(f);
    let empty = dir.path().join("empty.ndjson");
    std::fs::write(&empty, "").unwrap();
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
hot_sealed_chunks = 1
checkpoint_interval_s = 1

[[tables]]
name = "readings"
partition_by = "dev"
columns = [
  {{ name = "ts", type = "timestamp_ns" }},
  {{ name = "dev", type = "dict" }},
  {{ name = "v", type = "float64", nullable = true }},
]

[[sources]]
name = "batches"
kind = "file"
url = "{data}"
format = "json"
table = "readings"
ts_unit = "millis"
rows = "samples"
map = {{ ts = "t", dev = "$msg.dev", v = "v" }}
"#,
        wal = dir.path().join("wal").display(),
        ckpt = dir.path().join("ckpt").display(),
        data = data.display(),
    );
    let addr = format!("127.0.0.1:{flight}");
    let fingerprint = |addr: &str| -> Vec<String> {
        [
            "SELECT count(*) FROM readings",
            "SELECT count(v) FROM readings",
            "SELECT count(*) FROM (SELECT dev, ts FROM readings GROUP BY dev, ts) x",
            "SELECT sum(v) FROM readings",
            "SELECT min(ts) FROM readings",
            "SELECT max(ts) FROM readings",
        ]
        .iter()
        .map(|q| first_cell(&query(addr, q)))
        .collect()
    };
    let long = Duration::from_secs(60);
    let s = start(&config, dir.path(), "run1", &[]);
    wait_for(&s.1, "flight listening", long);
    wait_for(&s.1, "file source consumed", long);
    until(&addr, "SELECT count(*) FROM readings", "300000", long);
    let before = fingerprint(&addr);
    assert_eq!(before[0], "300000");
    assert_eq!(before[2], "300000", "every reading once");
    // 150,000 readings per device, every seventh without a value
    assert_eq!(
        before[1],
        (2 * (150_000 - 150_000_i64.div_euclid(7) - 1)).to_string()
    );
    let m = ureq_get(&format!("http://127.0.0.1:{metrics}/metrics"));
    assert!(m.contains("ankaa_user_rows_total 300000"), "{m}");
    assert!(m.contains("ankaa_user_rejected_total 0"), "{m}");
    stop(s);

    // clean restart: identical
    let s = start(&config, dir.path(), "run2", &[]);
    wait_for(&s.1, "flight listening", long);
    wait_for(&s.1, "file source consumed", long);
    until(&addr, "SELECT count(*) FROM readings", "300000", long);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(fingerprint(&addr), before, "after a clean restart");
    // crash (SIGKILL) and recover: identical
    drop(s);
    let s = start(&config, dir.path(), "run3", &[]);
    wait_for(&s.1, "flight listening", long);
    until(&addr, "SELECT count(*) FROM readings", "300000", long);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(fingerprint(&addr), before, "after a crash");
    stop(s);
    // the WAL is gone: every row comes from the segments
    std::fs::remove_dir_all(dir.path().join("wal")).unwrap();
    let config4 = config.replace(&data.display().to_string(), &empty.display().to_string());
    let s = start(&config4, dir.path(), "run4", &[]);
    wait_for(&s.1, "flight listening", long);
    until(&addr, "SELECT count(*) FROM readings", "300000", long);
    assert_eq!(fingerprint(&addr), before, "without the WAL");
    stop(s);
}
