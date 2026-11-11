//! Per-function microbenchmarks of the ingest and kernel hot paths.
//! Run: `cd bench && cargo bench --bench hot_paths -- --noplot` (release, native CPU).

use ankaa_feeds::binance::{BinanceJsonDecoder, BinanceSbeDecoder};
use ankaa_feeds::capture::{Frame, FrameKind, decode_frame, encode_frame};
use ankaa_ingest::SymbolDict;
use ankaa_ingest::{Decoder, EventBatch, parse_fixed};
use ankaa_simd::scalar::ScalarBackend;
use ankaa_simd::{BackendKind, CmpOp, SelVec, SimdBackend, VECTOR_LEN};
use ankaa_store::{Chunk, ChunkId, Schema, Value};
use ankaa_ts::{AsofDirection, BookSide, L2Book, asof_join_keyed, time_buckets};
use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

const TRADE: &[u8] = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1794231662266,"s":"BTCUSDT","t":6832857732,"p":"74670.01000000","q":"0.00400000","T":1794231662265,"m":false,"M":true}}"#;
const DEPTH: &[u8] = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1794231662214,"s":"BTCUSDT","U":102523803235,"u":102523803253,"b":[["74663.56000000","0.00015000"],["74663.07000000","0.00000000"],["74660.00000000","1.23456789"],["74659.99000000","0.50000000"],["74655.00000000","0.00000000"]],"a":[["74670.01000000","0.41867000"],["74670.02000000","0.00070000"],["74671.43000000","0.00000000"],["74672.00000000","2.00000000"],["74675.50000000","0.10000000"]]}}"#;

fn backends() -> Vec<Box<dyn SimdBackend>> {
    let mut v: Vec<Box<dyn SimdBackend>> = vec![Box::new(ScalarBackend)];
    for k in [BackendKind::Neon, BackendKind::Sve2] {
        if let Ok(b) = ankaa_simd::dispatch::make(k) {
            v.push(b);
        }
    }
    v
}

fn simd(c: &mut Criterion) {
    let f32s: Vec<f32> = (0..VECTOR_LEN)
        .map(|i| (i as f32 * 0.37).sin() * 100.0)
        .collect();
    let i64s: Vec<i64> = (0..VECTOR_LEN).map(|i| (i as i64 * 7919) % 1000).collect();
    let mut g = c.benchmark_group("simd_1024");
    g.throughput(Throughput::Elements(VECTOR_LEN as u64));
    for b in backends() {
        let k = b.kind();
        g.bench_with_input(
            BenchmarkId::new("filter_range_f32_half", k),
            &f32s,
            |bch, d| {
                let mut sel = SelVec::new();
                bch.iter(|| black_box(b.filter_range_f32(black_box(d), -50.0, 50.0, &mut sel)));
            },
        );
        g.bench_with_input(
            BenchmarkId::new("filter_range_i64_half", k),
            &i64s,
            |bch, d| {
                let mut sel = SelVec::new();
                bch.iter(|| black_box(b.filter_range_i64(black_box(d), 250, 750, &mut sel)));
            },
        );
        g.bench_with_input(BenchmarkId::new("sum_f32", k), &f32s, |bch, d| {
            bch.iter(|| black_box(b.sum_f32(black_box(d))))
        });
        g.bench_with_input(BenchmarkId::new("sum_i64", k), &i64s, |bch, d| {
            bch.iter(|| black_box(b.sum_i64(black_box(d))))
        });
        g.bench_with_input(BenchmarkId::new("min_max_i64", k), &i64s, |bch, d| {
            bch.iter(|| black_box(b.min_max_i64(black_box(d))))
        });
        g.bench_with_input(BenchmarkId::new("count_i64_gt", k), &i64s, |bch, d| {
            bch.iter(|| black_box(b.count_i64(black_box(d), CmpOp::Gt(500))))
        });
        g.bench_with_input(BenchmarkId::new("min_max_f32", k), &f32s, |bch, d| {
            bch.iter(|| black_box(b.min_max_f32(black_box(d))))
        });
        g.bench_with_input(BenchmarkId::new("count_f32_gt", k), &f32s, |bch, d| {
            bch.iter(|| black_box(b.count_f32(black_box(d), CmpOp::Gt(0.0))))
        });
    }
    g.finish();
}

fn decode(c: &mut Criterion) {
    let mut g = c.benchmark_group("decode");
    g.throughput(Throughput::Bytes(TRADE.len() as u64));
    g.bench_function("parse_fixed_price", |b| {
        b.iter(|| black_box(parse_fixed(black_box(b"74670.01000000"), 8)))
    });
    g.bench_function("binance_trade_json", |b| {
        let mut d = BinanceJsonDecoder::new();
        let mut out = EventBatch::with_capacity(16);
        b.iter(|| {
            out.clear();
            black_box(d.decode(black_box(TRADE), 1, &mut out)).unwrap()
        });
    });
    g.throughput(Throughput::Bytes(DEPTH.len() as u64));
    g.bench_function("binance_depth10_json", |b| {
        let mut d = BinanceJsonDecoder::new();
        let mut out = EventBatch::with_capacity(16);
        b.iter(|| {
            out.clear();
            black_box(d.decode(black_box(DEPTH), 1, &mut out)).unwrap()
        });
    });
    // SBE (binary) equivalents of the two JSON frames above, encoded per stream_1_0.xml
    let sbe_trade = sbe_trade_frame();
    let sbe_depth = sbe_depth_frame(5, 5);
    g.throughput(Throughput::Bytes(sbe_trade.len() as u64));
    g.bench_function("binance_trade_sbe", |b| {
        let mut d = BinanceSbeDecoder::new();
        let mut syms = SymbolDict::new();
        let mut out = EventBatch::with_capacity(16);
        b.iter(|| {
            out.clear();
            black_box(d.decode(black_box(&sbe_trade), 1, &mut syms, &mut out)).unwrap()
        });
    });
    g.throughput(Throughput::Bytes(sbe_depth.len() as u64));
    g.bench_function("binance_depth10_sbe", |b| {
        let mut d = BinanceSbeDecoder::new();
        let mut syms = SymbolDict::new();
        let mut out = EventBatch::with_capacity(16);
        b.iter(|| {
            out.clear();
            black_box(d.decode(black_box(&sbe_depth), 1, &mut syms, &mut out)).unwrap()
        });
    });
    g.bench_function("capture_frame_encode_decode", |b| {
        let f = Frame {
            recv_ns: 1,
            kind: FrameKind::Text,
            payload: Bytes::from_static(DEPTH),
        };
        let mut buf = BytesMut::with_capacity(1024);
        b.iter(|| {
            buf.clear();
            encode_frame(&f, &mut buf);
            black_box(decode_frame(&buf, 0).unwrap())
        });
    });
    g.finish();
}

fn sbe_header(template: u16, block_len: u16) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&block_len.to_le_bytes());
    v.extend_from_slice(&template.to_le_bytes());
    v.extend_from_slice(&1u16.to_le_bytes());
    v.extend_from_slice(&0u16.to_le_bytes());
    v
}

fn sbe_trade_frame() -> Vec<u8> {
    let mut v = sbe_header(10000, 18);
    v.extend_from_slice(&1_794_231_662_266_123i64.to_le_bytes());
    v.extend_from_slice(&1_794_231_662_265_000i64.to_le_bytes());
    v.push((-8i8) as u8);
    v.push((-8i8) as u8);
    v.extend_from_slice(&25u16.to_le_bytes());
    v.extend_from_slice(&1u32.to_le_bytes());
    v.extend_from_slice(&6_832_857_732i64.to_le_bytes());
    v.extend_from_slice(&7_467_001_000_000i64.to_le_bytes());
    v.extend_from_slice(&400_000i64.to_le_bytes());
    v.push(0);
    v.push(7);
    v.extend_from_slice(b"BTCUSDT");
    v
}

fn sbe_depth_frame(bids: u16, asks: u16) -> Vec<u8> {
    let mut v = sbe_header(10003, 26);
    v.extend_from_slice(&1_794_231_662_214_000i64.to_le_bytes());
    v.extend_from_slice(&102_523_803_235i64.to_le_bytes());
    v.extend_from_slice(&102_523_803_253i64.to_le_bytes());
    v.push((-8i8) as u8);
    v.push((-8i8) as u8);
    for n in [bids, asks] {
        v.extend_from_slice(&16u16.to_le_bytes());
        v.extend_from_slice(&n.to_le_bytes());
        for i in 0..i64::from(n) {
            v.extend_from_slice(&(7_466_356_000_000 - i * 1_000_000).to_le_bytes());
            v.extend_from_slice(&(15_000 + i).to_le_bytes());
        }
    }
    v.push(7);
    v.extend_from_slice(b"BTCUSDT");
    v
}

fn store(c: &mut Criterion) {
    let mut g = c.benchmark_group("store");
    g.throughput(Throughput::Elements(1));
    g.bench_function("chunk_append_row", |b| {
        let mut chunk = Chunk::new(ChunkId(0), Schema::trades());
        let mut ts = 0i64;
        b.iter(|| {
            if chunk.is_full() {
                chunk = Chunk::new(ChunkId(0), Schema::trades());
            }
            ts += 1;
            chunk
                .append_row(&[
                    Value::I64(ts),
                    Value::U32(1),
                    Value::I64(5_000_000_000_000),
                    Value::I64(100),
                    Value::U8(0),
                    Value::I64(ts),
                ])
                .unwrap();
        });
    });
    g.bench_function("l2book_apply", |b| {
        let mut book = L2Book::new();
        for i in 0..500i64 {
            book.apply(BookSide::Bid, 74_000_00000000 - i * 1_000_000, 1, i);
            book.apply(BookSide::Ask, 74_001_00000000 + i * 1_000_000, 1, i);
        }
        let mut i = 0i64;
        b.iter(|| {
            i += 1;
            let p = 74_000_00000000 - (i % 500) * 1_000_000;
            black_box(book.apply(BookSide::Bid, p, (i % 3) * 100, i))
        });
    });
    g.finish();
}

fn operators(c: &mut Criterion) {
    let n = 100_000usize;
    let lt: Vec<i64> = (0..n).map(|i| i as i64 * 20 + 5).collect();
    let rt: Vec<i64> = (0..2 * n).map(|i| i as i64 * 10).collect();
    let lk: Vec<u32> = (0..n).map(|i| (i % 8) as u32).collect();
    let rk: Vec<u32> = (0..2 * n).map(|i| (i % 8) as u32).collect();
    let px: Vec<i64> = (0..n).map(|i| 1000 + (i % 50) as i64).collect();
    let sz: Vec<i64> = (0..n).map(|i| 1 + (i % 7) as i64).collect();
    let mut g = c.benchmark_group("operators_100k");
    g.throughput(Throughput::Elements(n as u64));
    g.bench_function("asof_join_keyed_backward", |b| {
        let mut out = vec![0u32; n];
        b.iter(|| {
            black_box(asof_join_keyed(
                &lt,
                &lk,
                &rt,
                &rk,
                AsofDirection::Backward,
                None,
                &mut out,
            ))
        });
    });
    g.bench_function("time_buckets_1000ns", |b| {
        let mut out = Vec::new();
        b.iter(|| black_box(time_buckets(&lt, &px, &sz, 1000, &mut out)));
    });
    g.finish();
}

fn wal(c: &mut Criterion) {
    let dir = std::env::temp_dir().join(format!("ankaa-bench-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut g = c.benchmark_group("wal");
    g.throughput(Throughput::Bytes(DEPTH.len() as u64 + 24));
    // Appends land in an in-memory buffer until a commit. Measure them in batches of at most
    // 65,536 records on a fresh writer, so the buffer stays bounded (an unbounded run filled
    // the 16 GB of a cloud VM); opening and dropping the writer is not timed.
    g.bench_function("append_no_commit", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            let mut left = iters;
            while left > 0 {
                let n = left.min(65_536);
                let _ = std::fs::remove_dir_all(&dir);
                let (mut w, _) =
                    ankaa_wal::Wal::open(&dir, ankaa_wal::WalConfig::default()).unwrap();
                let t0 = std::time::Instant::now();
                for _ in 0..n {
                    black_box(w.append(1, black_box(DEPTH)).unwrap());
                }
                total += t0.elapsed();
                drop(w);
                left -= n;
            }
            total
        });
    });
    g.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Sealed trades chunk with realistic value distributions (random-walk price, small sizes).
fn tier_chunk(rows: usize) -> Chunk {
    let mut c = Chunk::new(ChunkId(1), Schema::trades());
    let mut s = 0x9E37_79B9_7F4A_7C15u64;
    let mut px = 65_000_00000000i64;
    for i in 0..rows {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        px += (s % 5) as i64 * 1_000_000 - 2_000_000;
        c.append_row(&[
            Value::I64(1_700_000_000_000_000_000 + i as i64 * 1_000),
            Value::U32((i % 3) as u32),
            Value::I64(px),
            Value::I64((s % 1_000) as i64 * 1_000_000),
            Value::U8((s % 2) as u8),
            Value::I64(i as i64),
        ])
        .unwrap();
    }
    c.seal();
    c
}

fn tiers(c: &mut Criterion) {
    use ankaa_store::EncodedChunk;
    let enc = std::sync::Arc::new(ankaa_tier::Encoder::new());
    let chunk = tier_chunk(65_536);
    let raw = ankaa_tier::raw_bytes(&chunk) as u64;
    let mut g = c.benchmark_group("tier");
    g.throughput(Throughput::Bytes(raw));
    g.bench_function("encode_warm_64k", |b| {
        b.iter(|| enc.encode_warm("trades", 0, black_box(&chunk)).unwrap());
    });
    let warm = enc.encode_warm("trades", 0, &chunk).unwrap();
    eprintln!(
        "tier: raw {raw} B -> warm {} B ({:.2}x)",
        warm.encoded_bytes(),
        raw as f64 / warm.encoded_bytes() as f64
    );
    g.bench_function("decode_warm_all_columns", |b| {
        b.iter(|| warm.decode(black_box(&[0, 1, 2, 3, 4, 5])).unwrap());
    });
    g.bench_function("decode_warm_ts_price", |b| {
        b.iter(|| warm.decode(black_box(&[0, 2])).unwrap());
    });
    let dir = std::env::temp_dir().join(format!("ankaa-bench-tier-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    g.bench_function("write_cold_64k", |b| {
        b.iter(|| enc.write_cold(&dir, black_box(&warm)).unwrap());
    });
    let cold = enc.write_cold(&dir, &warm).unwrap();
    g.bench_function("read_cold_ts_price", |b| {
        b.iter(|| cold.decode(black_box(&[0, 2])).unwrap());
    });
    g.bench_function("read_cold_all_columns", |b| {
        b.iter(|| cold.decode(black_box(&[0, 1, 2, 3, 4, 5])).unwrap());
    });
    g.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

fn framing(c: &mut Criterion) {
    use ankaa_ingest::framing::{Framer, LengthPrefixed, Prefix};
    // 1,024 frames of 48 bytes with a 2-byte LE length (payload only)
    let mut stream = Vec::new();
    for i in 0..1024u32 {
        stream.extend(48u16.to_le_bytes());
        stream.extend(std::iter::repeat_n((i % 251) as u8, 48));
    }
    let layout = LengthPrefixed::new(Prefix::U16Le).payload_only();
    let mut g = c.benchmark_group("framing");
    g.throughput(Throughput::Bytes(stream.len() as u64));
    g.bench_function("framer_1024x48_single_push", |b| {
        b.iter(|| {
            let mut f = Framer::new(layout, 1 << 16);
            f.push(black_box(&stream));
            let mut n = 0usize;
            while let Some(fr) = f.next_frame() {
                n += fr.len();
            }
            n
        });
    });
    g.bench_function("framer_1024x48_1460B_segments", |b| {
        b.iter(|| {
            let mut f = Framer::new(layout, 1 << 16);
            let mut n = 0usize;
            for seg in black_box(&stream).chunks(1460) {
                f.push(seg);
                while let Some(fr) = f.next_frame() {
                    n += fr.len();
                }
            }
            n
        });
    });
    g.bench_function("split_all_1024x48", |b| {
        b.iter(|| {
            let mut n = 0usize;
            ankaa_ingest::framing::split_all(layout, 1 << 16, black_box(&stream), |fr| {
                n += fr.len()
            })
            .unwrap();
            n
        });
    });
    g.finish();
}

/// Generic ingest: one JSON or CSV message → one row (the path every NATS/Kafka/file message
/// takes), and the incremental operators behind continuous queries.
fn generic(c: &mut Criterion) {
    use ankaa_ingest::generic::{RowMapper, TsUnit};
    use ankaa_store::{ColumnType, Field};
    use ankaa_ts::{IncrementalOhlcv, IncrementalPercentiles};
    use std::collections::HashMap;

    let schema = Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("rack", ColumnType::DictU32),
        Field::new("temp", ColumnType::FixedPoint { scale: 8 }),
        Field::new("n", ColumnType::Int64),
        Field::new("ok", ColumnType::U8),
        Field::new("load", ColumnType::Float64),
    ])
    .unwrap();
    let map = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect()
    };
    let json_map = map(&[
        ("ts", "meta.time"),
        ("rack", "meta.id"),
        ("temp", "temp"),
        ("n", "n"),
        ("ok", "ok"),
        ("load", "load"),
    ]);
    let csv_map = map(&[
        ("ts", "0"),
        ("rack", "1"),
        ("temp", "2"),
        ("n", "3"),
        ("ok", "4"),
        ("load", "5"),
    ]);
    let json: Vec<Vec<u8>> = (0..1024u64)
        .map(|i| {
            format!(
                r#"{{"meta":{{"time":{},"id":"rack-{}"}},"temp":"{}.{:02}","n":{},"ok":true,"load":0.{}}}"#,
                1_762_700_000_000 + i,
                i % 16,
                20 + i % 5,
                i % 100,
                i,
                i % 97
            )
            .into_bytes()
        })
        .collect();
    let csv: Vec<Vec<u8>> = (0..1024u64)
        .map(|i| {
            format!(
                "{},rack-{},{}.{:02},{},1,0.{}",
                1_762_700_000_000 + i,
                i % 16,
                20 + i % 5,
                i % 100,
                i,
                i % 97
            )
            .into_bytes()
        })
        .collect();
    let mut g = c.benchmark_group("generic_ingest");
    g.throughput(Throughput::Elements(1024));
    g.bench_function("map_json_1024", |b| {
        let mut m = RowMapper::new(
            schema.clone(),
            &json_map,
            false,
            TsUnit::Millis,
            false,
            Some("rack"),
        )
        .unwrap();
        let mut dicts = RowMapper::dictionaries_for(&schema);
        b.iter(|| {
            let mut acc = 0u32;
            for msg in &json {
                let (key, _) = m.map(black_box(msg), 0, false, &mut dicts).unwrap();
                acc = acc.wrapping_add(key);
            }
            acc
        });
    });
    g.bench_function("map_csv_1024", |b| {
        let mut m = RowMapper::new(
            schema.clone(),
            &csv_map,
            true,
            TsUnit::Millis,
            false,
            Some("rack"),
        )
        .unwrap();
        let mut dicts = RowMapper::dictionaries_for(&schema);
        b.iter(|| {
            let mut acc = 0u32;
            for msg in &csv {
                let (key, _) = m.map(black_box(msg), 0, true, &mut dicts).unwrap();
                acc = acc.wrapping_add(key);
            }
            acc
        });
    });
    g.finish();

    // batched readings: 1,024 messages of 10 readings each (`rows`), per reading
    let batch: Vec<Vec<u8>> = (0..1024u64)
        .map(|i| {
            let samples: Vec<String> = (0..10u64)
                .map(|k| {
                    format!(
                        r#"{{"t":{},"v":{{"temp":"{}.{:02}","n":{},"ok":true,"load":0.{}}}}}"#,
                        1_762_700_000_000 + i * 10 + k,
                        20 + k % 5,
                        (i + k) % 100,
                        i,
                        k % 97
                    )
                })
                .collect();
            format!(
                r#"{{"meta":{{"id":"rack-{}"}},"samples":[{}]}}"#,
                i % 16,
                samples.join(",")
            )
            .into_bytes()
        })
        .collect();
    let rows_map: HashMap<String, String> = [
        ("ts", "t"),
        ("rack", "$msg.meta.id"),
        ("temp", "v.temp"),
        ("n", "v.n"),
        ("ok", "v.ok"),
        ("load", "v.load"),
    ]
    .into_iter()
    .map(|(a, b)| (a.to_owned(), b.to_owned()))
    .collect();
    let mut g = c.benchmark_group("generic_ingest_rows");
    g.throughput(Throughput::Elements(10 * 1024));
    g.bench_function("map_json_rows_10x1024", |b| {
        let mut m = RowMapper::new(
            schema.clone(),
            &rows_map,
            false,
            TsUnit::Millis,
            false,
            Some("rack"),
        )
        .unwrap()
        .with_rows(Some("samples"))
        .unwrap();
        let mut dicts = RowMapper::dictionaries_for(&schema);
        b.iter(|| {
            let mut acc = 0u32;
            for msg in &batch {
                let n = m.parse(black_box(msg), 0, false, &dicts).unwrap();
                for r in 0..n {
                    let (key, _) = m.finish_row(r, &mut dicts);
                    acc = acc.wrapping_add(key);
                }
            }
            acc
        });
    });
    g.finish();

    // Sparkplug B: 1,024 DDATA messages of 20 metrics each, by alias and without a datatype
    // (as a compliant edge node sends them), after one DBIRTH that defines the aliases
    fn varint(mut v: u64, out: &mut Vec<u8>) {
        while v >= 0x80 {
            out.push((v as u8) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
    fn sp_payload(ts: u64, metrics: &[Vec<u8>]) -> Vec<u8> {
        let mut p = vec![0x08];
        varint(ts, &mut p);
        for m in metrics {
            p.push(0x12);
            varint(m.len() as u64, &mut p);
            p.extend_from_slice(m);
        }
        p.extend_from_slice(&[0x18, 7]);
        p
    }
    fn sp_metric(name: Option<&str>, alias: u64, ts: u64, dt: u32, x: f64) -> Vec<u8> {
        let mut m = Vec::new();
        if let Some(n) = name {
            m.push(0x0a);
            varint(n.len() as u64, &mut m);
            m.extend_from_slice(n.as_bytes());
        }
        m.push(0x10);
        varint(alias, &mut m);
        m.push(0x18);
        varint(ts, &mut m);
        if dt != 0 {
            m.push(0x20);
            varint(u64::from(dt), &mut m);
        }
        m.push(0x69);
        m.extend_from_slice(&x.to_le_bytes());
        m
    }
    let sp_msg = |kind: &str, p: Vec<u8>| {
        let mut m = format!("spBv1.0/plant/{kind}/gw1/press").into_bytes();
        m.push(0);
        m.extend_from_slice(&p);
        m
    };
    let base = 1_762_700_000_000u64;
    let birth = sp_msg(
        "DBIRTH",
        sp_payload(
            base,
            &(0..20)
                .map(|a| sp_metric(Some(&format!("Press/Sensor{a:02}")), a, base, 10, 0.0))
                .collect::<Vec<_>>(),
        ),
    );
    let sp_batch: Vec<Vec<u8>> = (0..1024u64)
        .map(|i| {
            let ts = base + 1 + i;
            sp_msg(
                "DDATA",
                sp_payload(
                    ts,
                    &(0..20)
                        .map(|a| sp_metric(None, a, ts, 0, 20.0 + (i * 20 + a) as f64 * 0.01))
                        .collect::<Vec<_>>(),
                ),
            )
        })
        .collect();
    let sp_schema = Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("device", ColumnType::DictU32),
        Field::new("metric", ColumnType::DictU32),
        Field::nullable("value", ColumnType::Float64),
    ])
    .unwrap();
    let sp_map: HashMap<String, String> = [
        ("ts", "$timestamp"),
        ("device", "$device"),
        ("metric", "$metric"),
        ("value", "$value"),
    ]
    .into_iter()
    .map(|(a, b)| (a.to_owned(), b.to_owned()))
    .collect();
    let mut g = c.benchmark_group("generic_ingest_sparkplug");
    g.throughput(Throughput::Elements(20 * 1024));
    g.bench_function("map_ddata_20x1024", |b| {
        let mut m = RowMapper::new(
            sp_schema.clone(),
            &sp_map,
            false,
            TsUnit::Millis,
            false,
            Some("metric"),
        )
        .unwrap()
        .with_topic_prefix(true)
        .with_sparkplug(true)
        .unwrap();
        let mut dicts = RowMapper::dictionaries_for(&sp_schema);
        let n = m.parse(&birth, 0, false, &dicts).unwrap();
        for r in 0..n {
            m.finish_row(r, &mut dicts);
        }
        b.iter(|| {
            let mut acc = 0u32;
            for msg in &sp_batch {
                let n = m.parse(black_box(msg), 0, false, &dicts).unwrap();
                for r in 0..n {
                    let (key, _) = m.finish_row(r, &mut dicts);
                    acc = acc.wrapping_add(key);
                }
            }
            acc
        });
    });
    g.finish();

    let mut g = c.benchmark_group("continuous_100k");
    g.throughput(Throughput::Elements(100_000));
    g.bench_function("ohlcv_push_16_keys", |b| {
        b.iter(|| {
            let mut op = IncrementalOhlcv::new(1_000_000);
            let mut out = Vec::new();
            for i in 0..100_000i64 {
                op.push(
                    (i % 16) as u32,
                    i * 1_000,
                    100 + (i % 37),
                    1 + (i % 5),
                    &mut out,
                );
            }
            out.len()
        });
    });
    g.bench_function("percentiles_push_16_keys", |b| {
        b.iter(|| {
            let mut op = IncrementalPercentiles::new(1_000_000, vec![0.5, 0.99], 0.01);
            let mut out = Vec::new();
            for i in 0..100_000i64 {
                op.push(
                    (i % 16) as u32,
                    i * 1_000,
                    100.0 + (i % 37) as f64,
                    &mut out,
                );
            }
            out.len()
        });
    });
    g.finish();
}

/// Replication: reading a WAL through the tail reader (the standby path on a primary).
fn wal_tail(c: &mut Criterion) {
    use ankaa_wal::{Tail, Wal, WalConfig};
    let dir = std::env::temp_dir().join(format!("ankaa-bench-tail-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (mut w, _) = Wal::open(&dir, WalConfig::default()).unwrap();
    let payload = [7u8; 200];
    for _ in 0..100_000 {
        w.append(1, &payload).unwrap();
    }
    w.commit().unwrap();
    let mut g = c.benchmark_group("wal_tail");
    g.throughput(Throughput::Elements(100_000));
    g.sample_size(20);
    g.bench_function("catch_up_100k_x_200B", |b| {
        b.iter(|| {
            let mut t = Tail::new(&dir, 1);
            let mut n = 0usize;
            loop {
                let got = t.poll(4096).unwrap();
                if got.is_empty() {
                    break;
                }
                n += got.len();
            }
            assert_eq!(n, 100_000);
            n
        });
    });
    g.bench_function("idle_poll", |b| {
        let mut t = Tail::new(&dir, 100_001);
        b.iter(|| t.poll(4096).unwrap().len());
    });
    g.finish();
    drop(w);
    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(
    benches, simd, decode, store, operators, wal, tiers, framing, generic, wal_tail
);
criterion_main!(benches);
