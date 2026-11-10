//! Replay sources other than `.ankc` captures (PRD F-01): CSV and Parquet trade files and
//! Databento DBN files, all funnelled through the same `Tables::ingest` path as live data.
//!
//! CSV/Parquet column contract (names, case-insensitive; extra columns are ignored):
//! - `ts`: timestamp (any Arrow timestamp unit, or Int64 nanoseconds, or RFC 3339 text in CSV)
//! - `symbol`: text or integer id
//! - `price`, `size`: Float64 / Decimal / Int64 ticks at scale 8 (`*_ticks` suffix forces ticks)
//! - `side` (optional): `B`/`S`, `buy`/`sell`, `bid`/`ask`, or 0/1
//! - `seq` (optional): Int64

use std::path::Path;
use std::sync::Arc;

use ankaa_ingest::{EventBatch, Side, SymbolDict, SymbolId, Trade, parse_fixed};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Float64Type, Int64Type, TimeUnit, UInt32Type};

/// Which reader handles a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    /// ANKAA capture.
    Capture,
    /// Databento DBN (optionally zstd).
    Dbn,
    /// CSV trades.
    Csv,
    /// Parquet trades.
    Parquet,
}

/// Classify a path by extension (lower-cased first, so the comparison is case-insensitive).
#[must_use]
#[allow(clippy::case_sensitive_file_extension_comparisons)]
pub fn kind_of(path: &Path) -> SourceKind {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if name.ends_with(".dbn") || name.ends_with(".dbn.zst") {
        SourceKind::Dbn
    } else if name.ends_with(".csv") || name.ends_with(".csv.gz") {
        SourceKind::Csv
    } else if name.ends_with(".parquet") || name.ends_with(".pq") {
        SourceKind::Parquet
    } else {
        SourceKind::Capture
    }
}

/// Arrow batches from a CSV or Parquet file, 1,024 rows at a time.
pub fn arrow_batches(
    path: &Path,
    kind: SourceKind,
) -> anyhow::Result<Box<dyn Iterator<Item = anyhow::Result<RecordBatch>>>> {
    match kind {
        SourceKind::Csv => {
            // `.csv.gz` is decompressed on the fly (two passes: schema inference, then rows)
            let open = || -> anyhow::Result<Box<dyn std::io::Read>> {
                let file = std::fs::File::open(path)?;
                let gz = path
                    .extension()
                    .is_some_and(|e| e.eq_ignore_ascii_case("gz"));
                Ok(if gz {
                    Box::new(flate2::read::MultiGzDecoder::new(file))
                } else {
                    Box::new(file)
                })
            };
            let (schema, _) = arrow::csv::reader::Format::default()
                .with_header(true)
                .infer_schema(open()?, Some(2_000))?;
            let reader = arrow::csv::ReaderBuilder::new(Arc::new(schema))
                .with_header(true)
                .with_batch_size(1024)
                .build(open()?)?;
            Ok(Box::new(reader.map(|r| r.map_err(Into::into))))
        }
        SourceKind::Parquet => {
            let file = std::fs::File::open(path)?;
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?
                    .with_batch_size(1024)
                    .build()?;
            Ok(Box::new(reader.map(|r| r.map_err(Into::into))))
        }
        SourceKind::Capture | SourceKind::Dbn => anyhow::bail!("not an Arrow source: {kind:?}"),
    }
}

fn col<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a ArrayRef> {
    batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(name))
        .map(|i| batch.column(i))
}

/// Timestamp column → ns.
fn ts_ns(a: &ArrayRef, row: usize) -> anyhow::Result<i64> {
    let out_of_range = || anyhow::anyhow!("timestamp out of the nanosecond range");
    Ok(match a.data_type() {
        DataType::Timestamp(unit, _) => {
            let (v, scale) = match unit {
                TimeUnit::Nanosecond => (
                    a.as_primitive::<arrow::datatypes::TimestampNanosecondType>()
                        .value(row),
                    1,
                ),
                TimeUnit::Microsecond => (
                    a.as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                        .value(row),
                    1_000,
                ),
                TimeUnit::Millisecond => (
                    a.as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                        .value(row),
                    1_000_000,
                ),
                TimeUnit::Second => (
                    a.as_primitive::<arrow::datatypes::TimestampSecondType>()
                        .value(row),
                    1_000_000_000,
                ),
            };
            v.checked_mul(scale).ok_or_else(out_of_range)?
        }
        DataType::Int64 => a.as_primitive::<Int64Type>().value(row),
        other => anyhow::bail!("ts column type {other} not supported"),
    })
}

/// Price/size column → ticks at scale 8.
fn ticks(a: &ArrayRef, row: usize, is_ticks: bool) -> anyhow::Result<i64> {
    Ok(match a.data_type() {
        DataType::Int64 if is_ticks => a.as_primitive::<Int64Type>().value(row),
        DataType::Int64 => a
            .as_primitive::<Int64Type>()
            .value(row)
            .checked_mul(100_000_000)
            .ok_or_else(|| anyhow::anyhow!("integer value overflows scale 8"))?,
        DataType::Float64 => {
            let v = a.as_primitive::<Float64Type>().value(row);
            if !v.is_finite() || v.abs() > 9.0e10 {
                anyhow::bail!("value {v} is not a finite number within the fixed-point range");
            }
            #[allow(clippy::cast_possible_truncation)]
            let t = (v * 1e8).round() as i64;
            t
        }
        DataType::Decimal128(_, scale) => {
            let v = a
                .as_primitive::<arrow::datatypes::Decimal128Type>()
                .value(row);
            let s = i32::from(*scale);
            let adj = 8 - s;
            // checked: an extreme scale (i8 allows -128..=127) must not overflow the power
            let t = 10i128.checked_pow(adj.unsigned_abs()).and_then(|p| {
                if adj >= 0 {
                    v.checked_mul(p)
                } else {
                    v.checked_div(p)
                }
            });
            i64::try_from(t.ok_or_else(|| anyhow::anyhow!("decimal overflow"))?)?
        }
        DataType::Utf8 => parse_fixed(a.as_string::<i32>().value(row).as_bytes(), 8)?,
        other => anyhow::bail!("numeric column type {other} not supported"),
    })
}

fn side(a: Option<&ArrayRef>, row: usize) -> Side {
    let Some(a) = a else { return Side::Ask };
    match a.data_type() {
        DataType::Utf8 => {
            let s = a.as_string::<i32>().value(row);
            match s.trim().to_ascii_lowercase().as_str() {
                "b" | "buy" | "bid" | "0" => Side::Bid,
                _ => Side::Ask,
            }
        }
        DataType::Int64 => {
            if a.as_primitive::<Int64Type>().value(row) == 0 {
                Side::Bid
            } else {
                Side::Ask
            }
        }
        DataType::UInt8 => {
            if a.as_primitive::<arrow::datatypes::UInt8Type>().value(row) == 0 {
                Side::Bid
            } else {
                Side::Ask
            }
        }
        _ => Side::Ask,
    }
}

/// Convert one Arrow batch of trades into `out`; returns the number of trades.
pub fn trades_from_batch(
    batch: &RecordBatch,
    symbols: &mut SymbolDict,
    out: &mut EventBatch,
) -> anyhow::Result<usize> {
    let ts = col(batch, "ts").ok_or_else(|| anyhow::anyhow!("missing column ts"))?;
    let (price, price_ticks) = match col(batch, "price_ticks") {
        Some(p) => (p, true),
        None => (
            col(batch, "price").ok_or_else(|| anyhow::anyhow!("missing column price"))?,
            false,
        ),
    };
    let (size, size_ticks) = match col(batch, "size_ticks") {
        Some(p) => (p, true),
        None => (
            col(batch, "size").ok_or_else(|| anyhow::anyhow!("missing column size"))?,
            false,
        ),
    };
    let sym = col(batch, "symbol").ok_or_else(|| anyhow::anyhow!("missing column symbol"))?;
    let side_col = col(batch, "side");
    let seq = col(batch, "seq");
    let mut n = 0;
    let mut last_sym: Option<(String, SymbolId)> = None;
    for row in 0..batch.num_rows() {
        if ts.is_null(row) || price.is_null(row) || size.is_null(row) {
            continue;
        }
        // every symbol goes through the dictionary so ids stay dense: raw integer ids would
        // allocate one partition per id value (sparse numeric tickers = gigabytes)
        let name: std::borrow::Cow<'_, str> = match sym.data_type() {
            DataType::Utf8 => sym.as_string::<i32>().value(row).into(),
            DataType::UInt32 => sym
                .as_primitive::<UInt32Type>()
                .value(row)
                .to_string()
                .into(),
            DataType::Int64 => sym
                .as_primitive::<Int64Type>()
                .value(row)
                .to_string()
                .into(),
            other => anyhow::bail!("symbol column type {other} not supported"),
        };
        let symbol = match &last_sym {
            Some((n, id)) if *n == name => *id,
            _ => {
                let id = symbols.intern(&name);
                last_sym = Some((name.into_owned(), id));
                id
            }
        };
        let ts_ns = ts_ns(ts, row)?;
        out.trades.push(Trade {
            ts_ns,
            recv_ns: ts_ns,
            symbol,
            price: ticks(price, row, price_ticks)?,
            size: ticks(size, row, size_ticks)?,
            side: side(side_col, row),
            seq: seq.map_or(0, |s| match s.data_type() {
                DataType::Int64 => s.as_primitive::<Int64Type>().value(row),
                _ => 0,
            }),
        });
        n += 1;
    }
    Ok(n)
}

#[cfg(test)]
#[allow(
    clippy::large_digit_groups,
    clippy::inconsistent_digit_grouping,
    clippy::unreadable_literal
)]
mod tests {
    use super::*;

    #[test]
    fn csv_trades_round_trip() {
        let dir = std::env::temp_dir().join(format!("ankaa-src-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("t.csv");
        std::fs::write(
            &p,
            "ts,symbol,price,size,side,seq\n\
             2026-11-09T10:00:00.000000001Z,BTCUSDT,65000.5,0.25,buy,1\n\
             2026-11-09T10:00:00.000000002Z,ETHUSDT,3000.125,2,sell,2\n\
             2026-11-09T10:00:00.000000003Z,BTCUSDT,65001,1,B,3\n",
        )
        .unwrap();
        assert_eq!(kind_of(&p), SourceKind::Csv);
        let mut symbols = SymbolDict::new();
        let mut out = EventBatch::with_capacity(16);
        let mut n = 0;
        for b in arrow_batches(&p, SourceKind::Csv).unwrap() {
            n += trades_from_batch(&b.unwrap(), &mut symbols, &mut out).unwrap();
        }
        assert_eq!(n, 3);
        assert_eq!(out.trades[0].price, 65000_50000000);
        assert_eq!(out.trades[0].size, 25_000_000);
        assert_eq!(out.trades[0].side, Side::Bid);
        assert_eq!(out.trades[1].price, 3000_12500000);
        assert_eq!(out.trades[1].side, Side::Ask);
        assert_eq!(out.trades[2].symbol, out.trades[0].symbol);
        assert_eq!(out.trades[2].seq, 3);
        assert_eq!(symbols.len(), 2);
        assert_eq!(out.trades[0].ts_ns % 1_000_000_000, 1);
    }

    #[test]
    fn gzip_csv_is_decompressed() {
        use std::io::Write as _;
        let dir = std::env::temp_dir().join(format!("ankaa-src-gz-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("t.csv.gz");
        let mut gz = flate2::write::GzEncoder::new(
            std::fs::File::create(&p).unwrap(),
            flate2::Compression::default(),
        );
        gz.write_all(
            b"ts,symbol,price,size,side,seq\n\
              2026-11-09T10:00:00.000000001Z,BTCUSDT,65000.5,0.25,buy,1\n\
              2026-11-09T10:00:00.000000002Z,ETHUSDT,3000.125,2,sell,2\n",
        )
        .unwrap();
        gz.finish().unwrap();
        assert_eq!(kind_of(&p), SourceKind::Csv);
        let mut symbols = SymbolDict::new();
        let mut out = EventBatch::with_capacity(16);
        let mut n = 0;
        for b in arrow_batches(&p, SourceKind::Csv).unwrap() {
            n += trades_from_batch(&b.unwrap(), &mut symbols, &mut out).unwrap();
        }
        assert_eq!(n, 2);
        assert_eq!(out.trades[1].price, 3000_12500000);
    }
}
