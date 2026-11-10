//! P1-08: ASOF join and OHLCV as SQL table functions, checked against plain-SQL formulations.

use ankaa_sql::{register_chunks, register_functions};
use ankaa_store::{Chunk, ChunkId, ColumnType, Field, Schema, Value};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{Int64Type, UInt32Type};
use datafusion::prelude::*;

fn quotes_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("symbol", ColumnType::DictU32),
        Field::new("bid", ColumnType::FixedPoint { scale: 8 }),
        Field::new("ask", ColumnType::FixedPoint { scale: 8 }),
    ])
    .unwrap()
}

fn ctx_with_data() -> SessionContext {
    let ctx = SessionContext::new();
    let mut trades = Chunk::new(ChunkId(1), Schema::trades());
    let mut quotes = Chunk::new(ChunkId(2), quotes_schema());
    // two symbols; quotes every 10 ns, trades at odd times
    for i in 0..200i64 {
        let ts = i * 10;
        let sym = u32::try_from(i % 2).unwrap();
        quotes
            .append_row(&[
                Value::I64(ts),
                Value::U32(sym),
                Value::I64(1000 + i),
                Value::I64(1001 + i),
            ])
            .unwrap();
    }
    for i in 0..100i64 {
        let ts = i * 20 + 5;
        let sym = u32::try_from(i % 2).unwrap();
        trades
            .append_row(&[
                Value::I64(ts),
                Value::U32(sym),
                Value::I64(2000 + i),
                Value::I64(3),
                Value::U8(0),
                Value::I64(i),
            ])
            .unwrap();
    }
    register_chunks(&ctx, "trades", Schema::trades(), vec![trades]).unwrap();
    register_chunks(&ctx, "quotes", quotes_schema(), vec![quotes]).unwrap();
    register_functions(&ctx);
    ctx
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn asof_join_matches_correlated_subquery() {
    let ctx = ctx_with_data();
    let fast = ctx
        .sql("SELECT ts, symbol, price, quotes_bid AS bid FROM asof_join('trades', 'quotes', 'symbol') ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let fast = arrow::compute::concat_batches(&fast[0].schema(), &fast).unwrap();
    // Oracle computed from the generator: quotes at ts = j*10 with symbol j % 2; the latest quote
    // for trade i (ts = 20i + 5, symbol i % 2) is j = 2i for even i and j = 2i - 1 for odd i.
    assert_eq!(fast.num_rows(), 100);
    let fb = fast
        .column_by_name("bid")
        .unwrap()
        .as_primitive::<Int64Type>();
    let fs = fast
        .column_by_name("symbol")
        .unwrap()
        .as_primitive::<UInt32Type>();
    let ft = fast
        .column_by_name("ts")
        .unwrap()
        .as_primitive::<arrow::datatypes::TimestampNanosecondType>();
    for row in 0..fast.num_rows() {
        let i = (ft.value(row) - 5) / 20;
        let j = if fs.value(row) == 0 { 2 * i } else { 2 * i - 1 };
        assert!(!fb.is_null(row), "row {row}");
        assert_eq!(fb.value(row), 1000 + j, "row {row} (trade {i})");
    }
    // tolerance: quotes are at most 15 ns old at trade time; a 1 ns tolerance matches nothing
    let none = ctx
        .sql("SELECT count(quotes_bid) AS n FROM asof_join('trades', 'quotes', 'symbol', 1)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(none[0].column(0).as_primitive::<Int64Type>().value(0), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ohlcv_matches_group_by() {
    let ctx = ctx_with_data();
    let fast = ctx
        .sql("SELECT symbol, bucket, open, high, low, close, volume, count FROM ohlcv('trades', 400) ORDER BY symbol, bucket")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let fast = arrow::compute::concat_batches(&fast[0].schema(), &fast).unwrap();
    let slow = ctx
        .sql(
            "SELECT symbol, date_bin(INTERVAL '400 nanoseconds', ts, TIMESTAMP '1970-01-01T00:00:00Z') AS bucket, \
                    first_value(price ORDER BY ts) AS open, max(price) AS high, min(price) AS low, \
                    last_value(price ORDER BY ts) AS close, sum(size) AS volume, count(*) AS count \
             FROM trades GROUP BY symbol, bucket ORDER BY symbol, bucket",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let slow = arrow::compute::concat_batches(&slow[0].schema(), &slow).unwrap();
    assert!(fast.num_rows() > 2);
    assert_eq!(fast.num_rows(), slow.num_rows());
    for col in ["open", "high", "low", "close", "volume", "count"] {
        let f = fast
            .column_by_name(col)
            .unwrap()
            .as_primitive::<Int64Type>();
        let s = slow
            .column_by_name(col)
            .unwrap()
            .as_primitive::<Int64Type>();
        assert_eq!(f.values(), s.values(), "{col}");
    }
    let fk = fast
        .column_by_name("symbol")
        .unwrap()
        .as_primitive::<UInt32Type>();
    let sk = slow
        .column_by_name("symbol")
        .unwrap()
        .as_primitive::<UInt32Type>();
    assert_eq!(fk.values(), sk.values());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn percentiles_within_relative_error() {
    let ctx = ctx_with_data();
    let fast = ctx
        .sql("SELECT symbol, count, min, max, p50, p99, collapses FROM percentiles('trades', 'price', '0.5,0.99', 'symbol') ORDER BY symbol")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let fast = arrow::compute::concat_batches(&fast[0].schema(), &fast).unwrap();
    assert_eq!(fast.num_rows(), 2);
    // exact nearest-rank quantiles per symbol from the generator: prices 2000 + i, i % 2 = symbol
    for row in 0..2usize {
        let sym = fast
            .column_by_name("symbol")
            .unwrap()
            .as_primitive::<UInt32Type>()
            .value(row);
        let mut prices: Vec<f64> = (0..100i32)
            .filter(|i| u32::try_from(i % 2).unwrap() == sym)
            .map(|i| f64::from(2000 + i))
            .collect();
        prices.sort_by(f64::total_cmp);
        let get = |c: &str| {
            fast.column_by_name(c)
                .unwrap()
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row)
        };
        assert_eq!(get("min"), prices[0]);
        assert_eq!(get("max"), prices[prices.len() - 1]);
        for (col, q) in [("p50", 0.5f64), ("p99", 0.99)] {
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::cast_precision_loss
            )]
            let rank = ((q * (prices.len() - 1) as f64).round()) as usize;
            let exact = prices[rank];
            let got = get(col);
            assert!(
                (got - exact).abs() <= exact * ankaa_sql::PERCENTILE_ALPHA * 2.0,
                "symbol {sym} {col}: got {got}, exact {exact}"
            );
        }
        assert_eq!(
            fast.column_by_name("collapses")
                .unwrap()
                .as_primitive::<UInt32Type>()
                .value(row),
            0
        );
    }
    // overall form without key, and bad input
    let all = ctx
        .sql("SELECT count FROM percentiles('trades', 'size', '0.5')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        all[0]
            .column(0)
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        100
    );
    assert!(
        ctx.sql("SELECT * FROM percentiles('trades', 'size', '2')")
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_functions_over_empty_tables_do_not_panic() {
    let ctx = SessionContext::new();
    register_chunks(&ctx, "trades", Schema::trades(), vec![]).unwrap();
    register_chunks(&ctx, "quotes", quotes_schema(), vec![]).unwrap();
    register_functions(&ctx);
    for q in [
        "SELECT count(*) FROM ohlcv('trades', 1000)",
        "SELECT count(*) FROM asof_join('trades', 'quotes', 'symbol')",
        "SELECT count(*) FROM asof_join('trades', 'quotes', 'symbol', 5, 'forward')",
        "SELECT count(*) FROM percentiles('trades', 'price', '0.5')",
    ] {
        let out = ctx.sql(q).await.unwrap().collect().await.unwrap();
        let n = out[0].column(0).as_primitive::<Int64Type>().value(0);
        assert_eq!(n, 0, "{q}");
    }
    // one side empty, the other populated
    let ctx = ctx_with_data();
    register_chunks(&ctx, "empty_quotes", quotes_schema(), vec![]).unwrap();
    let out = ctx
        .sql("SELECT count(*) AS n, count(empty_quotes_bid) AS matched FROM asof_join('trades', 'empty_quotes', 'symbol')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(out[0].column(0).as_primitive::<Int64Type>().value(0), 100);
    assert_eq!(out[0].column(1).as_primitive::<Int64Type>().value(0), 0);
    assert!(
        ctx.sql("SELECT * FROM asof_join('trades', 'quotes', 'symbol', -1)")
            .await
            .is_err()
    );
}

/// Planning a table function reads nothing; its input is charged to the session's memory pool
/// when the query runs, so a call over more data than the pool allows fails instead of taking
/// the process down.
#[tokio::test]
async fn table_functions_are_lazy_and_bounded_by_the_memory_pool() {
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(256 * 1024, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()
        .unwrap();
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let mut trades = Chunk::new(ChunkId(1), Schema::trades());
    for i in 0..60_000i64 {
        trades
            .append_row(&[
                Value::I64(i),
                Value::U32(u32::try_from(i % 4).unwrap()),
                Value::I64(100 + i % 7),
                Value::I64(1),
                Value::U8(0),
                Value::I64(i),
            ])
            .unwrap();
    }
    register_chunks(&ctx, "trades", Schema::trades(), vec![trades]).unwrap();
    register_functions(&ctx);
    for sql in [
        "SELECT * FROM ohlcv('trades', 10)",
        "SELECT * FROM percentiles('trades', 'price', '0.5', 'symbol')",
        "SELECT * FROM asof_join('trades', 'trades', 'symbol')",
    ] {
        // planning (what GetFlightInfo does) succeeds at once and gives the schema
        let df = ctx.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(!df.schema().fields().is_empty());
        // so does physical planning (EXPLAIN): nothing is read before execution
        let plan = df
            .clone()
            .create_physical_plan()
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(
            format!("{plan:?}").contains("LazyExec")
                || plan.name() == "TableFunctionExec"
                || !plan.children().is_empty()
        );
        // running it needs more than the pool has
        let err = df.collect().await.expect_err(sql).to_string();
        assert!(
            err.to_lowercase().contains("resources exhausted") || err.contains("memory"),
            "{sql}: {err}"
        );
    }
    // argument mistakes are still reported while planning
    assert!(ctx.sql("SELECT * FROM ohlcv('nope', 10)").await.is_err());
    assert!(ctx.sql("SELECT * FROM ohlcv('trades', 0)").await.is_err());
    assert!(
        ctx.sql("SELECT * FROM percentiles('trades', 'nope', '0.5')")
            .await
            .is_err()
    );
}

/// Quotes with a sequence number as the last column: it orders quotes that share a timestamp.
fn seq_quotes_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("symbol", ColumnType::DictU32),
        Field::new("bid", ColumnType::FixedPoint { scale: 8 }),
        Field::new("qseq", ColumnType::Int64),
    ])
    .unwrap()
}

const ORDER_SYMBOLS: i64 = 5;
const ORDER_CHUNK: i64 = 50_000;

/// Symbol of generated row `i`: runs of `block` rows per symbol (1 = symbols alternate row by
/// row; a few hundred = long runs, as in a table partitioned by symbol).
fn order_symbol(i: i64, block: i64) -> i64 {
    ((i / block) * 7) % ORDER_SYMBOLS
}

/// Three chunks of trades and of quotes, timestamps shared by several rows. `reversed`
/// registers the chunks latest first, so the rows of every key reach the table functions out
/// of time order.
fn order_ctx(reversed: bool, partitions: usize, block: i64) -> SessionContext {
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(partitions));
    let (mut trades, mut quotes) = (Vec::new(), Vec::new());
    for c in 0..3i64 {
        let mut t = Chunk::new(ChunkId(u64::try_from(c).unwrap()), Schema::trades());
        let mut q = Chunk::new(ChunkId(u64::try_from(10 + c).unwrap()), seq_quotes_schema());
        for r in 0..ORDER_CHUNK {
            let i = c * ORDER_CHUNK + r;
            let sym = u32::try_from(order_symbol(i, block)).unwrap();
            // trades: timestamps repeat four times; quotes: three times, on another grid
            t.append_row(&[
                Value::I64((i / 4) * 9 + 3),
                Value::U32(sym),
                Value::I64(100 + i % 13),
                Value::I64(1 + i % 5),
                Value::U8(0),
                Value::I64(i),
            ])
            .unwrap();
            q.append_row(&[
                Value::I64((i / 3) * 7),
                Value::U32(sym),
                Value::I64(5_000 + i),
                Value::I64(i),
            ])
            .unwrap();
        }
        t.seal();
        q.seal();
        trades.push(t);
        quotes.push(q);
    }
    if reversed {
        trades.reverse();
        quotes.reverse();
    }
    register_chunks(&ctx, "trades", Schema::trades(), trades).unwrap();
    register_chunks(&ctx, "quotes", seq_quotes_schema(), quotes).unwrap();
    register_functions(&ctx);
    ctx
}

/// Expected `qseq` of the quote joined to trade `i`, straight from the generator.
fn expected_quote(i: i64, block: i64, tolerance: Option<i64>, forward: bool) -> Option<i64> {
    let ts = (i / 4) * 9 + 3;
    let sym = order_symbol(i, block);
    let total = 3 * ORDER_CHUNK;
    let qts = |j: i64| (j / 3) * 7;
    // quotes are in (ts, qseq) order by index; walk outwards from the time boundary
    let first_after = {
        let (mut lo, mut hi) = (0, total);
        while lo < hi {
            let mid = i64::midpoint(lo, hi);
            if (forward && qts(mid) < ts) || (!forward && qts(mid) <= ts) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    };
    let found = if forward {
        (first_after..total).find(|&j| order_symbol(j, block) == sym)
    } else {
        (0..first_after)
            .rev()
            .find(|&j| order_symbol(j, block) == sym)
    }?;
    let gap = (ts - qts(found)).abs();
    tolerance.is_none_or(|t| gap <= t).then_some(found)
}

async fn asof_pairs(ctx: &SessionContext, args: &str) -> Vec<(i64, Option<i64>)> {
    let b = ctx
        .sql(&format!(
            "SELECT seq, quotes_qseq FROM asof_join('trades', 'quotes', 'symbol'{args})"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let b = arrow::compute::concat_batches(&b[0].schema(), &b).unwrap();
    let (seq, q) = (
        b.column(0).as_primitive::<Int64Type>(),
        b.column(1).as_primitive::<Int64Type>(),
    );
    let mut out: Vec<(i64, Option<i64>)> = (0..b.num_rows())
        .map(|r| (seq.value(r), (!q.is_null(r)).then(|| q.value(r))))
        .collect();
    out.sort_unstable();
    out
}

/// The table functions never sort a whole table: they rely on each key's rows arriving in time
/// order and repair a key that does not. Whatever the storage order and however many scan
/// partitions, the result is the one the generator predicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_functions_do_not_depend_on_storage_order() {
    let cases: [(&str, Option<i64>, bool); 4] = [
        ("", None, false),
        (", 2", Some(2), false),
        (", 1000000, 'forward'", Some(1_000_000), true),
        (", 0, 'forward'", Some(0), true),
    ];
    // long runs in stored order take the in-place path; alternating symbols or reversed
    // chunks take the path that groups and sorts: all of them must agree with the generator
    for block in [256, 1] {
        let mut bars: Vec<String> = Vec::new();
        for (reversed, partitions) in [(false, 1), (false, 4), (true, 1), (true, 4)] {
            let ctx = order_ctx(reversed, partitions, block);
            for (args, tolerance, forward) in cases {
                let got = asof_pairs(&ctx, args).await;
                assert_eq!(got.len(), usize::try_from(3 * ORDER_CHUNK).unwrap());
                for (seq, quote) in got {
                    assert_eq!(
                        quote,
                        expected_quote(seq, block, tolerance, forward),
                        "trade {seq}, args {args:?}, block {block}, reversed {reversed}, partitions {partitions}"
                    );
                }
            }
            let b = ctx
                .sql("SELECT * FROM ohlcv('trades', 1000) ORDER BY symbol, bucket")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            bars.push(
                arrow::util::pretty::pretty_format_batches(&b)
                    .unwrap()
                    .to_string(),
            );
        }
        assert!(
            bars.iter().all(|b| *b == bars[0]),
            "ohlcv changed with storage order (block {block})"
        );
    }
}

/// Only the columns a query uses are read and built; the result is the same as selecting them
/// from the full join.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn asof_join_builds_only_requested_columns() {
    let ctx = ctx_with_data();
    let one = |sql: &'static str| {
        let ctx = ctx.clone();
        async move {
            let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            arrow::util::pretty::pretty_format_batches(&b)
                .unwrap()
                .to_string()
        }
    };
    // no column at all, one right column, a mix in another order than the schema's
    let n = one("SELECT count(*) AS n FROM asof_join('trades', 'quotes', 'symbol')").await;
    assert!(n.contains("| 100 |"), "{n}");
    let narrow = one(
        "SELECT sum(quotes_ask - quotes_bid) AS spread, count(quotes_ts) AS m \
         FROM asof_join('trades', 'quotes', 'symbol')",
    )
    .await;
    let wide = one(
        "SELECT sum(quotes_ask - quotes_bid) AS spread, count(quotes_ts) AS m \
         FROM (SELECT * FROM asof_join('trades', 'quotes', 'symbol') ORDER BY seq) j",
    )
    .await;
    assert_eq!(narrow, wide);
    let mixed = one(
        "SELECT quotes_ask, seq, symbol FROM asof_join('trades', 'quotes', 'symbol') \
         ORDER BY seq LIMIT 3",
    )
    .await;
    let full = one("SELECT quotes_ask, seq, symbol FROM \
         (SELECT * FROM asof_join('trades', 'quotes', 'symbol')) j ORDER BY seq LIMIT 3")
    .await;
    assert_eq!(mixed, full);
    let plan = one("EXPLAIN SELECT quotes_ask FROM asof_join('trades', 'quotes', 'symbol')").await;
    assert!(
        plan.contains("TableFunctionExec: asof_join projection=Some("),
        "{plan}"
    );
}

/// (symbol, window end, open, high, low, close, volume, count)
type Bar = (u32, i64, i64, i64, i64, i64, i64, i64);

/// `rolling` gives, for every step end with a non-empty window, the bar of the rows inside the
/// trailing window; with a window of one step it is `ohlcv` labelled by the end of the bucket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rolling_matches_recomputing_every_window() {
    let ctx = ctx_with_data();
    let (window, step) = (300i64, 100i64);
    let out = ctx
        .sql(&format!(
            "SELECT symbol, window_end, open, high, low, close, volume, count \
             FROM rolling('trades', {window}, {step}) ORDER BY symbol, window_end"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let got = arrow::compute::concat_batches(&out[0].schema(), &out).unwrap();
    // trades of `ctx_with_data`: i in 0..100 at ts = 20 i + 5, symbol i % 2, price 2000 + i, size 3
    let mut want: Vec<Bar> = Vec::new();
    for sym in 0..2i64 {
        let mut end = step;
        while end <= 20 * 99 + 5 + window + step {
            let inside: Vec<i64> = (0..100i64)
                .filter(|i| i % 2 == sym && 20 * i + 5 >= end - window && 20 * i + 5 < end)
                .collect();
            if let (Some(first), Some(last)) = (inside.first(), inside.last()) {
                let n = i64::try_from(inside.len()).unwrap();
                want.push((
                    u32::try_from(sym).unwrap(),
                    end,
                    2000 + first,
                    2000 + last, // prices rise with i: the last is the highest
                    2000 + first,
                    2000 + last,
                    3 * n,
                    n,
                ));
            }
            end += step;
        }
    }
    assert_eq!(got.num_rows(), want.len());
    let sym = got.column(0).as_primitive::<UInt32Type>();
    let end = got
        .column(1)
        .as_primitive::<arrow::datatypes::TimestampNanosecondType>();
    let col = |c: usize| got.column(c).as_primitive::<Int64Type>();
    for (row, w) in want.iter().enumerate() {
        let g = (
            sym.value(row),
            end.value(row),
            col(2).value(row),
            col(3).value(row),
            col(4).value(row),
            col(5).value(row),
            col(6).value(row),
            col(7).value(row),
        );
        assert_eq!(g, *w, "row {row}");
    }
    // one step per window: the bucket bars, labelled by their end
    let text = |sql: &'static str| {
        let ctx = ctx.clone();
        async move {
            let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            arrow::util::pretty::pretty_format_batches(&b)
                .unwrap()
                .to_string()
        }
    };
    assert_eq!(
        text(
            "SELECT symbol, window_end AS t, open, high, low, close, volume, notional, count \
             FROM rolling('trades', 100, 100) ORDER BY symbol, t"
        )
        .await,
        text(
            "SELECT symbol, bucket + INTERVAL '100 nanoseconds' AS t, open, high, low, close, volume, notional, count \
             FROM ohlcv('trades', 100) ORDER BY symbol, t"
        )
        .await
    );
    // arguments are checked when the query is planned
    for bad in [
        "SELECT * FROM rolling('trades', 250, 100)",
        "SELECT * FROM rolling('trades', 100, 0)",
        "SELECT * FROM rolling('trades', 50, 100)",
        "SELECT * FROM rolling('nope', 100, 100)",
    ] {
        assert!(ctx.sql(bad).await.is_err(), "{bad}");
    }
}

/// `vwap(price, size)` is `Σ price·size / Σ size`, exactly: the same number as the
/// floating-point formula people write by hand, within rounding, and `NULL` without volume.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vwap_matches_the_hand_written_formula() {
    let ctx = ctx_with_data();
    let out = ctx
        .sql(
            "SELECT symbol, vwap(price, size) AS native, \
             sum(CAST(price AS DOUBLE) * size) / sum(CAST(size AS DOUBLE)) AS by_hand \
             FROM trades GROUP BY symbol ORDER BY symbol",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let b = arrow::compute::concat_batches(&out[0].schema(), &out).unwrap();
    assert_eq!(b.num_rows(), 2);
    let col = |i: usize| b.column(i).as_primitive::<arrow::datatypes::Float64Type>();
    for row in 0..2 {
        let (native, by_hand) = (col(1).value(row), col(2).value(row));
        assert!(
            (native - by_hand).abs() <= 1e-9 * by_hand.abs(),
            "{native} vs {by_hand}"
        );
    }
    // every trade has size 3 and prices 2000 + i: symbol 0 holds the even i
    let even: f64 = (0..100)
        .step_by(2)
        .map(|i| 2000.0 + f64::from(i))
        .sum::<f64>()
        / 50.0;
    assert!((col(1).value(0) - even).abs() < 1e-9);
    let none = ctx
        .sql("SELECT vwap(price, size) AS v FROM trades WHERE price < 0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(none[0].column(0).is_null(0));
}
