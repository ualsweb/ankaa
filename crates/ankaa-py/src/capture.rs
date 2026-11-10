//! Load a `.ankc` capture into a `DataFusion` session (the `ankaad replay` path, in-process).

use std::path::Path;
use std::sync::Arc;

use ankaa_feeds::binance::{BinanceJsonDecoder, BinanceSbeDecoder};
use ankaa_feeds::capture::{FrameKind, decode_all_bytes};
use ankaa_ingest::{Decoder, EventBatch};
use ankaa_sql::{register_functions, register_snapshot};
use arrow::array::{StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

use crate::tables::Tables;

/// Counters reported by `Dataset.stats()`.
#[derive(Debug, Default, Clone, Copy)]
pub struct CaptureStats {
    pub frames: u64,
    pub events: u64,
    pub trades: u64,
    pub deltas: u64,
    pub gaps: u64,
    pub snapshots: u64,
    pub dropped: u64,
    pub rejected_frames: u64,
    pub symbols: u64,
    pub resyncs: u64,
    pub crossed_messages: u64,
}

/// Decode `path`, build `trades`/`book_deltas`/`quotes`/`symbols` and register the table functions.
pub fn load(path: &Path, expect_snapshots: bool) -> Result<(SessionContext, CaptureStats), String> {
    let bytes = bytes::Bytes::from(
        std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?,
    );
    let (_hdr, frames) = decode_all_bytes(&bytes).map_err(|e| format!("capture: {e}"))?;

    let mut tables = Tables::new(expect_snapshots);
    let mut dec = BinanceJsonDecoder::new();
    let mut sbe = BinanceSbeDecoder::new();
    let mut batch = EventBatch::with_capacity(1024);
    let mut st = CaptureStats::default();
    for f in &frames {
        batch.clear();
        let decoded = match f.kind {
            FrameKind::Text => dec.decode(&f.payload, f.recv_ns, &mut batch),
            FrameKind::Binary => sbe.decode(&f.payload, f.recv_ns, dec.symbols_mut(), &mut batch),
            FrameKind::Snapshot => dec.decode_snapshot(&f.payload, f.recv_ns, &mut batch),
            // control notes and frames of user-defined sources carry no market data
            FrameKind::Control | FrameKind::User => continue,
        };
        st.frames += 1;
        match decoded {
            Ok(_) => {
                let out = tables.ingest(&batch);
                st.events += out.events as u64;
                st.dropped += out.dropped as u64;
            }
            Err(ankaa_ingest::DecodeError::Ignored) => {}
            Err(_) => st.rejected_frames += 1,
        }
    }
    tables.seal_all();
    let js = dec.stats();
    let ss = sbe.stats();
    st.trades = js.trades + ss.trades;
    st.deltas = js.deltas + ss.deltas;
    st.gaps = js.gaps + ss.gaps;
    st.snapshots = js.snapshots + ss.snapshots;
    st.symbols = dec.symbols().len() as u64;
    st.resyncs = tables.resyncs();
    st.crossed_messages = tables.crossed_messages;

    let ctx = ankaa_sql::default_session();
    register_snapshot(&ctx, tables.trades.snapshot()).map_err(|e| e.to_string())?;
    register_snapshot(&ctx, tables.deltas.snapshot()).map_err(|e| e.to_string())?;
    register_snapshot(&ctx, tables.quotes.snapshot()).map_err(|e| e.to_string())?;
    register_symbols(&ctx, &dec)?;
    register_functions(&ctx);
    Ok((ctx, st))
}

fn register_symbols(ctx: &SessionContext, dec: &BinanceJsonDecoder) -> Result<(), String> {
    let n = dec.symbols().len();
    let ids: Vec<u32> = (0..n).map(|i| u32::try_from(i).unwrap_or(u32::MAX)).collect();
    let names: Vec<String> = ids
        .iter()
        .map(|&id| {
            dec.symbols()
                .name(ankaa_ingest::SymbolId(id))
                .unwrap_or("?")
                .to_owned()
        })
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let rb = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt32Array::from(ids)), Arc::new(StringArray::from(names))],
    )
    .map_err(|e| e.to_string())?;
    ctx.register_batch("symbols", rb).map_err(|e| e.to_string())?;
    Ok(())
}

/// Registered table names in the default catalog/schema, sorted.
pub fn table_names(ctx: &SessionContext) -> Vec<String> {
    let mut names = ctx
        .catalog("datafusion")
        .and_then(|c| c.schema("public"))
        .map(|s| s.table_names())
        .unwrap_or_default();
    names.sort();
    names
}
