//! P0-07 spike: round-trip a price-like f64 column and an i64 tick column through Vortex's
//! adaptive (BtrBlocks-style) compressor, which picks ALP / FoR / bit-packing / dictionary /
//! run-end encodings; print the encoding tree and sizes. Functional check only, no timing claims.

use std::sync::Arc;

use arrow::array::{Array as _, ArrayRef as ArrowArrayRef, Float64Array, Int64Array};
use vortex::VortexSessionDefault;
use vortex::array::{Canonical, VortexSessionExecute};
use vortex::arrow::ArrowSessionExt;
use vortex::compressor::BtrBlocksCompressor;
use vortex::session::VortexSession;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n = 65_536usize;
    let mut px = Vec::with_capacity(n);
    let mut ticks = Vec::with_capacity(n);
    let mut p = 50_000.00f64;
    let mut s = 0x9E37_79B9_7F4A_7C15u64;
    for _ in 0..n {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let step = ((s % 5) as f64 - 2.0) * 0.01; // ±2 cents, decimal-origin like an exchange feed
        p = ((p + step).max(1.0) * 100.0).round() / 100.0;
        px.push(p);
        ticks.push((p * 100.0).round() as i64);
    }
    let f64_arr: ArrowArrayRef = Arc::new(Float64Array::from(px.clone()));
    let i64_arr: ArrowArrayRef = Arc::new(Int64Array::from(ticks.clone()));

    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();
    let compressor = BtrBlocksCompressor::from_session(&session);

    for (name, arrow_in) in [("f64 prices", f64_arr), ("i64 ticks", i64_arr)] {
        let raw_bytes = arrow_in.get_array_memory_size();
        let v = session.arrow().from_arrow_array(arrow_in.clone(), false)?;
        let c = compressor.compress(&v, &mut ctx)?;
        let nbytes = c.nbytes();
        println!("== {name}: raw {raw_bytes} B -> compressed {nbytes} B ({:.2}x)", raw_bytes as f64 / nbytes as f64);
        println!("{}", c.display_tree());
        // decode: canonical primitive, then back to Arrow, and compare values
        let canon = c.clone().execute::<Canonical>(&mut ctx)?;
        let prim = canon.into_primitive();
        assert_eq!(prim.len(), n);
        let back = session.arrow().execute_arrow(c, None, &mut ctx)?;
        assert_eq!(back.len(), n);
        if let Some(f) = back.as_any().downcast_ref::<Float64Array>() {
            assert!(f.values().iter().zip(&px).all(|(a, b)| a == b), "f64 round trip mismatch");
        }
        if let Some(i) = back.as_any().downcast_ref::<Int64Array>() {
            assert!(i.values().iter().zip(&ticks).all(|(a, b)| a == b), "i64 round trip mismatch");
        }
        println!("   round trip ok ({} rows)", back.len());
    }
    Ok(())
}
