# Data model

## Physical types
| Type | Storage | Notes |
|---|---|---|
| `TimestampNs` | `i64` | nanoseconds since the Unix epoch, UTC; always column 0; non-decreasing within a partition |
| `FixedPoint { scale }` | `i64` | value = ticks / 10^scale; feeds are parsed from decimal strings without floating point; more fractional digits than `scale` is an error, not a rounding |
| `Int64` | `i64` | sizes, sequence numbers, exchange update ids |
| `DictU32` | `u32` | dictionary code (symbols, venues, devices); ids are assigned first-seen, persisted and never change. In SQL the column is the name (`Dictionary(UInt32, Utf8)`): the ids are the keys, used in place (RFC-0002) |
| `U8` | `u8` | flags and enums (e.g. side: 0 = bid/buy, 1 = ask/sell) |
| `Float64` | `f64` | derived features; ALP-encoded when cold |

## Chunks and tables
- A **chunk** holds at most 65,536 rows = 64 vectors of 1,024. Rows are appended in timestamp order; the chunk **watermark** is the maximum accepted timestamp. Appends are type-checked atomically: a rejected row changes nothing.
- **Sealing** freezes a chunk and computes its **zone map** (min/max per column). Sealed chunks are immutable and shared (`Arc`).
- A **table** has one **partition** per partition key (a symbol, a device, a machine: the `partition_by` dictionary column). Each partition owns one open chunk plus its sealed chunks. A row older than the partition's watermark is inserted at its place in time when it is within the table's `max_lateness_ms` (default 0) and dropped and counted beyond it; for an exchange feed (lateness 0) a row out of order is a decoder or feed problem, not something to silently reorder.
- A column may be **nullable**: a validity vector next to the values marks the rows that hold one (created at the first `NULL`, absent otherwise); zone maps cover the valid rows only.
- **Retention** (`retention` per table) drops whole sealed chunks older than the window, measured from the newest row of the table.
- A **snapshot** is the list of sealed chunks plus the query view of the open chunk (incremental copies of its rows), with the minimum watermark across partitions. Queries read snapshots; they see data up to the last snapshot.

## Built-in tables produced by `ankaad`
| Table | Columns |
|---|---|
| `trades` | `ts, symbol, price(8), size(8), side, seq` |
| `book_deltas` | `ts, symbol, side, price(8), size(8), update_id` (absolute level quantities; 0 = delete) |
| `quotes` | `ts, symbol, bid(8), bid_size(8), ask(8), ask_size(8), update_id` (top of book after each exchange message) |
| `symbols` | `symbol, name` |

In SQL, divide fixed-point columns by `1e8`. Fixed-point columns are exposed to Arrow as `Int64` with field metadata `ankaa.scale`.

## Tiers (planned)
Hot: open and recent sealed chunks in memory, uncompressed. Warm: sealed chunks, FastLanes bit-packed + dictionary, in memory. Cold: immutable segments on NVMe (Vortex: ALP, FastLanes, FSST, RLE; or Parquet). Tiering is by chunk recency with a bounded CPU budget; there is no page-level sampling.
