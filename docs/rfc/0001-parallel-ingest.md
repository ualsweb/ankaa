# RFC-0001: Parallel ingest for many-core Arm servers

- **Status:** Accepted (stage 1 implemented 2026-11-11; stage 2 planned)
- **Date:** 2026-11-11
- **Affects:** `ankaa-ingest` (`generic`), `bins/ankaad` (`serve`, `tables`), config `[storage] ingest_workers`; PRD F-04 (partition-owned appenders), N-01/N-02 (ingest throughput)

## Summary

`ankaad` ingests through one loop: it takes frames from the sources, hands them to the write-ahead log
and then decodes, maps and appends them, one after the other. One core bounds the ingest rate (about
3 M readings/s for JSON on a Neoverse V2 core). Arm server parts now carry 64 to 136 cores (Neoverse V3
in the Arm AGI CPU, 2 MB of L2 per core but about 6 GB/s of memory bandwidth per core), so the engine
has to use them without giving up any of its guarantees. Stage 1 moves the expensive part, mapping
messages to rows, onto a pool of workers while one applier applies the results in log order. Stage 2
splits the applier into writers that each own a group of partitions.

## Motivation

End-to-end profile of file ingest on Axion (`docs/benchmarks/2026-11-11-axion-e2e-ingest.md`, 1 M JSON
readings, 1,000 devices): the ingest loop is 56 % of the server's CPU and most of that is mapping
(JSON scan, decimal conversion, dictionary lookups); appending to the table is a few percent. The loop
is the one thread that cannot be spread today. Sources are already concurrent (one task each, and MQTT
shared subscriptions or Kafka partitions give more), and queries already run one partition per core.

## Invariants that must hold (unchanged from the sequential engine)

1. **Log before table.** A frame is applied only after the log has taken it; a frame the log refused is
   never applied; every frame gets the LSN it would get sequentially.
2. **Log order is apply order.** Frames are applied in LSN order: a partition sees its rows in the order
   they arrived (bounded lateness and the stale check keep their meaning), a `src-state` position is
   noted after the frames it covers, control frames (`connect`, `src-id`, `sp-defs`) take effect at
   their place.
3. **Same result as one thread.** For any input and any number of workers, tables, dictionaries
   (values and ids), continuous-query outputs, counters and rejections are identical to the sequential
   engine. Recovery replays the log through the sequential path and must reach the same state.
4. **Dictionary cap per message.** New dictionary values of one message count together against
   `max_dict_entries`; a row that would exceed it is refused, the rows before it are kept.

## Design — stage 1 (implemented)

```
sources ──frames──▶ sequencer (ingest loop) ──batch──▶ WAL thread
                         │ job(seq, frames, first LSN)
                         ▼
                 workers 1..N (RowMapper clones; no shared state)
                         │ mapped rows, per frame, in a recycled buffer
                         ▼
                 applier (ingest loop, reorder buffer by seq) ──▶ dictionaries, tables, CQs
```

- **Sequencer.** Unchanged up to the log: it drains up to 1,024 frames, pushes them to the WAL as one
  batch (all or nothing) and so knows their LSNs. It then sends the batch as a job, numbered in order,
  to worker `seq mod N` (one queue per worker: no lock shared by the workers).
- **Workers** hold a clone of every JSON, CSV and line protocol source's `RowMapper` (the compiled
  paths or keys and scratch buffers; no state carries from one message to the next). They map each user frame of their job
  **without access to the dictionaries**: every dictionary value is left as a pending name. A worker
  never reads or writes shared state, so N workers scale with no atomic operation per row.
- **Workers learn dictionary ids.** A result goes back to its worker after it was applied, carrying
  the ids the applier gave to the values the worker had left pending; the worker remembers them (per
  source and column, up to 1 M values) and resolves those values itself afterwards. Ids never change
  once given, so nothing is ever invalidated, and nothing is shared between threads: in steady state
  the applier hashes only values it has never seen (a TSBS row has 10 dictionary columns: 10 hash
  lookups per row taken off the one serial thread).
- **`Mapped`**, the output of mapping one message (row-major cells, per-row errors, pending names), is
  a value of its own. The applier returns it to its worker after use, so buffers are reused and nothing
  is allocated per message in steady state; the memory a worker touches stays in its own L2.
- **Applier** (the ingest loop thread): results come back out of order and wait in a reorder buffer
  until their sequence number is next, so frames are applied strictly in LSN order. For a mapped frame
  it resolves the pending names (known values to their ids, new values checked against the cap per
  message, in row order: invariant 4) and appends the rows through `Table::writer` exactly as the
  sequential path does. Every other frame (market data, control frames, Sparkplug sources, whose
  BIRTH definitions make each message depend on the ones before) is handled by the applier as before.
- **Bounded in-flight work.** At most `2 × N` jobs are outstanding; beyond that the sequencer applies
  results before it takes more frames, so memory stays bounded and backpressure reaches the sources.
- **Shutdown and ticks** wait for the outstanding jobs, so positions, checkpoints and the final flush
  see everything that was logged.
- **`storage.ingest_workers`**: 0 keeps the sequential engine; the default is the number of cores
  minus two (the ingest loop and the log thread), at most 32.

## Design — stage 2 (planned): writers per partition group

The applier becomes S writers, each owning the partitions whose key falls in its group (key mod S), with
its own chunk lists and no partition lock. The reorder buffer stays in front of them, so each writer
still sees its rows in LSN order (invariant 2). Open points: dictionary interning shared by the writers
(new values are rare: a mutex on the slow path, per-writer caches of known ids, since ids never change);
continuous queries keyed by the partition column shard with the writers, others take the merged order;
the WAL becomes one log per group when one log thread is the limit (about 1 GB/s of records).

## Alternatives considered

- **Do nothing.** One core per server: 3 M readings/s whatever the machine.
- **One ingest loop per source.** Simple, but a single large subscription (one MQTT topic tree for a
  whole plant, one Kafka topic) still runs on one core, and two sources writing one table contend on it.
- **Shared tables behind finer locks, mapped and appended by every worker.** Breaks invariant 2 (rows of
  one device from two messages could be appended out of order) and costs atomic operations per row.
- **Lock-free concurrent dictionaries read by the workers.** Saves the applier one hash per dictionary
  cell but puts shared cache lines on every worker's path; the applier's hash is cheaper than the
  coherence traffic at 100+ cores.

## Verification

- Property test: random message streams (JSON, `rows` batches and line protocol, good and bad rows,
  new and known dictionary values, a small dictionary cap) mapped by the pooled path with 1, 2 and 7 workers give the same tables,
  dictionaries and counters as the sequential path.
- End-to-end tests run with workers on: restarts, crash and lost WAL (`tests/e2e.rs`), real brokers.
- Measured on Axion: rows/s and CPU per row for 0, 1, 2 workers on 4 vCPU, and the scaling curve on a
  larger Axion instance (`c4a-standard-48`/`-72`).

## Rollout / de-scope impact

Stage 1: about one engineering day, behind `storage.ingest_workers` (0 = previous behaviour). Stage 2:
a few days; it does not change formats. Nothing else moves if stage 2 slips.
