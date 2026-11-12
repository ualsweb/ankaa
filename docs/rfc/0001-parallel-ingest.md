# RFC-0001: Parallel ingest for many-core Arm servers

- **Status:** Accepted (stage 1 implemented 2026-11-11, stage 2 2026-11-12)
- **Date:** 2026-11-11
- **Affects:** `ankaa-ingest` (`generic`), `bins/ankaad` (`serve`, `tables`), config `[storage] ingest_workers`; PRD F-04 (partition-owned appenders), N-01/N-02 (ingest throughput)

## Summary

`ankaad` ingests through one loop: it takes frames from the sources, hands them to the write-ahead log
and then decodes, maps and appends them, one after the other. One core bounds the ingest rate (about
3 M readings/s for JSON on a Neoverse V2 core). Arm server parts now carry 64 to 136 cores (Neoverse V3
in the Arm AGI CPU, 2 MB of L2 per core but about 6 GB/s of memory bandwidth per core), so the engine
has to use them without giving up any of its guarantees. Stage 1 moves the expensive part, mapping
messages to rows, onto a pool of workers while one applier applies the results in log order. Stage 2
splits the appends among writers that each own a group of partitions.

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

## Design — stage 2 (implemented 2026-11-12): writers per partition group

```
workers ──mapped run──▶ ingest loop: resolve ids, split rows by partition key
                             │ run (shared, read-only) + one row list per writer
                             ▼
                 writers 1..S (writer = key mod S): stale check, append
                             │ what each message came to
                             ▼
                 ingest loop: counters, buffers back to the workers
```

- **What stays in the loop** is what depends on the order of rows of different partitions: giving
  dictionary ids. A mapped run goes to the writers only when every value of every row that is not
  refused has its id already (`Mapped::has_pending`); since the workers learn ids, that is every run
  of a stream whose devices are known. A run with a value never seen, a table with continuous
  queries, a Sparkplug source and recovery take the stage 1 path, row by row.
- **What a writer does** depends on one partition only: the stale check against the partition's
  watermark and the append (`Table::append_shared`: the list of partitions is read, the partition
  locked). All rows of a partition go to one writer, and a writer takes its runs in the order they
  were handed over: every partition gets its rows in LSN order (invariant 2).
- **Pipelined, not joined.** The loop hands a run over and goes on. Waiting for the writers after
  every run was measured first and was a loss: two thread wake-ups per writer cost more than the
  appends of a 1,024-row run (2 to 4 writers halved the rate). The loop waits for the writers only
  where everything must be applied: before a run that goes row by row, before a snapshot tick, at
  shutdown. Ticks, tiering and checkpoints therefore still see tables at a message boundary.
- **Partitions behind their own locks.** `Table` keeps `RwLock<Vec<Mutex<Partition>>>`: whoever
  needs the table to itself (one message row by row, a tiering step, growth of the list) holds the
  outer lock for writing and pays nothing for the inner ones; writers hold it for reading.
- **`storage.ingest_writers`**: unset = a quarter of the cores, at most 8, none below 8 cores; 0 or
  1 = the loop appends.
- Around it, from the same profile: the snapshot tick no longer stalls the loop (source positions
  saved by a thread of their own, table views built on several threads, zone maps computed by their
  first reader, ticks spaced by their own duration under load), and the loop reports where it waits
  (`ankaa_ingest_wait_*_ns_total`).

Not done: dictionary interning shared by the writers (new values still send a run through the
loop), continuous queries sharded with the writers, one log per group.

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
