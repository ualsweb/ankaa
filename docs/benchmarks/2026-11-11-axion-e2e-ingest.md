# End-to-end ingest profiles on Google Axion — 2026-11-11

Every change of the telemetry pack was measured end to end on real Arm server hardware, before and
after, with the same harness: `scripts/e2e-profile.sh` drives `bench/src/bin/loadgen` into a broker (or
a file), `ankaad` stores every reading through its write-ahead log into a table partitioned by device,
and the script reports the server's own CPU time per stored row (`utime + stime` from `/proc`), the wall
time from the first to the last stored row, peak resident memory and a `perf` profile.

- **Host:** Google Cloud `c4a-standard-4` (Axion, Arm Neoverse V2, 4 vCPU, 16 GB), Ubuntu 24.04,
  `europe-southwest1-a`. Broker (Mosquitto 2, unlimited queues), generator and server on the same VM.
- **Data:** 1,000 devices, one JSON reading per message unless stated; WAL and segments on the boot disk.
- **Profiles:** GCE does not expose the PMU to guests, so `perf` samples `cpu-clock` only (no cycles,
  IPC or cache-miss counters on this host). Server CPU per row is the comparable figure: MQTT throughput
  is bounded by the generator and the single-threaded broker on a 4-vCPU box.

## Results

Server CPU per stored row (lower is better) and rows per second.

| Scenario | 2e8aee7 (before) | 6c22e0a (scanner) | c6f8d24 (+ batching) | ca31409 (+ moderation) |
|---|---|---|---|---|
| File, JSON, 1 M readings | 3,350 ns · 678 k/s | 2,000 ns · 1.23 M/s | 850 ns · 2.72 M/s | 860 ns · **3.19 M/s** |
| MQTT, JSON, 1 publisher | 24,580 ns | 23,660 ns | 20,900 ns | **9,880 ns** |
| MQTT, JSON, 4 publishers | — | — | 17,960 ns | **9,170 ns** (55 k msg/s, generator-bound) |
| MQTT + TLS, JSON | 26,080 ns | 24,320 ns | 25,170 ns | **13,870 ns** |
| MQTT, 10 readings per message (`rows`) | — | 2,095 ns · 483 k/s | 2,025 ns · 477 k/s | **1,255 ns** · 492 k/s |
| MQTT, Sparkplug B, 10 metrics per message (2a2c9cb) | | | | **1,124 ns** · 505 k/s; 4 publishers 1,070 ns · **726 k/s** |

Peak resident memory stayed between 140 and 196 MiB in every run.

## What the profiles showed, and what changed

1. **Parsing was not the bottleneck it looked like.** Replacing the JSON document tree with a
   path-directed single-pass scanner made mapping 2.3× faster in isolation and the file path 1.8× faster
   end to end, but over MQTT it changed almost nothing: about 30 % of the server's CPU was in the
   scheduler (`finish_task_switch`, futex wake-ups) and in `el0_sve_acc`, the trap that restores SVE
   register state after a context switch (glibc's `memcpy` uses SVE on Neoverse V2).
2. **Per-message cross-thread hand-offs.** Each message woke three threads: the MQTT task, the ingest
   loop and the log writer. On a VM a cross-vCPU wake-up is an inter-processor interrupt, a VM exit.
   *Batching* (the ingest loop drains every queued frame and hands the run to the log thread as one
   shared batch; one table lock per message instead of three per row; counters published per batch;
   frames encoded straight into the log buffer) helped where a backlog exists (file: 2.4×) but not at
   32 k messages/s, where every batch is one frame.
3. **Interrupt moderation**, as network cards do it: while frames keep arriving, the ingest loop waits
   `storage.ingest_coalesce_us` (200 µs) between batches and the log thread polls every millisecond
   instead of blocking on its queue, so producers stop waking them. Rows become queryable at the next
   snapshot (50 ms) anyway, so the wait is invisible to queries. Server CPU per MQTT message halved.
4. **What is left over MQTT is the client library**: three quarters of the remaining CPU is in the
   tokio worker that runs rumqttc, which writes and flushes every QoS 1 acknowledgement on its own. The
   engine's share is about 2.5 µs per message. Messages that carry several readings (`rows`, Sparkplug)
   amortise it: about 1.1–1.3 µs per reading.
5. **Outline atomics.** The generic `aarch64-unknown-linux-gnu` build calls a helper for every atomic
   operation (`__aarch64_cas8_acq_rel` and friends, about 11 % of samples) because it cannot assume LSE.
   A build for Neoverse (`-C target-cpu=neoverse-n1`, LSE inline) was measured against it below.

## Generic arm64 build vs Neoverse build (2a2c9cb)

| Scenario | generic | `target-cpu=neoverse-n1` |
|---|---|---|
| File, JSON | 850 ns · 3.22 M/s | 820 ns · 3.12 M/s |
| MQTT, JSON | 10,940 ns | 10,870 ns |
| MQTT, Sparkplug, 1 publisher | 1,124 ns · 505 k/s | 1,080 ns · 508 k/s |
| MQTT, Sparkplug, 4 publishers | 1,070 ns · 726 k/s | 1,085 ns · 749 k/s |

Inlining LSE atomics removes the helper calls from the profile (`__aarch64_*` 9 % → 0.6 %; the rest
comes from the precompiled standard library) but changes the CPU per row by −4 % to +1 %, within
run-to-run noise: the cost is the atomic operation, not the call to it. The release binaries stay
generic `aarch64` (they also run on Armv8.0 cores such as Cortex-A72 gateways); a Neoverse-only build
buys nothing measurable here.

## Reproduce

```sh
scripts/gcp/ankaa-gcp.sh deploy                     # with ANKAA_GCP_VM=<bench VM>
cargo build --release -p ankaad && (cd bench && cargo build --release --bin loadgen)
scripts/e2e-profile.sh json-file --transport file
scripts/e2e-profile.sh json-mqtt [--tls] [--publishers 4]
scripts/e2e-profile.sh array-mqtt --format json-array --batch 10 --messages 200000
scripts/e2e-profile.sh sp-mqtt --format sparkplug --batch 10 --messages 200000
ANKAAD=/path/to/other/ankaad scripts/e2e-profile.sh ...   # A/B against another build
```

Each run writes `target/e2e/<scenario>/` with `summary.txt`, `perf-report.txt` (self time per symbol),
`perf-threads.txt` (CPU by thread) and the server log.
