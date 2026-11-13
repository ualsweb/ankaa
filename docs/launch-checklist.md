# Launch checklist

What has to be true before the repository is public and before a number is published, in order.
Each line says how to check it. Boxes marked **(maintainer)** need an account, a decision or
hardware and cannot be done from inside the repository.

## 1. The tree is ready to be read by strangers

- [ ] `git status` is clean and `internal/` is not tracked: `git ls-files internal | wc -l` prints `0`.
- [ ] No secrets or private details in the tracked tree:
      `git ls-files -z | xargs -0 grep -nIE 'BEGIN [A-Z ]*PRIVATE KEY|AKIA[0-9A-Z]{16}|ghp_[A-Za-z0-9]{30,}|xox[baprs]-|/Users/|/home/[a-z]+/' | grep -v '^docs/launch-checklist.md'`
      must print nothing that is not an example path in documentation.
- [ ] **(maintainer)** The public repository starts from a single commit made in a **fresh
      clone-free directory**, never from a branch of the development repository (a later
      `git push --all` or `--mirror` from there would publish every earlier commit):
      `git archive --format=tar HEAD | tar -x -C ../ankaa-public && cd ../ankaa-public && git init -b main && git add -A && git commit -s -m "ANKAA-ENGINE: initial public commit"`.
- [ ] **(maintainer)** The author and sign-off address of that commit is the one meant to be
      public: `git config user.email` (GitHub offers `<id>+<login>@users.noreply.github.com`).
- [ ] **(maintainer)** `gh auth login`, create the public repository, push, enable
      "Private vulnerability reporting" (Settings → Security) so `SECURITY.md` works as written,
      protect `main` (require the `ci` checks), enable Dependabot alerts.
- [ ] **(maintainer)** Website: Settings → Pages → Source = **GitHub Actions**; the `pages` workflow
      publishes `site/` to `https://ualsweb.github.io/ankaa/`. If the repository ever moves to another
      account, replace `ualsweb` in `site/*.html`, `README.md`, `NOTICE`, `Cargo.toml` and `pyproject.toml`.
- [ ] **(maintainer)** Repository page: description "Real-time time-series engine for Arm servers.
      Stream in, query in milliseconds.", website = the Pages URL, topics `time-series`, `database`,
      `rust`, `arm64`, `sve2`, `iot`, `mqtt`, `kafka`, `apache-arrow`, `datafusion`, `real-time`,
      `telemetry`, `market-data`; Settings → Social preview = `docs/assets/social-preview.png`;
      enable Discussions and add a *Design partner* label.

## 2. Gates are green

Local, from the repository root (the same commands CI runs):

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
for m in Cargo.toml crates/ankaa-py/Cargo.toml fuzz/Cargo.toml bench/Cargo.toml; do
  cargo deny --manifest-path "$m" --all-features check
done
cargo check --manifest-path fuzz/Cargo.toml
cargo check --manifest-path crates/ankaa-py/Cargo.toml --all-features
cargo check --manifest-path bench/Cargo.toml --all-targets
```

- [ ] All of the above pass.
- [ ] SVE2 kernels under emulation at three vector lengths: `cargo qtest128 && cargo qtest256 && cargo qtest512`
      on aarch64 Linux (from a Mac: `scripts/vm-qtest.sh 16|32|64` in the dev VM).
- [ ] Real brokers: `ANKAA_BROKER_TESTS=1 cargo test -p ankaad --test brokers` with NATS, Kafka and Mosquitto
      running (commands at the top of `bins/ankaad/tests/brokers.rs`).
- [ ] Python package: `cd crates/ankaa-py && maturin develop --release && pytest`.
- [ ] After the first push: every workflow under `.github/workflows/` is green on the Arm runners
      (`ci`, `sve-qemu`, `miri`, `fuzz-smoke`, `power-loss`, `python`, `brokers`). The `ci` job
      prints whether the runner has SVE2; the native SVE2 tests must have run, not been skipped.

## 3. First run on real hardware

On the first Arm server (Graviton4 `c8g`, Axion `c4a`, or equivalent). On Google Cloud,
`scripts/gcp/ankaa-gcp.sh all` creates an Axion VM, runs the steps below and starts a 24 h soak
against a live service (see the header of that script). By hand:

```sh
git clone <repository> && cd ankaa
FIRST_LIGHT_LABEL=c8g.4xlarge scripts/first-light.sh
```

- [ ] `summary.txt` shows `feature_sve2: yes` and `tests-simd-sve2 ok`: the SVE2 assembly ran on
      silicon for the first time (until now it has only run under QEMU).
- [ ] `tests-workspace ok`, `bench ok`, five replay runs, and the crash-recovery section reports the
      same order of row counts before the crash and a recovery time.
- [ ] Copy the result directory into `docs/benchmarks/<date>-<label>-first-light/` and write the
      note from `bench/compare/report-template.md`. Only numbers from this kind of run are quoted
      anywhere (`docs/measurement-protocol.md`); laptop and QEMU figures stay labelled as
      development signals.
- [ ] Comparison run (`bench/compare/README.md`) on the same machine, same day, with
      `ANKAA_BENCH_HOST_LABEL` set. Publish the scripts, the versions and the raw result file with
      the report.

## 4. Soak

A server that has been up for a day tells more than any test:

```sh
export ANKAA_TOKEN=...                      # never on the command line
ankaad serve --config deploy/ankaad.example.toml &     # with wal_dir and checkpoint_dir set
ANKAA_TOKEN=$ANKAA_TOKEN scripts/soak.sh --hours 24 --pid $! --out soak-$(date +%F).csv
```

- [x] `soak: clean` after 24 h (v0.1.0, 2026-11-12, `docs/benchmarks/2026-11-12-axion-24h-soak.md`): no feed gaps, no channel or WAL drops, no WAL errors, no bad
      frames, no restart, memory outside the data tiers (resident minus hot and warm tier bytes) within 50 % of its level after the first hour.
- [ ] Restart the server once in the middle of a second run (SIGTERM) and once with `kill -9`:
      row counts continue, `ankaa_wal_recovered_frames` is non-zero after the crash, file and
      Kafka sources resume without re-reading (`resumes` in the log).
- [ ] If replication is part of the test: a standby reaches the primary's
      `ankaa_wal_durable_lsn`, survives a primary restart, and a deliberately stale standby
      (stopped across a checkpoint) exits with the re-seed message instead of serving a gap.
- [x] Keep the CSV; it goes into the soak note under `docs/benchmarks/` (`bench/soak/`).

## 5. Deployment defaults to confirm on the first internet-reachable host

- [ ] Flight has TLS (`server.tls_cert`/`tls_key`) and a token from the environment; the Postgres
      listener and `/metrics` are on loopback or behind a TLS terminator (`docs/security.md`).
- [ ] `replication.serve_wal` is off unless a standby exists; when it is on, `replication.token_env`
      is set.
- [ ] The systemd unit from `deploy/systemd/` is used (restart on failure, resource limits) and the
      alert rules from `deploy/prometheus/alerts.yml` are loaded (`promtool check rules` passes).
- [ ] Kafka sources beyond the host use `tls = true` (and `sasl_*` where the broker asks for it),
      with the password in the environment. NATS is reached over a private network: its
      connections are not encrypted by ANKAA yet.

## 6. Still open after launch (tracked in `docs/STATUS.md`, "Known gaps")

Performance figures on Graviton4 and Axion, the `data=writeback` power-loss variant, live SBE
verification, bare-metal timestamping, exactly-once sources, fencing for failover.
