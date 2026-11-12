# Releasing

A release is a tag on `main`. Pushing the tag `vX.Y.Z` runs `.github/workflows/release.yml`, which builds
`ankaad` for Linux arm64, Linux x86-64 and macOS arm64, publishes the multi-arch container image
`ghcr.io/ualsweb/ankaa:X.Y.Z` (and moves `latest`), and creates the GitHub release with the binaries and
their SHA-256 files. Nothing is built or uploaded by hand.

Versions before 1.0: a minor bump (`0.2.0`) for anything that changes behaviour or an interface, a patch
bump (`0.2.1`) for fixes only. The changelog says what breaks.

## Checklist

1. **Everything is on `main` and CI is green** on the commit to release: `ci`, `miri`, `sve-qemu`,
   `brokers`, `python`, `fuzz-smoke` (`gh run list --branch main`). A workflow that was cancelled by a
   later push is not green: re-run it.
2. **Version.** `version` in `Cargo.toml` (`[workspace.package]`), `crates/ankaa-py/Cargo.toml` and
   `crates/ankaa-py/pyproject.toml`; then `cargo check` in the workspace, `crates/ankaa-py`, `bench` and
   `fuzz` so that the four lock files follow.
3. **Changelog.** A section `## X.Y.Z — date` at the top of `CHANGELOG.md`: breaking changes first, each
   with what to do about it; an "Upgrading" line that says whether data written by the previous release is
   read as it is.
4. **Documents say what the code does.** `README.md` (capabilities, status table), `docs/STATUS.md`,
   `docs/ROADMAP.md`, `docs/architecture.md`, `docs/deployment.md` (every configuration key),
   `docs/metrics-semantics.md` (every metric), `deploy/ankaad.example.toml`, the site under `site/`.
   `ankaad config print` and `curl /metrics` are the lists to check against.
5. **The demo still tells the truth.** `examples/factory`: `docker compose build ankaa && docker compose up -d`,
   wait a few minutes, check the dashboard at http://localhost:3000, then refresh the two assets the README
   shows when the queries or the output changed: `vhs examples/factory/demo.tape` (writes
   `docs/assets/factory-demo.gif` and `.mp4`) and a screenshot of the dashboard as
   `docs/assets/factory-dashboard.png`.
6. **Gates, locally:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`,
   `cargo test --workspace`, `cargo deny check`.
7. **Tag and push:** `git tag -s vX.Y.Z -m "ANKAA vX.Y.Z"` (or `-a` without a signing key), `git push origin vX.Y.Z`.
8. **Check what was published:** the `release` workflow is green; the GitHub release lists three archives
   and three `.sha256` files; `docker run --rm ghcr.io/ualsweb/ankaa:X.Y.Z --version` prints the version;
   `examples/factory` comes up with the pulled image (`docker compose pull && docker compose up -d`).

## If something is wrong after the tag

Do not move or delete a published tag. Fix on `main` and release the next patch version. The workflow can
rebuild the artefacts of an existing tag (`workflow_dispatch` with the tag) when only the build failed.

## What a release is not

Numbers. Benchmarks are published under `docs/benchmarks/` with their scripts and raw results, on named
hardware, independently of releases.
