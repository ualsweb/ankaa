#!/usr/bin/env bash
# Run the SVE2 differential suite under qemu-aarch64 inside the Linux dev VM (colima profile
# `ankaa`), at one vector length. Usage (from the Mac):
#   colima ssh -p ankaa -- <repo>/scripts/vm-qtest.sh 32   # VL bytes: 16|32|64
# Builds into $HOME/target-ankaa inside the VM so the macOS target directory is untouched.
set -euo pipefail
vl="${1:-32}"
cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target-ankaa}"
case "$vl" in 16) a=qtest128;; 32) a=qtest256;; 64) a=qtest512;; *) echo "VL bytes must be 16, 32 or 64" >&2; exit 2;; esac
cargo "$a" "${@:2}"
# branchless-v2 gate on the newest rlib that was just built
rlib=$(ls -t "$CARGO_TARGET_DIR"/debug/deps/libankaa_simd-*.rlib | head -1)
scripts/branchless-gate.sh "$rlib"
