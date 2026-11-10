#!/usr/bin/env bash
# WAL power-loss rig (PRD v4.1 F-05 / N-09; v3.2 RFC-015). Linux + root only.
#
# For each iteration: build a dm-flakey device over a loop file, mount ext4, start the torture
# writer with the ack file on the *host* filesystem, let it run, then make the device drop all
# writes (simulating power loss at the storage layer), SIGKILL the writer, tear down, remount
# the underlying loop device directly and run the verifier: every acknowledged LSN must be
# present and every record must pass CRC. Any failure stops the rig with a non-zero exit.
#
# Usage: sudo scripts/power-loss-rig.sh <iterations> [path-to-wal-torture]
set -euo pipefail
ITER="${1:-10}"
BIN="${2:-$(dirname "$0")/../target/release/wal-torture}"
WORK="${WORK:-/tmp/ankaa-ploss}"
IMG="$WORK/disk.img"; MNT="$WORK/mnt"; ACK="$WORK/ack"
DM=ankaa_flakey
[[ $EUID -eq 0 ]] || { echo "run as root"; exit 2; }
command -v dmsetup >/dev/null || { echo "dmsetup missing (apt-get install dmsetup)"; exit 2; }
[[ -x "$BIN" ]] || { echo "missing $BIN (cargo build --release -p ankaa-wal --bin wal-torture)"; exit 2; }
mkdir -p "$WORK" "$MNT"
# Clean up leftovers from an interrupted run.
umount -l "$MNT" 2>/dev/null || true
dmsetup remove --retry "$DM" 2>/dev/null || true
[[ -f "$IMG" ]] && losetup -j "$IMG" | cut -d: -f1 | xargs -r -n1 losetup -d 2>/dev/null || true
cleanup() {
  umount "$MNT" 2>/dev/null || true
  dmsetup remove "$DM" 2>/dev/null || true
  [[ -n "${LOOP:-}" ]] && losetup -d "$LOOP" 2>/dev/null || true
}
trap cleanup EXIT
rm -f "$IMG" "$ACK"; truncate -s 256M "$IMG"
LOOP=$(losetup --find --show "$IMG")
SECTORS=$(blockdev --getsz "$LOOP")
mkfs.ext4 -q -F "$LOOP"
pass=0
for i in $(seq 1 "$ITER"); do
  # up interval 1000s (never flips by itself); we swap tables manually below
  dmsetup create "$DM" --table "0 $SECTORS flakey $LOOP 0 1000 0"
  mount /dev/mapper/$DM "$MNT"
  # Every 50 iterations start from an empty WAL so the 256 MB image never fills up; within a
  # group each iteration recovers from the previous one's torn state (that is part of the test).
  if (( (i - 1) % 50 == 0 )); then rm -rf "$MNT/wal" "$ACK"; fi
  mkdir -p "$MNT/wal"
  "$BIN" write "$MNT/wal" "$ACK" --commit-every 8 --payload-len 300 2>/dev/null &
  W=$!
  sleep "0.$((RANDOM % 9 + 1))"
  # power loss. dm-flakey's drop_writes makes the kernel *accept* writes that never reach the
  # device, so a running writer would keep acknowledging commits after the cut (which no real
  # power loss allows). Freeze the writer first: nothing is acked after this instant. Dirty
  # pages not yet fdatasync'ed are then dropped by the table swap (no flush on suspend).
  if ! kill -STOP $W 2>/dev/null; then
    wait $W; rc=$?
    echo "ITERATION $i: writer exited early with code $rc (environment problem, not a durability result)"; exit 1
  fi
  dmsetup suspend --nolockfs --noflush "$DM"
  dmsetup load "$DM" --table "0 $SECTORS flakey $LOOP 0 0 1 1 drop_writes"
  dmsetup resume "$DM"
  kill -9 $W 2>/dev/null || true
  wait $W 2>/dev/null || true
  umount -l "$MNT" 2>/dev/null || true
  dmsetup remove --retry "$DM"
  # "power back on": the only data that exists is what reached the loop device before the drop
  fsck.ext4 -fy "$LOOP" >/dev/null 2>&1 || true
  mount "$LOOP" "$MNT"
  if "$BIN" verify "$MNT/wal" "$ACK" >"$WORK/verify.$i.log" 2>&1; then
    pass=$((pass+1)); tail -1 "$WORK/verify.$i.log"
  else
    echo "ITERATION $i FAILED:"; cat "$WORK/verify.$i.log"; exit 1
  fi
  umount "$MNT"
done
echo "power-loss rig: $pass/$ITER iterations passed"
