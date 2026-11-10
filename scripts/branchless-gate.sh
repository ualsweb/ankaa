#!/usr/bin/env bash
# Branchless-v2 gate (PRD v4.1 §4.6).
#
# Inside every SVE2 kernel, a conditional branch is allowed only as *loop control driven by a
# predicate-generating instruction*: the instruction immediately before the branch must be one
# of whilelt/whilele/whilelo/whilels/whilege/whilegt/whilehi/whilehs/ptest/brk*. Disassemblers
# print the SVE condition aliases as flags (b.none=b.eq, b.any=b.ne, b.first=b.mi, b.nfrst=b.pl,
# b.last=b.pl, b.nlast=b.mi), so the check is on the *predecessor*, not on the mnemonic.
# cbz/cbnz/tbz/tbnz and any other conditional branch fail the gate.
#
# Usage: scripts/branchless-gate.sh <object-or-binary>... (ANKAA_SYM_PREFIX=ankaa_sve2_)
set -euo pipefail
prefix="${ANKAA_SYM_PREFIX:-ankaa_sve2_}"
if [[ $# -lt 1 ]]; then echo "usage: $0 <object>..." >&2; exit 2; fi
host=$(rustc -vV | sed -n 's/^host: //p')
OBJDUMP="${OBJDUMP:-$(command -v llvm-objdump || true)}"
[[ -n "$OBJDUMP" ]] || OBJDUMP="$(rustc --print sysroot)/lib/rustlib/$host/bin/llvm-objdump"
fail=0; seen=0
for obj in "$@"; do
  for sym in $("$OBJDUMP" -t "$obj" 2>/dev/null | awk -v p="$prefix" '$NF ~ "^"p {print $NF}' | sort -u); do
    seen=$((seen+1))
    body=$("$OBJDUMP" -d --no-show-raw-insn --disassemble-symbols="$sym" "$obj" | grep -E '^\s*[0-9a-f]+:\s' || true)
    prev=""
    bad=""
    while IFS= read -r line; do
      mn=$(printf '%s' "$line" | awk '{print $2}')
      case "$mn" in
        cbz|cbnz|tbz|tbnz) bad+="$line"$'\n' ;;
        b.*)
          case "$prev" in
            whilelt|whilele|whilelo|whilels|whilege|whilegt|whilehi|whilehs|ptest|brka|brkas|brkb|brkbs|brkn|brkns|brkpa|brkpas|brkpb|brkpbs) ;;
            *) bad+="$line   (predecessor: $prev)"$'\n' ;;
          esac ;;
      esac
      prev="$mn"
    done <<< "$body"
    if [[ -n "$bad" ]]; then
      echo "FAIL $sym: data-dependent branch(es):"; printf '%s' "$bad"; fail=1
    else
      n=$(printf '%s\n' "$body" | grep -c . || true)
      echo "ok   $sym ($n instructions)"
    fi
  done
done
if [[ $seen -eq 0 ]]; then echo "FAIL: no symbols with prefix '$prefix' found in: $*" >&2; exit 1; fi
exit $fail
