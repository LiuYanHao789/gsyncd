#!/usr/bin/env bash
# Smoke test for a freshly copied gsyncd binary. Run it on the target host
# BEFORE pointing the daemon at real data: it proves the binary runs on this
# kernel/arch, that TOML parsing works, and that a copy actually lands.
#
#   bash scripts/smoke.sh ./dist/gsyncd-linux-amd64
#   bash scripts/smoke.sh /usr/local/bin/gsyncd
#
# Everything happens in one directory under the current working directory
# (override with WORKDIR=/some/where). mktemp -d is deliberately not used: on
# some hosts /tmp carries restrictive ACLs or noexec, and a copy that fails
# there says nothing about the real target filesystem.
set -euo pipefail
BIN=${1:-gsyncd}
command -v "$BIN" > /dev/null 2>&1 || { echo "not found: $BIN" >&2; exit 1; }

echo "== version"
"$BIN" -version

T=${WORKDIR:-$PWD/.gsyncd-smoke}
rm -rf "$T"
mkdir -p "$T"
trap 'rm -rf "$T"' EXIT
# Inside the TOML the path is plain data, so it is NOT translated by the shell
# the way command-line arguments are. On MSYS/Cygwin that means a /c/... path in
# the file would be read literally; convert it to the native form up front.
if command -v cygpath > /dev/null 2>&1; then T=$(cygpath -m "$T"); fi
mkdir -p "$T/src/sub/deep" "$T/src/cache"
echo hello > "$T/src/top.txt"
echo nested > "$T/src/sub/deep/a.txt"
echo junk > "$T/src/scratch.tmp"
echo blob > "$T/src/cache/blob.bin"
# Pre-existing target content: delete = true means the first run must remove it.
mkdir -p "$T/dst"
echo stale > "$T/dst/stale.txt"

cat > "$T/g.toml" <<TOML
[global]
log_level = "debug"
status_file = "$T/status.json"
status_interval = "2s"
nodaemon = true
# Only used by the polling backend (non-Linux). Linux ignores it; keeping it
# small here means the same script verifies the live path on every platform.
watch_interval = "2s"

[[sync]]
name = "smoke"
mode = "direct"
source = "$T/src"
target = "$T/dst"
delay = 1
delete = true
excludes = [
  "*.tmp",
  "/cache/",
]
TOML

echo "== check"
"$BIN" -config "$T/g.toml" -check

echo "== once"
"$BIN" -config "$T/g.toml" -once

fail=0
want() { if [ ! -e "$1" ]; then echo "FAIL missing: $1" >&2; fail=1; fi; }
dont() { if [ -e "$1" ]; then echo "FAIL should not exist: $1" >&2; fail=1; fi; }
want "$T/dst/top.txt"
want "$T/dst/sub/deep/a.txt"
dont "$T/dst/scratch.tmp"
dont "$T/dst/cache"
dont "$T/dst/stale.txt"

# Now the live path: the daemon must pick up a change without a restart.
echo "== live"
"$BIN" -config "$T/g.toml" &
PID=$!
sleep 2
echo changed > "$T/src/top.txt"
# Create several directories back to back and put a file in each one
# immediately. mkdir and the first write are microseconds apart, which is
# exactly the window where a watch-based watcher loses files: the kernel only
# reports changes made under a watch that already existed. One directory is a
# coin flip; five in a row is a real test.
for n in later later2 later3 later4 later5; do
  mkdir -p "$T/src/$n"
  echo "$n" > "$T/src/$n/new.txt"
done
# A file nested two levels down, created the same way: the intermediate
# directory is new too, so both watches are installed after the write.
mkdir -p "$T/src/later/deep/deeper"
echo deep > "$T/src/later/deep/deeper/x.txt"
for i in $(seq 1 40); do
  [ -f "$T/dst/later5/new.txt" ] && [ -f "$T/dst/later/deep/deeper/x.txt" ] && break
  sleep 0.5
done
for n in later later2 later3 later4 later5; do
  want "$T/dst/$n/new.txt"
done
want "$T/dst/later/deep/deeper/x.txt"
if [ "$(cat "$T/dst/top.txt" 2>/dev/null)" != "changed" ]; then
  echo "FAIL modification not propagated" >&2; fail=1
fi

echo "== status"
"$BIN" -status "$T/status.json" || true

kill -TERM "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true

if [ "$fail" -ne 0 ]; then echo "SMOKE FAILED" >&2; exit 1; fi
echo "SMOKE OK"