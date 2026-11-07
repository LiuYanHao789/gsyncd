#!/usr/bin/env bash
# End-to-end test: builds the real binary, runs it against a real directory
# tree and asserts what actually lands on the target.
#
#   bash scripts/e2e.sh
#
# Everything happens under .e2e/ (gitignored). Paths are converted to the
# native form before they are written into the TOML: on MSYS/Cygwin a /c/...
# path is shell syntax, not a real path, and gsyncd would read it literally.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOTDIR=$(cd "$HERE/.." && pwd)
OUT="$ROOTDIR/.e2e"

# go: PATH first, then the usual install locations.
if ! command -v go > /dev/null 2>&1; then
  for c in /usr/local/go/bin "$HOME/go/bin"; do
    if [ -x "$c/go" ]; then PATH="$c:$PATH"; break; fi
  done
fi
command -v go > /dev/null 2>&1 || { echo "go not found" >&2; exit 1; }

EXE=""
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) EXE=".exe";;
esac
BIN="$OUT/gsyncd$EXE"
# Windows builds need the native form of -o: given an MSYS path the toolchain
# treats "/c/..." as relative and quietly writes C:\c\... instead.
BOUT="$BIN"
if command -v cygpath > /dev/null 2>&1; then BOUT=$(cygpath -m "$BIN"); fi

# A previous failed run may have left a watcher behind, and it would keep
# copying the tree we are about to recreate.
if command -v taskkill > /dev/null 2>&1; then
  taskkill //F //IM "gsyncd$EXE" > /dev/null 2>&1 || true
fi
rm -rf "$OUT"
mkdir -p "$OUT"
( cd "$ROOTDIR" && go build -o "$BOUT" ./cmd/gsyncd )

# The TOML wants native paths; cygpath -m gives C:/... with forward slashes.
T="$OUT"
if command -v cygpath > /dev/null 2>&1; then T=$(cygpath -m "$T"); fi

mkdir -p "$T/src/sub/deep" "$T/dst"
echo one > "$T/src/sub/deep/a.txt"
echo two > "$T/src/top.txt"
echo stale > "$T/dst/stale.txt"
# Excluded at every layer: never watched, never copied, never transferred.
mkdir -p "$T/src/cache"
echo junk > "$T/src/cache/blob.bin"
echo tmp > "$T/src/scratch.tmp"

cat > "$T/template.toml" <<'TOML'
[global]
log_level = "debug"
watch_interval = "1s"
status_file = "@T@/status.json"
status_interval = "1s"

[[sync]]
source = "@T@/src"
target = "@T@/dst"
mode = "direct"
delay = 1
delete = true
excludes = ["*.tmp", "/cache/"]
TOML

sed "s|@T@|$T|g" "$T/template.toml" > "$T/gsyncd.toml"
cat "$T/gsyncd.toml"

"$BIN" -config "$T/gsyncd.toml" -check
"$BIN" -config "$T/gsyncd.toml" -nodaemon > "$T/out.log" 2>&1 &
PID=$!
sleep 3
echo "--- after startup"
find "$T/dst" -type f | sort
for f in "dst/scratch.tmp" "dst/cache/blob.bin"; do
  if [ -e "$T/$f" ]; then
    echo "FAIL: excluded path was copied: $f" >&2
    exit 1
  fi
done
if [ -e "$T/dst/stale.txt" ]; then
  echo "FAIL: delete = true did not remove the stale file" >&2
  exit 1
fi
for there in "dst/top.txt" "dst/sub/deep/a.txt"; do
  if [ ! -e "$T/$there" ]; then
    echo "FAIL: missing after startup: $there" >&2
    exit 1
  fi
done

echo "--- modify"
echo three > "$T/src/sub/deep/a.txt"
echo new > "$T/src/newfile.txt"
mkdir -p "$T/src/sub/newdir"
echo inside > "$T/src/sub/newdir/x.txt"
echo junk2 > "$T/src/cache/blob.bin"
echo tmp2 > "$T/src/scratch.tmp"
sleep 4
find "$T/dst" -type f | sort
for f in "dst/scratch.tmp" "dst/cache/blob.bin"; do
  if [ -e "$T/$f" ]; then
    echo "FAIL: excluded path was synced: $f" >&2
    exit 1
  fi
done
if [ "$(cat "$T/dst/sub/deep/a.txt")" != "three" ]; then
  echo "FAIL: modification not propagated" >&2
  exit 1
fi
if [ ! -e "$T/dst/sub/newdir/x.txt" ]; then
  echo "FAIL: new subdirectory not synced" >&2
  exit 1
fi

echo "--- delete"
rm -rf "$T/src/sub/deep"
sleep 4
find "$T/dst" -type f | sort
if [ -e "$T/dst/sub/deep" ]; then
  echo "FAIL: deleted directory still on the target" >&2
  exit 1
fi

# A rename must not turn into a full re-upload, and the old name must not
# survive on the target. Renaming a directory is the same story for its whole
# subtree: watch bookkeeping and pending entries have to follow the move.
echo "--- rename"
mv "$T/src/top.txt" "$T/src/renamed.txt"
mv "$T/src/sub/newdir" "$T/src/sub/moveddir"
echo inside2 > "$T/src/sub/moveddir/y.txt"
sleep 4
find "$T/dst" -type f | sort
for gone in "dst/top.txt" "dst/sub/newdir"; do
  if [ -e "$T/$gone" ]; then
    echo "FAIL: old name survived the rename: $gone" >&2
    exit 1
  fi
done
for there in "dst/renamed.txt" "dst/sub/moveddir/x.txt" "dst/sub/moveddir/y.txt"; do
  if [ ! -e "$T/$there" ]; then
    echo "FAIL: rename did not reach the target: $there" >&2
    exit 1
  fi
done

echo "--- status"
"$BIN" -status "$T/status.json"

kill "$PID" 2>/dev/null || true
sleep 1
echo "--- log"
cat "$T/out.log"
echo "E2E OK"
