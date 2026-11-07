#!/usr/bin/env bash
# Cross-compile every release target into dist/, then write dist/SHA256SUMS.
#
#   bash scripts/dist.sh                  # version from the Makefile default
#   VERSION=0.1.22 bash scripts/dist.sh   # stamp a specific version
#
# All targets are static (CGO_ENABLED=0), so the output runs on any kernel of
# the matching arch without a libc, a Lua interpreter or a Go toolchain.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
cd "$ROOT"
OUT="$ROOT/dist"

# Prefer a go on PATH; fall back to the usual install locations so the script
# also works on a machine where go is not globally installed.
if ! command -v go > /dev/null 2>&1; then
  for c in /usr/local/go/bin "$HOME/go/bin"; do
    if [ -x "$c/go" ]; then PATH="$c:$PATH"; break; fi
  done
fi
command -v go > /dev/null 2>&1 || { echo "go not found; install Go 1.23+ or put it on PATH" >&2; exit 1; }

# Keep in step with $(VERSION) in the Makefile: a dist built without an
# explicit VERSION must not advertise a release that is two versions old.
VERSION=${VERSION:-0.1.22}
LDFLAGS="-s -w -X main.version=$VERSION"
mkdir -p "$OUT"

# go is a native Windows binary here, so an MSYS path like /c/foo is not an
# absolute path to it: -o /c/foo/x quietly writes C:\c\foo\x. Convert once
# and use the native form for every path handed to the toolchain.
NOUT="$OUT"
if command -v cygpath > /dev/null 2>&1; then NOUT=$(cygpath -m "$OUT"); fi

build() {
  # printf would treat a format starting with '-' as an option, so prefix it
  # with a literal that cannot be confused for one.
  printf '%s %-8s %-6s %s\n' '-->' "$1" "$2" "$3"
  CGO_ENABLED=0 GOOS="$1" GOARCH="$2" GOARM="${4:-}" \
    go build -trimpath -ldflags "$LDFLAGS" -o "$NOUT/$3" ./cmd/gsyncd
}

build linux   amd64 gsyncd-linux-amd64
build linux   arm64 gsyncd-linux-arm64
build linux   arm   gsyncd-linux-arm 7
build darwin  amd64 gsyncd-darwin-amd64
build darwin  arm64 gsyncd-darwin-arm64
build windows amd64 gsyncd-windows-amd64.exe

# Checksums make it obvious when a copy got truncated in transit.
( cd "$OUT" && sha256sum gsyncd-* > SHA256SUMS )
echo
ls -l "$OUT"
echo ALL-OK