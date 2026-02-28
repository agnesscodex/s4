#!/usr/bin/env bash
set -euo pipefail

: "${S4_E2E_ENDPOINT:?S4_E2E_ENDPOINT is required}"
: "${S4_E2E_ACCESS_KEY:?S4_E2E_ACCESS_KEY is required}"
: "${S4_E2E_SECRET_KEY:?S4_E2E_SECRET_KEY is required}"

S4_E2E_REGION="${S4_E2E_REGION:-us-east-1}"

# First run full CRUD flow.
S4_E2E_ALIAS="ci-e2e"
S4_E2E_PREFIX="s4-ci-e2e"
S4_E2E_PATH_STYLE="1"
./scripts/e2e.sh

# Then run dedicated sync flow.
WORKDIR="$(mktemp -d)"
CFG_DIR="$WORKDIR/config"
trap 'rm -rf "$WORKDIR"' EXIT

TS="$(date +%s)"
SRC_BUCKET="s4-sync-src-${TS}"
DST_BUCKET="s4-sync-dst-${TS}"
SRC1="$WORKDIR/src1.txt"
SRC2="$WORKDIR/src2.txt"
OUT1="$WORKDIR/out1.txt"
OUT2="$WORKDIR/out2.txt"

printf 'sync-one-%s\n' "$TS" > "$SRC1"
printf 'sync-two-%s\n' "$TS" > "$SRC2"

cargo build

target/debug/s4 -C "$CFG_DIR" alias set ci "$S4_E2E_ENDPOINT" "$S4_E2E_ACCESS_KEY" "$S4_E2E_SECRET_KEY" --region "$S4_E2E_REGION" --path-style

target/debug/s4 -C "$CFG_DIR" mb "ci/$SRC_BUCKET"
target/debug/s4 -C "$CFG_DIR" mb "ci/$DST_BUCKET"


# ping/ready coverage
target/debug/s4 -C "$CFG_DIR" ping ci > "$WORKDIR/ping.out"
rg -q "alive|latency_ms" "$WORKDIR/ping.out"

target/debug/s4 -C "$CFG_DIR" ready ci > "$WORKDIR/ready.out"
rg -q "ready" "$WORKDIR/ready.out"

target/debug/s4 -C "$CFG_DIR" put "$SRC1" "ci/$SRC_BUCKET/photos/2024/a.txt"
target/debug/s4 -C "$CFG_DIR" put "$SRC2" "ci/$SRC_BUCKET/photos/2024/b.txt"

target/debug/s4 -C "$CFG_DIR" sync "ci/$SRC_BUCKET/photos" "ci/$DST_BUCKET/sync-copy"
target/debug/s4 -C "$CFG_DIR" mirror "ci/$SRC_BUCKET/photos" "ci/$DST_BUCKET/mirror-copy"

target/debug/s4 -C "$CFG_DIR" get "ci/$DST_BUCKET/sync-copy/2024/a.txt" "$OUT1"
target/debug/s4 -C "$CFG_DIR" get "ci/$DST_BUCKET/mirror-copy/2024/b.txt" "$OUT2"

cmp -s "$SRC1" "$OUT1"
cmp -s "$SRC2" "$OUT2"

# find/tree/head coverage
target/debug/s4 -C "$CFG_DIR" find "ci/$SRC_BUCKET/photos" "2024" > "$WORKDIR/find.out"
rg -q "photos/2024/a.txt|2024/a.txt" "$WORKDIR/find.out"

target/debug/s4 -C "$CFG_DIR" tree "ci/$SRC_BUCKET/photos" > "$WORKDIR/tree.out"
rg -q "a.txt" "$WORKDIR/tree.out"

target/debug/s4 -C "$CFG_DIR" head "ci/$SRC_BUCKET/photos/2024/a.txt" 1 > "$WORKDIR/head.out"
rg -q "sync-one" "$WORKDIR/head.out"

# cp/mv coverage
CP_LOCAL="$WORKDIR/cp-local.txt"
CP_BACK="$WORKDIR/cp-back.txt"
printf 'cp-check-%s
' "$TS" > "$CP_LOCAL"

target/debug/s4 -C "$CFG_DIR" cp "$CP_LOCAL" "ci/$SRC_BUCKET/cp/local.txt"
target/debug/s4 -C "$CFG_DIR" cp "ci/$SRC_BUCKET/cp/local.txt" "$CP_BACK"
cmp -s "$CP_LOCAL" "$CP_BACK"

target/debug/s4 -C "$CFG_DIR" cp "ci/$SRC_BUCKET/cp/local.txt" "ci/$DST_BUCKET/cp/copied.txt"
target/debug/s4 -C "$CFG_DIR" mv "ci/$DST_BUCKET/cp/copied.txt" "ci/$DST_BUCKET/cp/moved.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$SRC_BUCKET/cp/local.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/cp/moved.txt"


# pipe coverage
PIPE_EXPECT="$WORKDIR/pipe-expected.txt"
PIPE_GOT="$WORKDIR/pipe-got.txt"
printf 'pipe-check-%s
' "$TS" > "$PIPE_EXPECT"
cat "$PIPE_EXPECT" | target/debug/s4 -C "$CFG_DIR" pipe "ci/$SRC_BUCKET/pipe/stdin.txt"
target/debug/s4 -C "$CFG_DIR" get "ci/$SRC_BUCKET/pipe/stdin.txt" "$PIPE_GOT"
cmp -s "$PIPE_EXPECT" "$PIPE_GOT"
target/debug/s4 -C "$CFG_DIR" rm "ci/$SRC_BUCKET/pipe/stdin.txt"

target/debug/s4 -C "$CFG_DIR" rm "ci/$SRC_BUCKET/photos/2024/a.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$SRC_BUCKET/photos/2024/b.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/sync-copy/2024/a.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/sync-copy/2024/b.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/mirror-copy/2024/a.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/mirror-copy/2024/b.txt"

target/debug/s4 -C "$CFG_DIR" rb "ci/$SRC_BUCKET"
target/debug/s4 -C "$CFG_DIR" rb "ci/$DST_BUCKET"
target/debug/s4 -C "$CFG_DIR" alias rm ci

echo "[ci] S3 integration cases passed"
