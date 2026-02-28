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

# cors coverage
CORS_XML="$WORKDIR/cors.xml"
cat > "$CORS_XML" <<'EOF'
<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>*</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>*</AllowedHeader>
  </CORSRule>
</CORSConfiguration>
EOF

target/debug/s4 -C "$CFG_DIR" cors set "ci/$SRC_BUCKET" "$CORS_XML"
target/debug/s4 -C "$CFG_DIR" cors get "ci/$SRC_BUCKET" > "$WORKDIR/cors-get.out"
rg -q "CORSConfiguration|CORSRule" "$WORKDIR/cors-get.out"
target/debug/s4 -C "$CFG_DIR" cors remove "ci/$SRC_BUCKET"

# encrypt coverage
ENC_XML="$WORKDIR/encryption.xml"
cat > "$ENC_XML" <<'EOF'
<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>AES256</SSEAlgorithm>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>
EOF

target/debug/s4 -C "$CFG_DIR" encrypt set "ci/$SRC_BUCKET" "$ENC_XML"
target/debug/s4 -C "$CFG_DIR" encrypt info "ci/$SRC_BUCKET" > "$WORKDIR/encrypt-info.out"
rg -q "ServerSideEncryptionConfiguration|SSEAlgorithm" "$WORKDIR/encrypt-info.out"
target/debug/s4 -C "$CFG_DIR" encrypt clear "ci/$SRC_BUCKET"

# event coverage
EVENT_XML="$WORKDIR/notification.xml"
cat > "$EVENT_XML" <<'EOF'
<NotificationConfiguration>
  <QueueConfiguration>
    <Id>s4-ci-event</Id>
    <Event>s3:ObjectCreated:*</Event>
    <Queue>arn:minio:sqs::1:webhook</Queue>
  </QueueConfiguration>
</NotificationConfiguration>
EOF

target/debug/s4 -C "$CFG_DIR" event add "ci/$SRC_BUCKET" "$EVENT_XML"
target/debug/s4 -C "$CFG_DIR" event ls "ci/$SRC_BUCKET" > "$WORKDIR/event-list.out"
rg -q "NotificationConfiguration|QueueConfiguration|Event" "$WORKDIR/event-list.out"
target/debug/s4 -C "$CFG_DIR" event rm "ci/$SRC_BUCKET" --force

# idp coverage (placeholder behavior)
if target/debug/s4 -C "$CFG_DIR" idp openid > "$WORKDIR/idp-openid.out"; then
  rg -q "not implemented" "$WORKDIR/idp-openid.out"
else
  echo "[ci] idp openid command unexpectedly failed" >&2
  exit 1
fi
if target/debug/s4 -C "$CFG_DIR" idp ldap > "$WORKDIR/idp-ldap.out"; then
  rg -q "not implemented" "$WORKDIR/idp-ldap.out"
else
  echo "[ci] idp ldap command unexpectedly failed" >&2
  exit 1
fi

# ilm coverage (placeholder behavior)
if target/debug/s4 -C "$CFG_DIR" ilm rule > "$WORKDIR/ilm-rule.out"; then
  rg -q "not implemented" "$WORKDIR/ilm-rule.out"
else
  echo "[ci] ilm rule command unexpectedly failed" >&2
  exit 1
fi
if target/debug/s4 -C "$CFG_DIR" ilm restore > "$WORKDIR/ilm-restore.out"; then
  rg -q "not implemented" "$WORKDIR/ilm-restore.out"
else
  echo "[ci] ilm restore command unexpectedly failed" >&2
  exit 1
fi

# global flags coverage: resolve/custom header/limits
EP_HOSTPORT="${S4_E2E_ENDPOINT#http://}"
EP_HOSTPORT="${EP_HOSTPORT#https://}"
if [[ "$EP_HOSTPORT" == *":"* ]]; then
  EP_HOST="${EP_HOSTPORT%%:*}"
  EP_PORT="${EP_HOSTPORT##*:}"
else
  EP_HOST="$EP_HOSTPORT"
  EP_PORT="80"
fi

target/debug/s4 -C "$CFG_DIR"   --resolve "${EP_HOST}:${EP_PORT}=${EP_HOST}"   --limit-download "1G"   --custom-header "x-s4-ci: globals"   ls ci > "$WORKDIR/globals-ls.out"

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

# mirror/sync flags coverage: --dry-run should not copy
DRYRUN_OUT="$WORKDIR/dryrun.out"
target/debug/s4 -C "$CFG_DIR" mirror --dry-run "ci/$SRC_BUCKET/photos" "ci/$DST_BUCKET/dry-run" > "$DRYRUN_OUT"
rg -q "dry-run: true" "$DRYRUN_OUT"
if target/debug/s4 -C "$CFG_DIR" get "ci/$DST_BUCKET/dry-run/2024/a.txt" "$WORKDIR/dryrun-got.txt"; then
  echo "[ci] dry-run unexpectedly copied object" >&2
  exit 1
fi

# --exclude should skip matching keys
EXCL_LOCAL="$WORKDIR/exclude.tmp"
printf 'exclude-me-%s
' "$TS" > "$EXCL_LOCAL"
target/debug/s4 -C "$CFG_DIR" put "$EXCL_LOCAL" "ci/$SRC_BUCKET/photos/2024/exclude.tmp"
target/debug/s4 -C "$CFG_DIR" sync --exclude "*.tmp" "ci/$SRC_BUCKET/photos" "ci/$DST_BUCKET/exclude-copy"
if target/debug/s4 -C "$CFG_DIR" get "ci/$DST_BUCKET/exclude-copy/2024/exclude.tmp" "$WORKDIR/exclude-got.tmp"; then
  echo "[ci] --exclude did not filter *.tmp" >&2
  exit 1
fi

# --remove should delete extraneous object on destination
target/debug/s4 -C "$CFG_DIR" cp "$SRC1" "ci/$DST_BUCKET/sync-copy/2024/extraneous.txt"
target/debug/s4 -C "$CFG_DIR" sync --remove "ci/$SRC_BUCKET/photos" "ci/$DST_BUCKET/sync-copy"
if target/debug/s4 -C "$CFG_DIR" get "ci/$DST_BUCKET/sync-copy/2024/extraneous.txt" "$WORKDIR/extraneous.txt"; then
  echo "[ci] --remove did not clean extraneous object" >&2
  exit 1
fi


# --older-than should skip fresh objects
target/debug/s4 -C "$CFG_DIR" sync --older-than "365d" "ci/$SRC_BUCKET/photos" "ci/$DST_BUCKET/older-than"
if target/debug/s4 -C "$CFG_DIR" get "ci/$DST_BUCKET/older-than/2024/a.txt" "$WORKDIR/older-than-a.txt"; then
  echo "[ci] --older-than unexpectedly copied fresh object" >&2
  exit 1
fi

# --newer-than should include fresh objects
target/debug/s4 -C "$CFG_DIR" sync --newer-than "365d" "ci/$SRC_BUCKET/photos" "ci/$DST_BUCKET/newer-than"
target/debug/s4 -C "$CFG_DIR" get "ci/$DST_BUCKET/newer-than/2024/a.txt" "$WORKDIR/newer-than-a.txt"
cmp -s "$SRC1" "$WORKDIR/newer-than-a.txt"


# --watch should continuously mirror new objects
WATCH_EXPECT="$WORKDIR/watch.txt"
WATCH_GOT="$WORKDIR/watch-got.txt"
WATCH_LOG="$WORKDIR/watch.log"
printf 'watch-check-%s
' "$TS" > "$WATCH_EXPECT"
S4_SYNC_WATCH_INTERVAL_SEC=1 target/debug/s4 -C "$CFG_DIR" sync --watch "ci/$SRC_BUCKET/photos" "ci/$DST_BUCKET/watch-copy" > "$WATCH_LOG" 2>&1 &
WATCH_PID=$!
cleanup_watch() { kill "$WATCH_PID" 2>/dev/null || true; wait "$WATCH_PID" 2>/dev/null || true; }
trap 'cleanup_watch; rm -rf "$WORKDIR"' EXIT
sleep 2
target/debug/s4 -C "$CFG_DIR" put "$WATCH_EXPECT" "ci/$SRC_BUCKET/photos/2024/watch.txt"
for _ in $(seq 1 15); do
  if target/debug/s4 -C "$CFG_DIR" get "ci/$DST_BUCKET/watch-copy/2024/watch.txt" "$WATCH_GOT" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
cmp -s "$WATCH_EXPECT" "$WATCH_GOT"
cleanup_watch
trap 'rm -rf "$WORKDIR"' EXIT

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


# multipart upload coverage (file > threshold)
MP_LOCAL="$WORKDIR/multipart.bin"
MP_GOT="$WORKDIR/multipart-got.bin"
python3 - <<'PYS' > "$MP_LOCAL"
import sys
sys.stdout.buffer.write(b'Z' * (17 * 1024 * 1024))
PYS

target/debug/s4 -C "$CFG_DIR" put "$MP_LOCAL" "ci/$SRC_BUCKET/mp/large.bin"
target/debug/s4 -C "$CFG_DIR" get "ci/$SRC_BUCKET/mp/large.bin" "$MP_GOT"
cmp -s "$MP_LOCAL" "$MP_GOT"
target/debug/s4 -C "$CFG_DIR" rm "ci/$SRC_BUCKET/mp/large.bin"

target/debug/s4 -C "$CFG_DIR" rm "ci/$SRC_BUCKET/photos/2024/a.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$SRC_BUCKET/photos/2024/b.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$SRC_BUCKET/photos/2024/exclude.tmp"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/sync-copy/2024/a.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/sync-copy/2024/b.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/mirror-copy/2024/a.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/mirror-copy/2024/b.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/exclude-copy/2024/a.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/exclude-copy/2024/b.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/newer-than/2024/a.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/newer-than/2024/b.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$SRC_BUCKET/photos/2024/watch.txt"
target/debug/s4 -C "$CFG_DIR" rm "ci/$DST_BUCKET/watch-copy/2024/watch.txt"

target/debug/s4 -C "$CFG_DIR" rb "ci/$SRC_BUCKET"
target/debug/s4 -C "$CFG_DIR" rb "ci/$DST_BUCKET"
target/debug/s4 -C "$CFG_DIR" alias rm ci

echo "[ci] S3 integration cases passed"
