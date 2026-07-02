#!/usr/bin/env bash
# Head-to-head benchmark: rclone vs s3rsync copying the SAME source prefix to a
# FRESH dest, on the SAME link, SEQUENTIALLY (no interference). Reports
# files/s = objects actually written to the dest / DURATION (counts real
# results, no fragile log parsing).
#
# Usage:   ./bench.sh <src-bucket> <dst-bucket> [src-prefix] [seconds]
# Example: ./bench.sh mydata mydata-copy logs/ 240
#
# Env (destination, any S3-compatible: MinIO, Ceph, R2, ...):
#   MINIO_REMOTE_URL  MINIO_REMOTE_ACCESS_KEY  MINIO_REMOTE_SECRET_KEY
# Env (source, AWS S3): standard AWS_* / ~/.aws (env auth). AWS_DEFAULT_REGION.
# Requires: rclone, mc, and a built s3rsync (`cargo build --release`).

set -u
SRC_BUCKET="${1:?usage: bench.sh <src-bucket> <dst-bucket> [src-prefix] [seconds]}"
DST_BUCKET="${2:?usage: bench.sh <src-bucket> <dst-bucket> [src-prefix] [seconds]}"
PREFIX="${3:-}"
DUR="${4:-240}"

: "${MINIO_REMOTE_URL:?set MINIO_REMOTE_URL (dest endpoint)}"
: "${MINIO_REMOTE_ACCESS_KEY:?}"; : "${MINIO_REMOTE_SECRET_KEY:?}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

BIN="${S3RSYNC_BIN:-./target/release/s3rsync}"
command -v rclone >/dev/null 2>&1 || { echo "rclone not found"; exit 1; }
MC="mc"; command -v mc >/dev/null 2>&1 || MC="$HOME/.local/bin/mc"
[ -x "$BIN" ] || command -v "$BIN" >/dev/null 2>&1 || { echo "s3rsync not built ($BIN), run: cargo build --release"; exit 1; }

# ephemeral mc alias (for counting) + rclone config (source via env auth)
ALIAS="s3rsync_bench_$$"
CFG="$(mktemp)"
"$MC" alias set "$ALIAS" "$MINIO_REMOTE_URL" "$MINIO_REMOTE_ACCESS_KEY" "$MINIO_REMOTE_SECRET_KEY" >/dev/null
trap '"$MC" alias rm "$ALIAS" >/dev/null 2>&1; rm -f "$CFG"' EXIT
cat > "$CFG" <<EOF
[s3src]
type = s3
provider = AWS
env_auth = true
region = ${REGION}
[dst]
type = s3
provider = Other
access_key_id = ${MINIO_REMOTE_ACCESS_KEY}
secret_access_key = ${MINIO_REMOTE_SECRET_KEY}
endpoint = ${MINIO_REMOTE_URL}
region = us-east-1
EOF

SRC="s3src:$SRC_BUCKET"; [ -n "$PREFIX" ] && SRC="$SRC/$PREFIX"
count() { "$MC" ls --recursive "$ALIAS/$DST_BUCKET/$1/" 2>/dev/null | wc -l | tr -d ' '; }
run_for() {
  local label="$1"; shift
  echo "── $label : ${DUR}s ─────────────────────────"
  "$@" >"/tmp/bench_$label.log" 2>&1 &
  local pid=$!
  sleep "$DUR"
  kill "$pid" 2>/dev/null; pkill -P "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
}

echo "src=$SRC  dst=$DST_BUCKET  duration=${DUR}s"
"$MC" rm --recursive --force "$ALIAS/$DST_BUCKET/_bench_rclone" "$ALIAS/$DST_BUCKET/_bench_s3rsync" >/dev/null 2>&1

run_for rclone rclone --config "$CFG" copy "$SRC" "dst:$DST_BUCKET/_bench_rclone" \
  --transfers 24 --checkers 48 --s3-no-check-bucket --size-only --stats 5s --stats-one-line
R=$(count _bench_rclone)

run_for s3rsync "$BIN" --src-bucket "$SRC_BUCKET" --src-prefix "$PREFIX" \
  --dst-bucket "$DST_BUCKET/_bench_s3rsync" --transfers 128 --journal /dev/null
S=$(count _bench_s3rsync)

echo
echo "═══════════ RESULT (${DUR}s, same link) ═══════════"
awk -v r="$R" -v s="$S" -v d="$DUR" 'BEGIN{
  printf "  rclone  (24) : %6d files  → %6.1f files/s\n", r, r/d;
  printf "  s3rsync (128): %6d files  → %6.1f files/s\n", s, s/d;
  if (r > 0) printf "  → s3rsync = %.1fx rclone\n", (s/d)/(r/d);
}'
echo "cleanup: mc rm --recursive --force <alias>/$DST_BUCKET/_bench_rclone <alias>/$DST_BUCKET/_bench_s3rsync"
