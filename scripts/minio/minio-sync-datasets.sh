#!/usr/bin/env bash
# Sync every installed HiTorro dataset from $HITORRO_DATASETS_HOME to
# the MinIO bucket. Uses the `mc` MinIO client (bundled via docker if
# not on your PATH) so the script has zero host-side deps beyond docker.
#
# Layout in the bucket:
#   s3://$BUCKET/datasets/<id>/manifest.yaml
#   s3://$BUCKET/datasets/<id>/data/<file>.ndjson[.bz2|.gz]
#   s3://$BUCKET/datasets/<id>/types/<snake_id>.json
#
# Idempotent — mc's `mirror` command skips unchanged files.
#
# Usage:
#   minio-sync-datasets.sh              # mirror EVERY dataset dir
#   minio-sync-datasets.sh <dataset-id> # mirror ONE dataset dir
#
# Env:
#   HITORRO_DATASETS_HOME          default $HOME/.hitorro/datasets
#   HITORRO_MINIO_S3_PORT          default 9000
#   HITORRO_MINIO_ROOT_USER        default hitorro
#   HITORRO_MINIO_ROOT_PASSWORD    default hitorro-dev-only
#   HITORRO_MINIO_BUCKET           default hitorro
#   HITORRO_MINIO_ALIAS            default local
#   HITORRO_MINIO_ENDPOINT         default http://localhost:$PORT
#     Override for remote MinIO / real S3 (e.g. s3.us-east-1.amazonaws.com)
set -euo pipefail

DATASETS_HOME="${HITORRO_DATASETS_HOME:-$HOME/.hitorro/datasets}"
# Positional arg — when provided, scope to one dataset directory.
ONLY_DATASET="${1:-}"
S3_PORT="${HITORRO_MINIO_S3_PORT:-9000}"
USER="${HITORRO_MINIO_ROOT_USER:-hitorro}"
PASS="${HITORRO_MINIO_ROOT_PASSWORD:-hitorro-dev-only}"
BUCKET="${HITORRO_MINIO_BUCKET:-hitorro}"
ALIAS="${HITORRO_MINIO_ALIAS:-local}"
ENDPOINT="${HITORRO_MINIO_ENDPOINT:-http://localhost:$S3_PORT}"

info() { printf "\033[1;34m→\033[0m %s\n" "$*"; }
die()  { printf "\033[1;31m✗\033[0m %s\n" "$*" >&2; exit 1; }
ok()   { printf "\033[1;32m✓\033[0m %s\n" "$*"; }

[ -d "$DATASETS_HOME" ] || die "no datasets home at $DATASETS_HOME — install some first"

# Prefer native mc for speed; fall back to dockerized mc so the host
# needs zero extra deps beyond docker.
if command -v mc >/dev/null 2>&1; then
    MC=(mc)
    SRC="$DATASETS_HOME"
else
    info "using dockerized mc (native mc not on PATH)"
    MC=(docker run --rm -i --network host
        -v "$HOME/.mc":/root/.mc
        -v "$DATASETS_HOME":/datasets:ro
        minio/mc:latest)
    SRC="/datasets"
fi

info "configuring mc alias $ALIAS → $ENDPOINT"
"${MC[@]}" alias set "$ALIAS" "$ENDPOINT" "$USER" "$PASS" >/dev/null

"${MC[@]}" mb --ignore-existing "$ALIAS/$BUCKET" >/dev/null

if [ -n "$ONLY_DATASET" ]; then
    if [ ! -d "$DATASETS_HOME/$ONLY_DATASET" ]; then
        die "no such dataset dir: $DATASETS_HOME/$ONLY_DATASET"
    fi
    info "mirroring 1 dataset ($ONLY_DATASET) to $ALIAS/$BUCKET/datasets/$ONLY_DATASET/"
    # Note: --remove would purge unrelated objects in the destination; scope
    # scoped-sync to overwrite-only so it doesn't touch the rest of the bucket.
    "${MC[@]}" mirror --overwrite "$SRC/$ONLY_DATASET" \
        "$ALIAS/$BUCKET/datasets/$ONLY_DATASET/" \
        | grep -v '^\.\.\.$' | tail -20 || true
else
    count=$(find "$DATASETS_HOME" -maxdepth 1 -mindepth 1 -type d | wc -l | tr -d ' ')
    info "mirroring $count dataset(s) to $ALIAS/$BUCKET/datasets/"
    "${MC[@]}" mirror --overwrite --remove "$SRC" "$ALIAS/$BUCKET/datasets/" \
        | grep -v '^\.\.\.$' | tail -20 || true
fi

ok "sync done — console: http://localhost:${HITORRO_MINIO_CONSOLE_PORT:-9001}"

# Report total bytes stored (best-effort — mc du output shape varies).
size=$("${MC[@]}" du "$ALIAS/$BUCKET/datasets/" 2>/dev/null | awk '{print $1, $2}' | head -1)
[ -n "$size" ] && info "s3://$BUCKET/datasets/ total: $size"

cat <<EOF

Next: point the mesh at MinIO before mesh-up:

  export HITORRO_STORAGE_S3_ENDPOINT="$ENDPOINT"
  export HITORRO_STORAGE_S3_BUCKET="$BUCKET"
  export HITORRO_STORAGE_S3_ACCESS_KEY="$USER"
  export HITORRO_STORAGE_S3_SECRET_KEY="$PASS"
  # Tell mesh-init-data.sh to emit s3:// URIs in the agent config
  # instead of file: URIs (agent reads NDJson from MinIO):
  export HITORRO_DATASETS_S3_ENDPOINT="$ENDPOINT"

  cd hitorro-mesh-examples/scripts && ./mesh-down.sh && ./mesh-up.sh
EOF
