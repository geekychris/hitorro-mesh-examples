#!/usr/bin/env bash
# Boot local MinIO (S3-compatible object store) for HiTorro mesh dev.
#
# Prereqs:  docker + docker-compose (or `docker compose` plugin)
#
# After boot:
#   S3 API  : http://localhost:9000
#   Console : http://localhost:9001   (root user: hitorro / hitorro-dev-only)
#   Bucket  : hitorro
#
# Env overrides (all optional):
#   HITORRO_MINIO_S3_PORT           default 9000
#   HITORRO_MINIO_CONSOLE_PORT      default 9001
#   HITORRO_MINIO_ROOT_USER         default hitorro
#   HITORRO_MINIO_ROOT_PASSWORD     default hitorro-dev-only
#   HITORRO_MINIO_BUCKET            default hitorro
#
# Reachable from the mesh driver + agents in local dev via the same
# ports. In K8s use the bundled subchart at hitorro-mesh-k8s/helm/ instead.
set -euo pipefail
cd "$(dirname "$0")"

compose() {
    if command -v docker-compose >/dev/null 2>&1; then docker-compose "$@";
    else docker compose "$@"; fi
}

info() { printf "\033[1;34m→\033[0m %s\n" "$*"; }
ok()   { printf "\033[1;32m✓\033[0m %s\n" "$*"; }

info "starting MinIO + bucket-init"
compose up -d minio bucket-init

# Wait for the bootstrap container to exit so we know the bucket is ready.
info "waiting for bucket bootstrap …"
until [ "$(docker inspect --format '{{.State.Status}}' hitorro-minio-init 2>/dev/null)" = "exited" ]; do sleep 1; done

exit_code=$(docker inspect --format '{{.State.ExitCode}}' hitorro-minio-init 2>/dev/null)
if [ "$exit_code" != "0" ]; then
    echo "bucket bootstrap failed — logs:" >&2
    docker logs hitorro-minio-init >&2
    exit 1
fi

USER="${HITORRO_MINIO_ROOT_USER:-hitorro}"
PASS="${HITORRO_MINIO_ROOT_PASSWORD:-hitorro-dev-only}"
BUCKET="${HITORRO_MINIO_BUCKET:-hitorro}"
S3="${HITORRO_MINIO_S3_PORT:-9000}"
CONSOLE="${HITORRO_MINIO_CONSOLE_PORT:-9001}"

ok "MinIO up"
cat <<EOF

  S3 API   : http://localhost:$S3
  Console  : http://localhost:$CONSOLE   (login: $USER / $PASS)
  Bucket   : $BUCKET

  Set these when starting the mesh so basefile registers the MinIO adapter:

    export HITORRO_STORAGE_S3_ENDPOINT="http://localhost:$S3"
    export HITORRO_STORAGE_S3_BUCKET="$BUCKET"
    export HITORRO_STORAGE_S3_ACCESS_KEY="$USER"
    export HITORRO_STORAGE_S3_SECRET_KEY="$PASS"

  Next: copy the installed datasets over:
    ./minio-sync-datasets.sh
EOF
