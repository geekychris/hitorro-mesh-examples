#!/usr/bin/env bash
# Stop local MinIO. `--wipe` also removes the data volume.
set -euo pipefail
cd "$(dirname "$0")"

compose() {
    if command -v docker-compose >/dev/null 2>&1; then docker-compose "$@";
    else docker compose "$@"; fi
}

WIPE=0
for a in "$@"; do
    case "$a" in
        --wipe) WIPE=1 ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \?//' ; exit 0 ;;
    esac
done

if [ $WIPE -eq 1 ]; then
    echo "→ stopping + removing MinIO container + data volume"
    compose down -v
else
    echo "→ stopping MinIO (data preserved; re-up will resume)"
    compose down
fi
echo "✓ done"
