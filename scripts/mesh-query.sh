#!/usr/bin/env bash
# Submit a SQL query to the driver's REST endpoint and pretty-print the response.
#
# Usage:
#   ./mesh-query.sh "SELECT id, title FROM docs WHERE lang = 'en'"
#   ./mesh-query.sh "SELECT id, title FROM docs WHERE lang = 'en'" 10000    # 10s timeout
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

[ $# -ge 1 ] || { echo "usage: $0 <sql> [timeout-ms]"; exit 2; }

sql=$1
timeout_ms=${2:-5000}

# json-safe encode the sql
body=$(python3 -c 'import json, sys; print(json.dumps({"sql": sys.argv[1], "timeoutMs": int(sys.argv[2])}))' "$sql" "$timeout_ms")

echo "→ POST /mesh/queries  timeoutMs=$timeout_ms"
echo "  sql: $sql"
echo

resp=$(curl -sS -X POST "http://localhost:$MESH_DRIVER_PORT/mesh/queries" \
              -H 'Content-Type: application/json' -d "$body")

if command -v jq >/dev/null; then
    echo "$resp" | jq .
else
    echo "$resp"
fi
