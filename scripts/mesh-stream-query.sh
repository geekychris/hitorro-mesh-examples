#!/usr/bin/env bash
# Streaming SQL client — subscribes to /mesh/queries/stream (SSE) and
# prints one JSON-per-row to stdout. Ctrl-C to stop.
#
# Usage:
#   mesh-stream-query.sh "<SQL>"
#   mesh-stream-query.sh "<SQL>" --pretty
#
# Examples:
#   ./mesh-stream-query.sh "SELECT id, level FROM events WHERE level='WARN'"
#   ./mesh-stream-query.sh "SELECT * FROM events" --pretty
#
# The stream stays open until:
#   * the server sends `event: complete` (finite source drained), or
#   * the server sends `event: error` (query failed), or
#   * you Ctrl-C.
#
# For registering a Kafka/NATS topic as a queryable table first, see
# hitorro-mesh-pipelines/docs/streaming-sql.md.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

BASE_URL="http://localhost:${MESH_DRIVER_PORT}"
PRETTY=0

_die() { echo "$*" >&2; exit 2; }

# Parse flags — SQL is positional.
SQL=""
while [ $# -gt 0 ]; do
    case "$1" in
        --pretty) PRETTY=1 ;;
        -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
        *) SQL="$1" ;;
    esac
    shift
done
[ -n "$SQL" ] || _die "usage: $0 \"<SQL>\" [--pretty]"

# URL-encode the SQL. curl --data-urlencode works for POST bodies;
# for GET query params we need to encode by hand. python3 is on every
# macOS + Linux dev box.
ENCODED_SQL=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$SQL")

# curl -N disables output buffering — critical for SSE.
# curl --no-buffer is the same flag under a different name.
curl -N -sS "$BASE_URL/mesh/queries/stream?sql=$ENCODED_SQL" \
    | python3 -c "
import json, sys
pretty = '$PRETTY' == '1'
event = None
for line in sys.stdin:
    line = line.rstrip('\n')
    if line.startswith('event:'):
        event = line[6:].strip()
    elif line.startswith('data:'):
        payload = line[5:].strip()
        if event == 'opened':
            m = json.loads(payload)
            print(f\"[opened] queryId={m.get('queryId')} agents={m.get('assignedAgents')}\", file=sys.stderr)
        elif event == 'row':
            if pretty:
                print(json.dumps(json.loads(payload), indent=2))
            else:
                print(payload)
            sys.stdout.flush()
        elif event == 'complete':
            m = json.loads(payload)
            print(f\"[complete] rowCount={m.get('rowCount')}\", file=sys.stderr)
            sys.exit(0)
        elif event == 'error':
            m = json.loads(payload)
            print(f\"[error] {m.get('error')}\", file=sys.stderr)
            sys.exit(3)
    elif line == '':
        event = None
"
