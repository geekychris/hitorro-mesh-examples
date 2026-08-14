#!/usr/bin/env bash
# Quick health check: are processes alive, is the driver reachable, and
# which agents has the driver seen?
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

_check_pid() {
    local name=$1
    local pidfile="$MESH_PIDS/$name.pid"
    if [ ! -f "$pidfile" ]; then
        printf "  %-9s not started\n" "$name"
        return
    fi
    local pid; pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
        printf "  %-9s alive (pid %s)\n" "$name" "$pid"
    else
        printf "  %-9s DEAD (last pid %s — see %s)\n" "$name" "$pid" "$MESH_LOGS/$name.log"
    fi
}

echo "processes:"
for n in nats driver agent-us agent-eu; do _check_pid "$n"; done

echo
echo "driver actuator health:"
curl -s "http://localhost:$MESH_DRIVER_PORT/actuator/health" || echo "  (unreachable)"

echo
echo
echo "mesh agents (driver's live view):"
if command -v jq >/dev/null; then
    curl -s "http://localhost:$MESH_DRIVER_PORT/mesh/agents" | jq .
else
    curl -s "http://localhost:$MESH_DRIVER_PORT/mesh/agents"
    echo
fi

echo
echo "registered distributed tables:"
if command -v jq >/dev/null; then
    curl -s "http://localhost:$MESH_DRIVER_PORT/mesh/tables" | jq .
else
    curl -s "http://localhost:$MESH_DRIVER_PORT/mesh/tables"
    echo
fi
