#!/usr/bin/env bash
# Stop everything mesh-up.sh started. Idempotent.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

_stop() {
    local name=$1
    local pidfile="$MESH_PIDS/$name.pid"
    [ -f "$pidfile" ] || { echo "$name not running"; return; }
    local pid; pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" && echo "stopped $name (pid $pid)"
        # give it 3s to exit cleanly, then SIGKILL
        for _ in 1 2 3 4 5 6; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.5
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" && echo "  SIGKILL'd $name"
        fi
    else
        echo "$name pid $pid already gone"
    fi
    rm -f "$pidfile"
}

# reverse order of start
_stop agent-eu
_stop agent-us
_stop driver
_stop nats

echo "mesh is down. logs remain under $MESH_LOGS."
