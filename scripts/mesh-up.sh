#!/usr/bin/env bash
# Boot the whole local mesh: nats-server + driver + 2 agents.
# Requires:
#   - nats-server on $PATH (Orion ships one at ~/.orion/bin/nats-server)
#   - the two fat JARs built (mvn -pl hitorro-mesh-driver-app,hitorro-mesh-agent-app install -DskipTests)
#   - ./mesh-init-data.sh has been run at least once (creates $MESH_WORK)
#
# Everything runs in the background; PIDs live under $MESH_PIDS, logs under $MESH_LOGS.
# Stop with ./mesh-down.sh.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

[ -n "$MESH_NATS_BIN" ] || _mesh_die "nats-server not on PATH (Orion ships one at ~/.orion/bin/nats-server; try: export PATH=\$HOME/.orion/bin:\$PATH)"
[ -f "$MESH_DRIVER_JAR" ] || _mesh_die "driver jar missing: $MESH_DRIVER_JAR — run: (cd $MESH_ROOT/hitorro-mesh-driver-app && mvn install -DskipTests)"
[ -f "$MESH_AGENT_JAR" ]  || _mesh_die "agent jar missing:  $MESH_AGENT_JAR  — run: (cd $MESH_ROOT/hitorro-mesh-agent-app && mvn install -DskipTests)"
[ -f "$MESH_WORK/config/driver.yml" ] || _mesh_die "run ./mesh-init-data.sh first"

_launch() {
    local name=$1; shift
    local pidfile="$MESH_PIDS/$name.pid"
    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "$name already running (pid $(cat "$pidfile"))"
        return 0
    fi
    "$@" > "$MESH_LOGS/$name.log" 2>&1 &
    echo $! > "$pidfile"
    echo "started $name (pid $(cat "$pidfile"))  logs: $MESH_LOGS/$name.log"
}

_wait_tcp() {
    local host=$1 port=$2 name=$3 timeout=${4:-30}
    local deadline=$(( $(date +%s) + timeout ))
    until nc -z "$host" "$port" 2>/dev/null; do
        if [ "$(date +%s)" -ge "$deadline" ]; then
            _mesh_die "$name did not open $host:$port within ${timeout}s — check $MESH_LOGS/$name.log"
        fi
        sleep 0.3
    done
    echo "$name up on $host:$port"
}

# 1. NATS
_launch nats "$MESH_NATS_BIN" -p "$MESH_NATS_PORT"
_wait_tcp localhost "$MESH_NATS_PORT" nats

# 2. Driver
_launch driver java -jar "$MESH_DRIVER_JAR" --spring.config.location=file:"$MESH_WORK/config/driver.yml"
_wait_tcp localhost "$MESH_DRIVER_PORT" driver 60

# 3. Agents (in parallel)
_launch agent-us java -jar "$MESH_AGENT_JAR" --spring.config.location=file:"$MESH_WORK/config/agent-us.yml"
_launch agent-eu java -jar "$MESH_AGENT_JAR" --spring.config.location=file:"$MESH_WORK/config/agent-eu.yml"

echo
echo "mesh is up. try:"
echo "  ./mesh-status.sh"
echo "  ./mesh-query.sh \"SELECT id, title FROM docs WHERE lang = 'en'\""
echo "  ./mesh-down.sh"
