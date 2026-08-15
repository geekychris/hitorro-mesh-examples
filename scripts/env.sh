# Shared environment for the mesh scripts. Source, don't execute.
# Override anything by exporting before you source this file.

: "${MESH_ROOT:=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
: "${MESH_WORK:=/tmp/hitorro-mesh-smoke}"
: "${MESH_NATS_PORT:=4222}"
: "${MESH_DRIVER_PORT:=8085}"
: "${MESH_AGENT_US_PORT:=8091}"
: "${MESH_AGENT_EU_PORT:=8092}"
: "${MESH_DRIVER_JAR:=$MESH_ROOT/hitorro-mesh-driver-app/target/hitorro-mesh-driver-app-3.0.1.jar}"
: "${MESH_AGENT_JAR:=$MESH_ROOT/hitorro-mesh-agent-app/target/hitorro-mesh-agent-app-3.0.1.jar}"
: "${MESH_NATS_BIN:=$(command -v nats-server || true)}"

# HT_BIN — hitorro config root (config/types + config/jsonconfigs). The
# JVS type system + LuceneFieldTypes resolve here at runtime. Default
# to $HOME/hitorro if it looks like a hitorro checkout; else fall back
# to $MESH_ROOT which is the mesh checkout parent.
if [ -z "${HT_BIN:-}" ]; then
    if [ -d "$HOME/hitorro/config/types" ]; then
        HT_BIN="$HOME/hitorro"
    else
        HT_BIN="$MESH_ROOT"
    fi
fi
export HT_BIN

# Base JDWP port for the mesh. Driver = MESH_JDWP_BASE + 0 (5085),
# agent-us = +6 (5091), agent-eu = +7 (5092). Attach IntelliJ Remote
# JVM Debug at localhost:${MESH_JDWP_BASE+offset}. Set MESH_JDWP_ENABLE=0
# to skip JDWP wiring entirely.
: "${MESH_JDWP_BASE:=5085}"
: "${MESH_JDWP_ENABLE:=1}"
: "${MESH_DRIVER_JDWP:=$MESH_JDWP_BASE}"
: "${MESH_AGENT_US_JDWP:=$((MESH_JDWP_BASE + 6))}"
: "${MESH_AGENT_EU_JDWP:=$((MESH_JDWP_BASE + 7))}"

# Log dir + pid dir under the work dir
: "${MESH_LOGS:=$MESH_WORK/logs}"
: "${MESH_PIDS:=$MESH_WORK/pids}"
mkdir -p "$MESH_LOGS" "$MESH_PIDS"

export MESH_ROOT MESH_WORK MESH_NATS_PORT MESH_DRIVER_PORT \
       MESH_AGENT_US_PORT MESH_AGENT_EU_PORT \
       MESH_DRIVER_JAR MESH_AGENT_JAR MESH_NATS_BIN \
       MESH_LOGS MESH_PIDS \
       MESH_JDWP_BASE MESH_JDWP_ENABLE \
       MESH_DRIVER_JDWP MESH_AGENT_US_JDWP MESH_AGENT_EU_JDWP

# Emits the JDWP -agentlib arg for a given port when MESH_JDWP_ENABLE=1,
# empty string otherwise. Quoted so caller can embed via $(_jdwp_arg …).
_jdwp_arg() {
    local port=$1
    [ "${MESH_JDWP_ENABLE:-0}" = "1" ] || return 0
    printf -- "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:%s" "$port"
}

_mesh_die() { echo "error: $*" >&2; exit 1; }
