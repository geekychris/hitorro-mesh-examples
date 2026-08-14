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

# Log dir + pid dir under the work dir
: "${MESH_LOGS:=$MESH_WORK/logs}"
: "${MESH_PIDS:=$MESH_WORK/pids}"
mkdir -p "$MESH_LOGS" "$MESH_PIDS"

export MESH_ROOT MESH_WORK MESH_NATS_PORT MESH_DRIVER_PORT \
       MESH_AGENT_US_PORT MESH_AGENT_EU_PORT \
       MESH_DRIVER_JAR MESH_AGENT_JAR MESH_NATS_BIN \
       MESH_LOGS MESH_PIDS

_mesh_die() { echo "error: $*" >&2; exit 1; }
