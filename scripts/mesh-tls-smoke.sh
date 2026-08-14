#!/usr/bin/env bash
# End-to-end verification of the phase-7a NATS TLS + auth wiring.
#
# Generates a self-signed CA + server cert, boots NATS with TLS enabled,
# launches the driver + 2 agents with nats-url=tls:// + nats-security
# pointing at a PKCS12 truststore, runs one query, asserts the row count,
# and tears everything down. Non-zero exit on failure — safe to hook into CI.
#
# Uses its OWN work directory ($MESH_TLS_WORK) and ports (offset from the
# plain-mesh defaults) so it can run alongside the regular mesh-smoke.sh
# without collisions.
#
# Requires:
#   - openssl + keytool on $PATH
#   - nats-server on $PATH (Orion ships one at ~/.orion/bin/nats-server)
#   - the two fat JARs built (mvn install -DskipTests on both apps)
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

# TLS-specific overrides (own work dir + own ports).
: "${MESH_TLS_WORK:=/tmp/hitorro-mesh-tls-smoke}"
: "${MESH_TLS_NATS_PORT:=4322}"
: "${MESH_TLS_DRIVER_PORT:=8185}"
: "${MESH_TLS_AGENT_US_PORT:=8191}"
: "${MESH_TLS_AGENT_EU_PORT:=8192}"
: "${MESH_TLS_STORE_PASSWORD:=changeit}"

MESH_LOGS="$MESH_TLS_WORK/logs"
MESH_PIDS="$MESH_TLS_WORK/pids"
CERTS="$MESH_TLS_WORK/certs"
CONFIG="$MESH_TLS_WORK/config"
DATA="$MESH_TLS_WORK/data"
TYPES="$MESH_TLS_WORK/types"

command -v openssl >/dev/null || _mesh_die "openssl not on PATH"
command -v keytool >/dev/null || _mesh_die "keytool not on PATH (install a JDK)"
[ -n "$MESH_NATS_BIN" ] || _mesh_die "nats-server not on PATH"
[ -f "$MESH_DRIVER_JAR" ] || _mesh_die "driver jar missing: $MESH_DRIVER_JAR"
[ -f "$MESH_AGENT_JAR" ]  || _mesh_die "agent jar missing:  $MESH_AGENT_JAR"

mkdir -p "$MESH_LOGS" "$MESH_PIDS" "$CERTS" "$CONFIG" "$DATA" "$TYPES"

# ---------- teardown handler (runs on exit, success or failure) ----------
cleanup() {
    for pidfile in "$MESH_PIDS"/*.pid; do
        [ -f "$pidfile" ] || continue
        local pid; pid=$(cat "$pidfile") || continue
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
        rm -f "$pidfile"
    done
}
trap cleanup EXIT

# ---------- 1. generate CA + server cert ----------
echo "[cert] generating self-signed CA + server cert under $CERTS"
if [ ! -f "$CERTS/ca.pem" ]; then
    openssl genrsa -out "$CERTS/ca-key.pem" 2048 2>/dev/null
    openssl req -new -x509 -key "$CERTS/ca-key.pem" \
                -out "$CERTS/ca.pem" -days 3650 \
                -subj "/CN=hitorro-mesh-tls-smoke-ca" 2>/dev/null
fi
if [ ! -f "$CERTS/server.pem" ]; then
    openssl genrsa -out "$CERTS/server-key.pem" 2048 2>/dev/null
    openssl req -new -key "$CERTS/server-key.pem" \
                -out "$CERTS/server.csr" \
                -subj "/CN=localhost" 2>/dev/null
    cat > "$CERTS/server-san.cnf" <<EOF
[req_ext]
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF
    openssl x509 -req -in "$CERTS/server.csr" \
                 -CA "$CERTS/ca.pem" -CAkey "$CERTS/ca-key.pem" -CAcreateserial \
                 -out "$CERTS/server.pem" -days 3650 \
                 -extfile "$CERTS/server-san.cnf" -extensions req_ext 2>/dev/null
fi
# PKCS12 truststore for the Java clients (jnats sslContext expects one of these).
if [ ! -f "$CERTS/truststore.p12" ]; then
    keytool -import -alias hitorro-mesh-ca -file "$CERTS/ca.pem" \
            -keystore "$CERTS/truststore.p12" -storetype PKCS12 \
            -storepass "$MESH_TLS_STORE_PASSWORD" -noprompt >/dev/null
fi

# ---------- 2. NATS server config (TLS) ----------
cat > "$CONFIG/nats-tls.conf" <<EOF
port: $MESH_TLS_NATS_PORT
tls {
    cert_file: "$CERTS/server.pem"
    key_file:  "$CERTS/server-key.pem"
}
EOF

# ---------- 3. data + type (identical to mesh-init-data.sh) ----------
cat > "$TYPES/docs.json" <<'JSON'
{"name": "docs","fields": [
  {"name": "id", "type": "core_string"},
  {"name": "title", "type": "core_string"},
  {"name": "lang", "type": "core_string"},
  {"name": "size_kb", "type": "core_long"}]}
JSON
cat > "$DATA/us.ndjson" <<'JSON'
{"id":"us-1","title":"Quarterly Report","lang":"en","size_kb":210}
{"id":"us-2","title":"Roadmap Q4","lang":"en","size_kb":84}
{"id":"us-3","title":"Hiring Plan","lang":"en","size_kb":640}
JSON
cat > "$DATA/eu.ndjson" <<'JSON'
{"id":"eu-1","title":"Rapport Annuel","lang":"fr","size_kb":512}
{"id":"eu-2","title":"Marktbericht","lang":"de","size_kb":128}
{"id":"eu-3","title":"Product Overview","lang":"en","size_kb":45}
JSON

# ---------- 4. driver + agent configs (with nats-security block) ----------
# tls:// URL auto-enables TLS in jnats; nats-security.trust-store-path
# points the Java client at our self-signed CA. Without it, the connection
# would fail with "unable to find valid certification path".
cat > "$CONFIG/driver.yml" <<EOF
server: { port: $MESH_TLS_DRIVER_PORT }
spring: { main: { banner-mode: off } }
management:
  endpoints:
    web:
      exposure: { include: health,info,metrics,prometheus }
hitorro:
  mesh:
    driver:
      transport: nats
      nats-url: tls://localhost:$MESH_TLS_NATS_PORT
      nats-security:
        tls: true
        trust-store-path: $CERTS/truststore.p12
        trust-store-password: $MESH_TLS_STORE_PASSWORD
      agent-expiry: 15s
      tables:
        - name: docs
          type-json-resource: file:$TYPES/docs.json
          partitions:
            - { key: us, required-capabilities: [jvssql, "partition:docs:us"] }
            - { key: eu, required-capabilities: [jvssql, "partition:docs:eu"] }
EOF

for who in us eu; do
    upper=$(echo "$who" | tr '[:lower:]' '[:upper:]')
    portvar="MESH_TLS_AGENT_${upper}_PORT"
    port=${!portvar}
    cat > "$CONFIG/agent-$who.yml" <<EOF
server: { port: $port }
spring: { main: { banner-mode: off } }
hitorro:
  mesh:
    agent:
      id: agent-$who
      transport: nats
      nats-url: tls://localhost:$MESH_TLS_NATS_PORT
      nats-security:
        tls: true
        trust-store-path: $CERTS/truststore.p12
        trust-store-password: $MESH_TLS_STORE_PASSWORD
      heartbeat-interval: 500ms
      capabilities: [jvssql, "partition:docs:$who"]
      tables:
        - name: docs
          partition-key: $who
          type-json-resource: file:$TYPES/docs.json
          ndjson-file: file:$DATA/$who.ndjson
EOF
done

# ---------- 5. launch ----------
_launch() {
    local name=$1; shift
    "$@" > "$MESH_LOGS/$name.log" 2>&1 &
    echo $! > "$MESH_PIDS/$name.pid"
    echo "[boot] $name pid=$(cat "$MESH_PIDS/$name.pid")  log=$MESH_LOGS/$name.log"
}
_wait_tcp() {
    local host=$1 port=$2 name=$3 timeout=${4:-30}
    local deadline=$(( $(date +%s) + timeout ))
    until nc -z "$host" "$port" 2>/dev/null; do
        [ "$(date +%s)" -ge "$deadline" ] && _mesh_die "$name did not open $host:$port within ${timeout}s — see $MESH_LOGS/$name.log"
        sleep 0.3
    done
    echo "[boot] $name up on $host:$port"
}

_launch nats "$MESH_NATS_BIN" -c "$CONFIG/nats-tls.conf"
_wait_tcp localhost "$MESH_TLS_NATS_PORT" nats

_launch driver java -jar "$MESH_DRIVER_JAR" --spring.config.location=file:"$CONFIG/driver.yml"
_wait_tcp localhost "$MESH_TLS_DRIVER_PORT" driver 60

_launch agent-us java -jar "$MESH_AGENT_JAR" --spring.config.location=file:"$CONFIG/agent-us.yml"
_launch agent-eu java -jar "$MESH_AGENT_JAR" --spring.config.location=file:"$CONFIG/agent-eu.yml"

# ---------- 6. verify ----------
echo "[wait] both agents to register"
deadline=$(( $(date +%s) + 20 ))
while :; do
    count=$(curl -s "http://localhost:$MESH_TLS_DRIVER_PORT/mesh/agents" | grep -o '"agentId"' | wc -l | tr -d ' ' || echo 0)
    [ "$count" = "2" ] && break
    [ "$(date +%s)" -ge "$deadline" ] && _mesh_die "only $count/2 agents registered (TLS handshake failure? check $MESH_LOGS/agent-us.log, $MESH_LOGS/agent-eu.log)"
    sleep 0.5
done
echo "[ok] both agents registered — TLS handshake succeeded end-to-end"

echo "[query] SELECT id, title FROM docs WHERE lang='en'"
resp=$(curl -sS -X POST "http://localhost:$MESH_TLS_DRIVER_PORT/mesh/queries" \
             -H 'Content-Type: application/json' \
             -d '{"sql":"SELECT id, title FROM docs WHERE lang = '"'"'en'"'"'","timeoutMs":5000}')
echo "$resp"
rowCount=$(echo "$resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["rowCount"])')
[ "$rowCount" = "4" ] || _mesh_die "expected 4 rows, got $rowCount"
echo "[ok] query returned $rowCount rows across TLS-secured shuffle"

# Sanity check: startup log line proves the auth mode was resolved.
if ! grep -q "tls=true" "$MESH_LOGS/driver.log"; then
    _mesh_die "driver did not log tls=true — the security config was not picked up"
fi
echo "[ok] driver log confirms tls=true"

echo
echo "TLS SMOKE TEST PASSED"
