#!/usr/bin/env bash
# Create the sample dataset + config files under $MESH_WORK.
# Idempotent — safe to re-run. Won't touch $MESH_LOGS or $MESH_PIDS.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

mkdir -p "$MESH_WORK/types" "$MESH_WORK/data" "$MESH_WORK/config"

# ---- type definition ----
cat > "$MESH_WORK/types/docs.json" <<'JSON'
{
  "name": "docs",
  "fields": [
    {"name": "id",      "type": "core_string"},
    {"name": "title",   "type": "core_string"},
    {"name": "lang",    "type": "core_string"},
    {"name": "size_kb", "type": "core_long"}
  ]
}
JSON

# ---- partition data (NDJSON — one JSON object per line) ----
cat > "$MESH_WORK/data/us.ndjson" <<'JSON'
{"id":"us-1","title":"Quarterly Report","lang":"en","size_kb":210}
{"id":"us-2","title":"Roadmap Q4","lang":"en","size_kb":84}
{"id":"us-3","title":"Hiring Plan","lang":"en","size_kb":640}
JSON

cat > "$MESH_WORK/data/eu.ndjson" <<'JSON'
{"id":"eu-1","title":"Rapport Annuel","lang":"fr","size_kb":512}
{"id":"eu-2","title":"Marktbericht","lang":"de","size_kb":128}
{"id":"eu-3","title":"Product Overview","lang":"en","size_kb":45}
JSON

# ---- driver config ----
cat > "$MESH_WORK/config/driver.yml" <<YAML
server:
  port: $MESH_DRIVER_PORT
spring:
  main:
    banner-mode: off
# Actuator surfaces — /actuator/health for liveness, /actuator/prometheus for scraping.
# --spring.config.location REPLACES the packaged application.yml, so this block has to
# live in every deployment-facing driver.yml (or use --spring.config.additional-location instead).
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
hitorro:
  mesh:
    driver:
      transport: nats
      nats-url: nats://localhost:$MESH_NATS_PORT
      agent-expiry: 15s
      tables:
        - name: docs
          type-json-resource: file:$MESH_WORK/types/docs.json
          partitions:
            - key: us
              required-capabilities: [jvssql, partition:docs:us]
            - key: eu
              required-capabilities: [jvssql, partition:docs:eu]
YAML

# ---- agent-us config ----
cat > "$MESH_WORK/config/agent-us.yml" <<YAML
server:
  port: $MESH_AGENT_US_PORT
spring:
  main:
    banner-mode: off
hitorro:
  mesh:
    agent:
      id: agent-us
      transport: nats
      nats-url: nats://localhost:$MESH_NATS_PORT
      heartbeat-interval: 500ms
      capabilities:
        - jvssql
        - partition:docs:us
      tables:
        - name: docs
          partition-key: us
          type-json-resource: file:$MESH_WORK/types/docs.json
          ndjson-file: file:$MESH_WORK/data/us.ndjson
YAML

# ---- agent-eu config ----
cat > "$MESH_WORK/config/agent-eu.yml" <<YAML
server:
  port: $MESH_AGENT_EU_PORT
spring:
  main:
    banner-mode: off
hitorro:
  mesh:
    agent:
      id: agent-eu
      transport: nats
      nats-url: nats://localhost:$MESH_NATS_PORT
      heartbeat-interval: 500ms
      capabilities:
        - jvssql
        - partition:docs:eu
      tables:
        - name: docs
          partition-key: eu
          type-json-resource: file:$MESH_WORK/types/docs.json
          ndjson-file: file:$MESH_WORK/data/eu.ndjson
YAML

echo "initialized $MESH_WORK:"
find "$MESH_WORK" -type f -not -path '*/logs/*' -not -path '*/pids/*' | sort
