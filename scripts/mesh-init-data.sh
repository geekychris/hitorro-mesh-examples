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

# ---- discover broadcast datasets installed under $HITORRO_DATASETS_HOME
# and emit a yaml block that every agent will pre-load. Each broadcast
# dataset ships one manifest with partitionBy: null; the layout on disk is:
#     $HITORRO_DATASETS_HOME/<id>/
#       manifest.yaml
#       data/<file>.ndjson
#       types/<snake_case_name>.json
# The driver's datasets autoconfig registers these with the driver's
# metadata; this loop wires the actual data at each agent side.
DATASETS_HOME="${HITORRO_DATASETS_HOME:-$HOME/.hitorro/datasets}"

# When HITORRO_DATASETS_S3_ENDPOINT is set, agents get s3:// URIs pointing
# at the MinIO / S3 bucket populated by minio-sync-datasets.sh — no local
# NDJson needed on the agent host. Both the URI scheme AND the S3
# adapter registration (via S3StorageAutoConfig on the agent JVM) must
# be in place for reads to succeed.
S3_ENDPOINT="${HITORRO_DATASETS_S3_ENDPOINT:-}"
S3_BUCKET="${HITORRO_DATASETS_S3_BUCKET:-${HITORRO_MINIO_BUCKET:-hitorro}}"
S3_PREFIX="${HITORRO_DATASETS_S3_PREFIX:-datasets}"

BROADCAST_BLOCK=""
if [[ -d "$DATASETS_HOME" ]]; then
    for dsdir in "$DATASETS_HOME"/*/; do
        [[ -d "$dsdir" ]] || continue
        id="$(basename "$dsdir")"
        table_name="${id//-/_}"
        type_file="$dsdir/types/${table_name}.json"
        # Match .ndjson OR .ndjson.bz2 / .ndjson.gz / .ndjson.zst — install
        # scripts can compress via HITORRO_DATASETS_COMPRESS (default: bz2).
        # Basefile in NdjsonLocalTable decodes by extension.
        # `find` avoids the shell glob failing with pipefail set when some
        # patterns don't match — `ls a*.ndjson b*.bz2` exits non-zero when
        # `a*.ndjson` matches nothing, killing the whole pipeline.
        ndjson="$(find "$dsdir/data/" -maxdepth 1 -type f \
                        \( -name "*.ndjson" -o -name "*.ndjson.bz2" \
                        -o -name "*.ndjson.gz" -o -name "*.ndjson.zst" \) \
                    2>/dev/null | head -1)"
        [[ -f "$type_file" && -f "$ndjson" ]] || continue

        # Rewrite absolute paths as s3:// URIs when S3_ENDPOINT is set.
        # Everything under $DATASETS_HOME lives at s3://$BUCKET/$PREFIX/<id>/…
        # after minio-sync-datasets.sh has run. Computed BEFORE the
        # broadcast/partitioned split so both branches see fresh URIs
        # for the current dataset (regression fix — before this, the
        # partitioned branch reused stale values from a prior iteration
        # and every partitioned table pointed at the same file).
        if [[ -n "$S3_ENDPOINT" ]]; then
            rel_type="${type_file#$DATASETS_HOME/}"
            rel_data="${ndjson#$DATASETS_HOME/}"
            type_uri="s3://$S3_BUCKET/$S3_PREFIX/$rel_type"
            data_uri="s3://$S3_BUCKET/$S3_PREFIX/$rel_data"
        else
            type_uri="file:$type_file"
            data_uri="file:$ndjson"
        fi

        # Only include broadcast datasets — manifest declares partitionBy: null.
        # The regex tolerates a trailing "# comment" on the same line, which
        # the shipped manifests use for row-count hints ("# broadcast — ~2k rows").
        if grep -qE '^partitionBy:[[:space:]]*(null|~)?[[:space:]]*(#.*)?$' "$dsdir/manifest.yaml"; then
            BROADCAST_BLOCK+="        - name: $table_name"$'\n'
            BROADCAST_BLOCK+="          type-json-resource: $type_uri"$'\n'
            BROADCAST_BLOCK+="          ndjson-file: $data_uri"$'\n'
        else
            # Partitioned dataset (partitionBy: <something>). The driver's
            # Autoregistrar registers these with a single partition named
            # "all" — every jvssql agent must hold the data and advertise
            # partition:<table>:all so SELECT * FROM works. Without this
            # branch, agents would say "does not hold partition all of
            # table X" the first time you queried it.
            PARTITIONED_ALL_BLOCK+="        - name: $table_name"$'\n'
            PARTITIONED_ALL_BLOCK+="          partition-key: all"$'\n'
            PARTITIONED_ALL_BLOCK+="          type-json-resource: $type_uri"$'\n'
            PARTITIONED_ALL_BLOCK+="          ndjson-file: $data_uri"$'\n'
            PARTITIONED_ALL_CAPS+="        - \"partition:$table_name:all\""$'\n'
        fi
    done
fi

# Newline-terminate PARTITIONED_ALL_BLOCK/CAPS so heredocs see YAML at column 1.
PARTITIONED_ALL_BLOCK="${PARTITIONED_ALL_BLOCK:-}"
PARTITIONED_ALL_CAPS="${PARTITIONED_ALL_CAPS:-}"

if [[ -n "$BROADCAST_BLOCK" ]]; then
    # Trailing newline before the closing YAML terminator so the heredoc
    # parser sees YAML at column 1 on its own line.
    BROADCAST_YAML="      broadcast-tables:"$'\n'"${BROADCAST_BLOCK%$'\n'}"$'\n'
    # ALSO expose each broadcast dataset as a partitioned table with
    # partition-key=broadcast. That's what the driver's dual registration
    # (see hitorro-mesh-datasets' Autoregistrar) dispatches against when
    # a user runs "SELECT * FROM wikidata_cities" — the FROM-must-be-
    # distributed constraint means we need a partitioned entry, and every
    # jvssql agent carries the same broadcast data so the partition key
    # is a constant "broadcast".
    PART_BLOCK=""
    while IFS= read -r line; do
        if [[ "$line" =~ ^[[:space:]]*-[[:space:]]name:[[:space:]](.*)$ ]]; then
            PART_BLOCK+="        - name: ${BASH_REMATCH[1]}"$'\n'
            PART_BLOCK+="          partition-key: broadcast"$'\n'
        elif [[ "$line" =~ ^[[:space:]]+(type-json-resource|ndjson-file):[[:space:]](.*)$ ]]; then
            PART_BLOCK+="          ${BASH_REMATCH[1]}: ${BASH_REMATCH[2]}"$'\n'
        fi
    done <<< "$BROADCAST_BLOCK"
    PARTITIONED_APPEND="${PART_BLOCK%$'\n'}"$'\n'
else
    BROADCAST_YAML=""
    PARTITIONED_APPEND=""
fi

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
        - pipeline-node
        - partition:docs:us
${PARTITIONED_ALL_CAPS}      tables:
        - name: docs
          partition-key: us
          type-json-resource: file:$MESH_WORK/types/docs.json
          ndjson-file: file:$MESH_WORK/data/us.ndjson
${PARTITIONED_APPEND}${PARTITIONED_ALL_BLOCK}${BROADCAST_YAML}    pipelines:
      enabled: true
      agent-id: agent-us
      nats-url: nats://localhost:$MESH_NATS_PORT
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
        - pipeline-node
        - partition:docs:eu
${PARTITIONED_ALL_CAPS}      tables:
        - name: docs
          partition-key: eu
          type-json-resource: file:$MESH_WORK/types/docs.json
          ndjson-file: file:$MESH_WORK/data/eu.ndjson
${PARTITIONED_APPEND}${PARTITIONED_ALL_BLOCK}${BROADCAST_YAML}    pipelines:
      enabled: true
      agent-id: agent-eu
      nats-url: nats://localhost:$MESH_NATS_PORT
YAML

echo "initialized $MESH_WORK:"
find "$MESH_WORK" -type f -not -path '*/logs/*' -not -path '*/pids/*' | sort
if [[ -n "$BROADCAST_BLOCK" ]]; then
    echo ""
    echo "broadcast datasets loaded on each agent:"
    echo "$BROADCAST_BLOCK" | grep -oE '\- name: \S+' | sed 's/- /  /'
fi
