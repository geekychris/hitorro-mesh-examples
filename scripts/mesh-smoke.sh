#!/usr/bin/env bash
# End-to-end smoke test: init data → boot mesh → wait for agents →
# run one WHERE query → assert row count → tear down.
# Non-zero exit on failure — safe to hook into CI.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

./mesh-init-data.sh > /dev/null
./mesh-up.sh

# wait for both agents to be registered (up to 15s)
deadline=$(( $(date +%s) + 15 ))
while :; do
    count=$(curl -s "http://localhost:$MESH_DRIVER_PORT/mesh/agents" | grep -o '"agentId"' | wc -l | tr -d ' ' || echo 0)
    [ "$count" = "2" ] && break
    [ "$(date +%s)" -ge "$deadline" ] && { echo "only $count/2 agents registered"; ./mesh-down.sh; exit 1; }
    sleep 0.5
done
echo "both agents registered"

# run the canonical WHERE query
resp=$(curl -sS -X POST "http://localhost:$MESH_DRIVER_PORT/mesh/queries" \
             -H 'Content-Type: application/json' \
             -d '{"sql":"SELECT id, title FROM docs WHERE lang = '"'"'en'"'"'","timeoutMs":5000}')
echo "$resp"

# assert 4 rows (us-1, us-2, us-3, eu-3)
rowCount=$(echo "$resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["rowCount"])')
if [ "$rowCount" != "4" ]; then
    echo "FAIL: expected 4 rows, got $rowCount"
    ./mesh-down.sh
    exit 1
fi

# phase 2: distributed GROUP BY + COUNT(*) — should combine across partitions
agg=$(curl -sS -X POST "http://localhost:$MESH_DRIVER_PORT/mesh/queries" \
            -H 'Content-Type: application/json' \
            -d '{"sql":"SELECT lang, COUNT(*) FROM docs GROUP BY lang","timeoutMs":5000}')
echo "$agg"
# us has 3 en, eu has 1 en + 1 fr + 1 de → globally: en=4, fr=1, de=1
en_count=$(echo "$agg" | python3 -c '
import json, sys
d = json.load(sys.stdin)
for r in d["rows"]:
    if r["lang"] == "en":
        print(r["c0"])
        break
')
if [ "$en_count" != "4" ]; then
    echo "FAIL: expected global en count=4, got $en_count"
    ./mesh-down.sh
    exit 1
fi

# JOIN against non-broadcast still rejected — sanity check the remaining guard
err=$(curl -sS -X POST "http://localhost:$MESH_DRIVER_PORT/mesh/queries" \
            -H 'Content-Type: application/json' \
            -d '{"sql":"SELECT * FROM docs JOIN x ON docs.id = x.id","timeoutMs":5000}')
if ! echo "$err" | grep -q "only registered broadcast tables can be joined"; then
    echo "FAIL: expected broadcast-only JOIN guard, got: $err"
    ./mesh-down.sh
    exit 1
fi

# phase 6c.1: SSE endpoint should stream rows as they arrive
sse=$(curl -sN --max-time 5 "http://localhost:$MESH_DRIVER_PORT/mesh/queries/stream?sql=SELECT%20id%20FROM%20docs%20WHERE%20lang%3D%27en%27" || true)
if ! echo "$sse" | grep -q "^event:opened$"; then
    echo "FAIL: expected SSE 'opened' event, got:"; echo "$sse"
    ./mesh-down.sh; exit 1
fi
if ! echo "$sse" | grep -q "^event:complete$"; then
    echo "FAIL: expected SSE 'complete' event, got:"; echo "$sse"
    ./mesh-down.sh; exit 1
fi
sse_rows=$(echo "$sse" | grep -c "^event:row$" || true)
if [ "$sse_rows" != "4" ]; then
    echo "FAIL: expected 4 SSE row events, got $sse_rows"
    ./mesh-down.sh; exit 1
fi
echo "SSE stream returned 4 rows + opened + complete events"

# Prometheus scrape — verify mesh_* meters land after we've run queries.
prom=$(curl -sS "http://localhost:$MESH_DRIVER_PORT/actuator/prometheus")
for m in mesh_queries_total mesh_tables_registered mesh_agents_live mesh_query_duration_seconds_count; do
    if ! echo "$prom" | grep -q "^$m"; then
        echo "FAIL: $m missing from /actuator/prometheus"
        ./mesh-down.sh; exit 1
    fi
done
echo "Prometheus scrape has mesh_queries_total / mesh_tables_registered / mesh_agents_live / mesh_query_duration_seconds"

./mesh-down.sh
echo
echo "SMOKE TEST PASSED"
