# Streaming SQL walkthrough

Hands-on: stand up a local Kafka broker, register its topic as a
mesh-queryable table, and live-tail matching rows over Server-Sent
Events. Runs entirely on your laptop; the only external dep is
Docker (or a local Kafka install).

Concept doc: [streaming-sql.md](../../hitorro-mesh-pipelines/docs/streaming-sql.md).

## 1. A one-node Kafka

If you don't already have one:

```bash
docker run -d --name redpanda \
    -p 9092:9092 -p 9644:9644 \
    docker.redpanda.com/redpandadata/redpanda:latest \
    redpanda start --smp 1 --overprovisioned --node-id 0 \
                   --kafka-addr PLAINTEXT://0.0.0.0:9092 \
                   --advertise-kafka-addr PLAINTEXT://localhost:9092
```

Redpanda is Kafka-protocol-compatible and boots in <5s. Regular
`bitnami/kafka` works identically — the mesh only cares about the
Kafka wire protocol on port 9092.

## 2. Boot the mesh

```bash
cd hitorro-mesh-examples/scripts
./mesh-init-data.sh    # first-time only
./mesh-up.sh
./mesh-status.sh       # confirm 2 agents + driver visible
```

## 3. Register the topic as a mesh table

The mesh needs to know the row schema. Write a minimal JVS type:

```bash
cat > /tmp/event-type.json <<'JSON'
{
  "name": "events",
  "core": [
    { "field": "id",      "type": "core_string" },
    { "field": "level",   "type": "core_string" },
    { "field": "message", "type": "core_string" },
    { "field": "ts",      "type": "core_long"   }
  ]
}
JSON
```

Register the topic as table `events`:

```bash
curl -X POST http://localhost:8085/mesh/queries/register-streaming \
     -H 'content-type: application/json' \
     -d "$(python3 -c "
import json
print(json.dumps({
  'name': 'events',
  'format': 'kafka',
  'typeJson': open('/tmp/event-type.json').read(),
  'sourceConfig': {
    'bootstrap-servers': 'localhost:9092',
    'group-id':          'mesh-events-live',
    'topic':             'events'
  }
}))")"
```

Response:

```json
{ "tableName":"events","format":"kafka","sourceConfig":{...},"agentsNotified":2 }
```

Every agent now has a live Kafka consumer for `events` under
consumer group `mesh-events-live`. Keep the group id stable — that's
the resume anchor if you restart the mesh.

## 4. Subscribe to a live query

Terminal A — live-tail every WARN:

```bash
./mesh-stream-query.sh "SELECT id, level, message FROM events WHERE level='WARN'"
```

Terminal B — produce a few messages into the topic:

```bash
docker exec -i redpanda rpk topic produce events <<'JSON'
{"id":"e-1","level":"INFO","message":"boot"}
{"id":"e-2","level":"WARN","message":"disk 92%"}
{"id":"e-3","level":"WARN","message":"queue backlog"}
{"id":"e-4","level":"INFO","message":"gc pause 12ms"}
JSON
```

Terminal A prints the two WARN rows as they arrive:

```
{"id":"e-2","level":"WARN","message":"disk 92%"}
{"id":"e-3","level":"WARN","message":"queue backlog"}
```

Ctrl-C to stop.

## 5. The same query in a browser

Open the driver UI at http://localhost:8085/ → **Streaming** tab.
Paste the same SQL. Rows scroll in as they arrive. Same underlying
`GET /mesh/queries/stream?sql=…` SSE endpoint — the UI is just an
`EventSource` reader.

## 6. Combined shape: pipeline + streaming SQL over the same topic

The clean pattern is one topic, two consumers with different group ids:

- **Pipeline** (durable): reads with group `kafka-events-to-index-consumer`,
  writes every message into a Lucene index. See
  [kafka-events-to-index.yaml](../../hitorro-mesh-pipelines/docs/examples/kafka-events-to-index.yaml).
- **Streaming SQL** (live view): reads with group `mesh-events-live`,
  serves the UI dashboard.

Kafka tracks offset per consumer group — the two consumers don't
compete. Each survives independently: pipeline resumes via
`restartable:true`; SSE clients reconnect from the current live
position.

## 7. What isn't shown here

- **Aggregates over a stream** (`SELECT level, COUNT(*) FROM events`) —
  works, but the counter resets when the stream closes. For durable
  rolling aggregates, run a pipeline that writes into a `kvstore` with
  a time-bucketed key.
- **JetStream durable pull** — the NATS equivalent of a Kafka consumer
  group. Same registration shape, `format: "nats"` + `stream` +
  `subject` + `durable`.

## Reference

- [streaming-sql.md](../../hitorro-mesh-pipelines/docs/streaming-sql.md) — full endpoint reference
- [authoring-a-pipeline.md](../../hitorro-mesh-pipelines/docs/authoring-a-pipeline.md) — pipeline vs streaming SQL decision matrix
- [pipeline-lifecycles.md](../../hitorro-mesh-pipelines/docs/pipeline-lifecycles.md) — resume semantics for continuous jobs
- [../../hitorro-mesh-pipelines/docs/examples/kafka-events-to-index.yaml](../../hitorro-mesh-pipelines/docs/examples/kafka-events-to-index.yaml) — companion pipeline
