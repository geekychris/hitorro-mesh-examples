# Mesh HTTP recipes (copy-paste + IntelliJ-runnable)

Every recipe in two flavors on the same line: a `bash`+`curl` block
you can copy-paste into any terminal, and an `http` block IntelliJ's
HTTP Client renders with a green run-arrow.

## Prereqs — start the mesh once

```bash
cd hitorro-mesh-examples/scripts
./mesh-init-data.sh
./mesh-up.sh
```

Tear down when done:

```bash
./mesh-down.sh
```

All examples below assume the driver is at `http://localhost:8085`.

---

## 1 — Cluster inspection

### Live agents

```bash
curl -s http://localhost:8085/mesh/agents | jq
```

```http
GET http://localhost:8085/mesh/agents
```

### Registered tables

```bash
curl -s http://localhost:8085/mesh/tables | jq
```

```http
GET http://localhost:8085/mesh/tables
```

### Agent status (HEALTHY / MISSING / ORPHAN)

```bash
curl -s http://localhost:8085/mesh/cluster | jq
```

```http
GET http://localhost:8085/mesh/cluster
```

### Composite health (includes MeshHealthIndicator)

```bash
curl -s http://localhost:8085/actuator/health | jq
```

```http
GET http://localhost:8085/actuator/health
```

### Prometheus metrics

```bash
curl -s http://localhost:8085/actuator/prometheus | grep '^mesh_'
```

```http
GET http://localhost:8085/actuator/prometheus
```

---

## 2 — Basic batch queries

### Filter + projection

```bash
curl -sS -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT id, title FROM docs WHERE lang = '\''en'\''","timeoutMs":5000}' | jq
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT id, title FROM docs WHERE lang = 'en'",
  "timeoutMs": 5000
}
```

### GROUP BY + COUNT

```bash
curl -sS -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT lang, COUNT(*) AS n FROM docs GROUP BY lang","timeoutMs":5000}' | jq
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT lang, COUNT(*) AS n FROM docs GROUP BY lang",
  "timeoutMs": 5000
}
```

### AVG (decomposed to SUM+COUNT partials internally)

```bash
curl -sS -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT lang, AVG(size_kb) AS avg_size FROM docs GROUP BY lang","timeoutMs":5000}' | jq
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT lang, AVG(size_kb) AS avg_size FROM docs GROUP BY lang",
  "timeoutMs": 5000
}
```

### DISTINCT (auto-rewritten to GROUP BY)

```bash
curl -sS -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT DISTINCT lang FROM docs","timeoutMs":5000}' | jq
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT DISTINCT lang FROM docs",
  "timeoutMs": 5000
}
```

### ORDER BY + LIMIT (LIMIT pushed down; N-way merge sort)

```bash
curl -sS -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT id, size_kb FROM docs ORDER BY size_kb DESC LIMIT 5","timeoutMs":5000}' | jq
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT id, size_kb FROM docs ORDER BY size_kb DESC LIMIT 5",
  "timeoutMs": 5000
}
```

---

## 3 — Query planning + debugging

### EXPLAIN a simple query

```bash
curl -sG http://localhost:8085/mesh/queries/explain \
     --data-urlencode "sql=SELECT id FROM docs WHERE lang='en'" | jq
```

```http
GET http://localhost:8085/mesh/queries/explain?sql=SELECT+id+FROM+docs+WHERE+lang%3D%27en%27
```

### EXPLAIN an aggregate — see the partial + combine SQL

```bash
curl -sG http://localhost:8085/mesh/queries/explain \
     --data-urlencode "sql=SELECT lang, COUNT(*) FROM docs GROUP BY lang" | jq
```

```http
GET http://localhost:8085/mesh/queries/explain?sql=SELECT+lang%2C+COUNT%28*%29+FROM+docs+GROUP+BY+lang
```

Response shows `planType: "TwoStagePlan"` + the derived partial and
combine SQL (`SELECT lang, COUNT(*) AS __c0__ ... GROUP BY lang` and
`SELECT lang, SUM(__c0__) AS c0 FROM __mesh_partial__ GROUP BY lang`).

---

## 4 — Query lifecycle

### Query with retry on transient failure

```bash
curl -sS -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT id FROM docs","timeoutMs":5000,"retries":2}' | jq
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT id FROM docs",
  "timeoutMs": 5000,
  "retries": 2
}
```

Response includes `attempts` showing how many were needed. Retries only
fire on `AgentTaskException`; timeouts and planner errors fail fast.

### Short deadline → `timedOut: true`

```bash
curl -sS -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT id FROM docs","timeoutMs":1}' | jq
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT id FROM docs",
  "timeoutMs": 1
}
```

### List in-flight queries

```bash
curl -s http://localhost:8085/mesh/queries | jq
```

```http
GET http://localhost:8085/mesh/queries
```

### Cancel a running query

```bash
curl -sS -X DELETE http://localhost:8085/mesh/queries/YOUR-QUERY-ID | jq
```

```http
DELETE http://localhost:8085/mesh/queries/YOUR-QUERY-ID
```

Get the queryId from a running SSE query's `opened` event, or from
`GET /mesh/queries`. Unknown queryId returns 400 (idempotent).

---

## 5 — Server-Sent Events streaming

### Row-at-a-time delivery

```bash
curl -N 'http://localhost:8085/mesh/queries/stream?sql=SELECT+id+FROM+docs+WHERE+lang=%27en%27'
```

```http
GET http://localhost:8085/mesh/queries/stream?sql=SELECT+id+FROM+docs+WHERE+lang%3D%27en%27
Accept: text/event-stream
```

Expected event sequence:

```
event: opened
data: {"queryId":"3f21a8bc","assignedAgents":["agent-us","agent-eu"]}

event: row
data: {"id":"us-1","title":"Quarterly Report"}

event: row
data: {"id":"us-2","title":"Roadmap Q4"}

... more rows ...

event: complete
data: {"queryId":"3f21a8bc","rowCount":4}
```

---

## 6 — Structured error responses

Every typed error goes through `MeshExceptionHandler` and returns
consistent JSON `{error, message, queryId}`.

### Unregistered table → 400 Bad Request

```bash
curl -sSi -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT * FROM does_not_exist","timeoutMs":5000}'
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT * FROM does_not_exist",
  "timeoutMs": 5000
}
```

### Non-broadcast JOIN guard → 400 with actionable message

```bash
curl -sSi -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT d.id FROM docs d JOIN unknown x ON d.id = x.did","timeoutMs":5000}'
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT d.id FROM docs d JOIN unknown x ON d.id = x.did",
  "timeoutMs": 5000
}
```

Error body includes the currently-registered broadcast table names so
the user knows what's available.

---

## 7 — Broadcast JOIN (requires `langs` table)

Enable by adding to `driver.yml`:

```yaml
hitorro.mesh.driver.broadcast-tables: [langs]
```

And to every agent's config:

```yaml
hitorro.mesh.agent.broadcast-tables:
  - name: langs
    type-json-resource: file:/path/to/langs.json
    ndjson-file: file:/path/to/langs.ndjson
```

Then:

```bash
curl -sS -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT d.id, l.name FROM docs d JOIN langs l ON d.lang = l.code","timeoutMs":5000}' | jq
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT d.id, l.name FROM docs d JOIN langs l ON d.lang = l.code",
  "timeoutMs": 5000
}
```

---

## 8 — Shuffle-hash JOIN (requires two distributed tables)

Register `events` as a second distributed table on the driver, host
its partitions on their respective agents. Then:

### Inner join

```bash
curl -sS -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT d.id, e.action FROM docs d JOIN events e ON d.id = e.doc_id","timeoutMs":15000}' | jq
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT d.id, e.action FROM docs d JOIN events e ON d.id = e.doc_id",
  "timeoutMs": 15000
}
```

### With WHERE pushdown (per-side scans filtered before shuffle)

```bash
curl -sS -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT d.id, e.action FROM docs d JOIN events e ON d.id = e.doc_id WHERE d.lang = '\''en'\'' AND e.action = '\''view'\''","timeoutMs":15000}' | jq
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT d.id, e.action FROM docs d JOIN events e ON d.id = e.doc_id WHERE d.lang = 'en' AND e.action = 'view'",
  "timeoutMs": 15000
}
```

### With GROUP BY (3-stage combine)

```bash
curl -sS -X POST http://localhost:8085/mesh/queries \
     -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT d.lang, COUNT(*) AS interactions FROM docs d JOIN events e ON d.id = e.doc_id GROUP BY d.lang","timeoutMs":15000}' | jq
```

```http
POST http://localhost:8085/mesh/queries
Content-Type: application/json

{
  "sql": "SELECT d.lang, COUNT(*) AS interactions FROM docs d JOIN events e ON d.id = e.doc_id GROUP BY d.lang",
  "timeoutMs": 15000
}
```

---

## 9 — Streaming windowed aggregate

Requires a streaming source (Kafka / NATS JetStream / in-memory
streaming table). See phase 6d.1/6d.2 in
[`hitorro-mesh-core/ROADMAP.md`](https://github.com/geekychris/hitorro-mesh-core/blob/main/ROADMAP.md).

Long-lived query — per-window rows arrive as SSE events as watermarks
advance. Cancel via `DELETE /mesh/queries/{queryId}` when done.

```bash
curl -N 'http://localhost:8085/mesh/queries/stream?sql=SELECT+WIN_START(event_time,60000)+AS+ws,+COUNT(*)+AS+n+FROM+events+GROUP+BY+WIN_START(event_time,60000)&timeoutMs=3600000'
```

```http
GET http://localhost:8085/mesh/queries/stream?sql=SELECT+WIN_START%28event_time%2C+60000%29+AS+ws%2C+COUNT%28*%29+AS+n+FROM+events+GROUP+BY+WIN_START%28event_time%2C+60000%29&timeoutMs=3600000
Accept: text/event-stream
```

---

## More

- Full user guide (PDF + HTML with diagrams):
  [`hitorro-mesh-core/docs/user-guide/`](https://github.com/geekychris/hitorro-mesh-core/tree/main/docs/user-guide)
- Getting started tutorial:
  [`hitorro-mesh-core/GETTING_STARTED.md`](https://github.com/geekychris/hitorro-mesh-core/blob/main/GETTING_STARTED.md)
- Every shipped feature + design:
  [`hitorro-mesh-core/ROADMAP.md`](https://github.com/geekychris/hitorro-mesh-core/blob/main/ROADMAP.md)
