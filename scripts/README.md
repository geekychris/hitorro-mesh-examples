# Local mesh reproducer scripts

Boots a real Hitorro Mesh on your laptop: `nats-server` + one driver + two
agents, each in its own JVM. Uses the actual production fat JARs (same
ones you'd deploy to Orion or Kubernetes), just pointed at localhost.

## Where to start

If you've never written a mesh pipeline, read
[**authoring-a-pipeline.md**](../../hitorro-mesh-pipelines/docs/authoring-a-pipeline.md)
first — a hands-on walkthrough of source → steps → sinks and the
YAML/Groovy DSL. Then come back here to actually run one.

Other high-value guides:

- [pipeline-lifecycles.md](../../hitorro-mesh-pipelines/docs/pipeline-lifecycles.md) — batch vs continuous, Kafka/NATS resume model
- [scheduling.md](../../hitorro-mesh-pipelines/docs/scheduling.md) — cron/interval runs with durable checkpoints
- [streaming-sql.md](../../hitorro-mesh-pipelines/docs/streaming-sql.md) — ad-hoc live SQL over Kafka/NATS via SSE
- [MAIL_ENRICHMENT_TUTORIAL.md](../docs/MAIL_ENRICHMENT_TUTORIAL.md) — end-to-end: SQLite Mail → JVS enrich → Lucene + search
- [CLI_USERGUIDE.md](../docs/CLI_USERGUIDE.md) — full `mesh-pipeline.sh` reference

## Prereqs

1. **Java 21** on `$PATH`.
2. **`nats-server`** on `$PATH`. If you have Orion, it ships one at
   `~/.orion/bin/nats-server` — either `export PATH=$HOME/.orion/bin:$PATH`
   or set `MESH_NATS_BIN=$HOME/.orion/bin/nats-server` before running.
3. **Fat JARs built**:
   ```bash
   (cd hitorro-mesh-driver-app && mvn install -DskipTests)
   (cd hitorro-mesh-agent-app  && mvn install -DskipTests)
   ```

Optional but nice: `jq` and `nc` — the scripts fall back gracefully if
either is missing.

## Workflow

```bash
cd hitorro-mesh-examples/scripts

# 1) Create the sample dataset + Spring config under /tmp/hitorro-mesh-smoke
./mesh-init-data.sh

# 2) Boot nats + driver + 2 agents in the background
./mesh-up.sh

# 3) See what the driver thinks is live
./mesh-status.sh

# 4) Submit a query
./mesh-query.sh "SELECT id, title FROM docs WHERE lang = 'en'"

# 5) Tear it all down
./mesh-down.sh
```

Or all-in-one, self-asserting:
```bash
./mesh-smoke.sh    # inits, boots, queries, checks, tears down. Exit 0 = pass.
```

### The CLI wrappers at a glance

| script | purpose | docs |
|---|---|---|
| `mesh-pipeline.sh` | Run/list/cancel pipeline jobs, tail progress events, query registered tables | [CLI_USERGUIDE.md](../docs/CLI_USERGUIDE.md) |
| `mesh-schedule.sh` | Manage durable schedules (cron/interval + checkpoint) | [scheduling.md](../../hitorro-mesh-pipelines/docs/scheduling.md) |
| `mesh-stream-query.sh` | Subscribe to a streaming SQL query over SSE | [streaming-sql.md](../../hitorro-mesh-pipelines/docs/streaming-sql.md) |
| `mesh-query.sh` | Submit a one-shot batch SQL query | — |
| `mesh-status.sh` | Show live driver + agents + registered tables | — |

Example — run a bundled pipeline and watch its progress live:
```bash
./mesh-pipeline.sh run-bundled countries-triple-sink --wait --events
```

Example — schedule an hourly Mac Mail ingest with a persistent checkpoint:
```bash
./mesh-schedule.sh create mac-mail-hourly \
    ../../hitorro-mesh-pipelines/docs/examples/mac-mail-hourly-time.yaml \
    --cron '0 0 * * * *' --catchup-grace 60s
```

Example — live-tail matching rows from a Kafka topic (registered as `events`):
```bash
./mesh-stream-query.sh "SELECT id, level FROM events WHERE level='WARN'"
```

### TLS smoke test

```bash
./mesh-tls-smoke.sh    # generates self-signed CA, boots TLS-secured NATS,
                       # runs mesh with tls:// + nats-security truststore.
                       # Uses its own work dir + port range so it composes
                       # cleanly with mesh-smoke.sh above.
```
Verifies phase-7a end-to-end: real openssl-generated CA + server cert,
`nats-server` with `tls { cert_file ... key_file ... }`, driver + agents
connecting via `tls://` with a PKCS12 truststore pointing at the CA.
Requires `openssl` and `keytool` in addition to the usual prereqs.

### mTLS smoke test

```bash
./mesh-mtls-smoke.sh   # like mesh-tls-smoke.sh but with mutual TLS —
                       # NATS requires a client cert, mesh presents one
                       # from a PKCS12 keystore that chains to the same CA.
```
Verifies the `key-store-path` / `key-store-password` half of `nats-security`.
NATS config gets `verify: true` + `ca_file`; the mesh's Java client sends
a client cert during the TLS handshake, NATS validates it against the CA.
Same port-offset pattern as mesh-tls-smoke.sh — composes cleanly with the
plain and TLS variants.

## Where things live

Everything under `$MESH_WORK` (defaults to `/tmp/hitorro-mesh-smoke`):

```
/tmp/hitorro-mesh-smoke/
├── types/docs.json             # JVS type definition
├── data/us.ndjson              # partition data (3 English docs)
├── data/eu.ndjson              # partition data (1 en, 1 fr, 1 de)
├── config/driver.yml           # driver Spring config
├── config/agent-us.yml         # US agent Spring config
├── config/agent-eu.yml         # EU agent Spring config
├── logs/nats.log
├── logs/driver.log
├── logs/agent-us.log
├── logs/agent-eu.log
└── pids/*.pid                  # tracked by mesh-up.sh / mesh-down.sh
```

## Overriding defaults

Every knob is an env var — export before you source or run:

```bash
export MESH_WORK=$HOME/mesh-scratch
export MESH_NATS_PORT=14222
export MESH_DRIVER_PORT=18085
./mesh-init-data.sh && ./mesh-up.sh
```

Full list is in `env.sh`.

## Debugging

* Driver refuses your query with `400`? Read the `error` field — most
  common is the phase-1 guardrail on `GROUP BY` / `JOIN` / aggregates.
* No agents show up in `./mesh-status.sh`? Check `logs/agent-*.log` for a
  NATS connection failure or missing capability entry.
* Query hangs then times out? Usually means one partition has no live
  agent that advertises its `partition:docs:<key>` capability.
* Watch messages live:
  ```bash
  # (install `nats` cli or use nats-box container)
  nats sub 'mesh.>'
  ```

## What these scripts do NOT set up

* **JetStream** — phase 1 uses plain pub/sub. Phase 2 (shuffle) will
  require JetStream; `mesh-up.sh` will start `nats-server -js` at that point.
* **TLS / auth** — plain TCP to `localhost:4222`. Production deployments
  configure creds via `hitorro.mesh.driver.nats-url` and
  `hitorro.mesh.agent.nats-url`.
* **Kafka broker** — the mesh has first-class Kafka source + sink
  (`kind: kafka` in pipelines; `POST /mesh/queries/register-streaming`
  for live SQL) but you supply the broker. For local testing, Redpanda
  or a single-node Kafka in Docker works out of the box; point
  `bootstrap-servers` at it.
