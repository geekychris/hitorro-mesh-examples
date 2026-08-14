# Tier-2: docker-compose mesh

Real network hops. Real NATS. 3 agents, each holding a different shard,
all in containers. No cluster manager involved — just Docker.

## Prereqs

- Docker (or Podman with `docker compose` alias)
- Fat JARs built:
  ```bash
  cd ../..           # repo root
  mvn -pl hitorro-mesh-driver-app,hitorro-mesh-agent-app install -DskipTests
  ```

## Bring up

```bash
cd hitorro-mesh-examples/docker
docker compose up -d --build
docker compose ps                 # driver + 3 agents should be Up
```

The first build takes a minute because Docker copies each ~900MB fat JAR
into an image layer; subsequent runs are cached.

## Query it

```bash
curl -s -X POST http://localhost:8085/mesh/queries \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT id, title, lang FROM docs WHERE lang='\''en'\''","timeoutMs":5000}' \
  | jq .
```

Should return 5 rows — 3 from `agent-us`, 1 from `agent-eu`, 1 from `agent-apac`.

## What's running

- `mesh-nats` (`nats:2.10-alpine`) — plain NATS on `4222`, monitoring on `8222`
- `mesh-driver` — fat JAR container, REST on host port `8085`
- `mesh-agent-us`, `mesh-agent-eu`, `mesh-agent-apac` — each holds one
  partition, mounted from `./data/*.ndjson`

Config for each container is bind-mounted from `./config/` so you can edit
`agent-us.yml` and `docker compose restart agent-us` without a rebuild.

## Inspecting

```bash
docker compose logs -f driver             # tail driver logs
docker compose logs agent-eu | tail -50   # last 50 lines from EU agent
docker exec -it mesh-nats sh              # NATS shell if you want to inspect
```

To watch messages flowing on NATS, run the `nats` CLI outside the compose:
```bash
docker run --rm -it --network hitorro-mesh_default natsio/nats-box:latest \
  nats -s nats://nats:4222 sub 'mesh.>'
```

## Tear down

```bash
docker compose down                  # stops and removes containers
docker compose down --rmi local      # also removes the built images
```

## Adding a new agent (say, an `sa` shard for South America)

1. Add `docker/data/sa.ndjson` with rows for that region.
2. Copy `config/agent-us.yml` → `config/agent-sa.yml`, change `id`,
   `partition-key`, `capabilities`, and `ndjson-file` to `sa`.
3. Add a partition entry to `config/driver.yml`.
4. Add an `agent-sa` service to `docker-compose.yml` mirroring the others.
5. `docker compose up -d --build agent-sa driver`.

## What this does NOT do

- **No cluster manager.** Docker Compose starts containers; that's it. See
  `hitorro-mesh-orion` and `hitorro-mesh-k8s` for capability-aware placement
  and multi-host scheduling.
- **No JetStream.** Plain NATS pub/sub — enough for phase 1. Phase 2
  (shuffle) will need `nats-server -js`; the compose file will grow a
  `-js` flag then.
- **No TLS / auth.** All containers on the same Docker bridge network,
  plain TCP.
