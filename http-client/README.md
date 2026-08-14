# Mesh HTTP recipes (IntelliJ HTTP Client)

Runnable REST examples for the Hitorro Mesh driver. Two flavors:

- **`mesh-recipes.http`** — IntelliJ HTTP Client file. Green run-arrow
  in the gutter next to every `###` block. Response shows inline.
- **`mesh-recipes.md`** — same examples as a Markdown doc with copy-
  paste `bash` + `http` blocks. Also runnable in IntelliJ (the HTTP
  Client plugin recognizes ```http fenced blocks in `.md` files).

## Prereqs — start the mesh once

The mesh is a long-running service (driver + agents). Start it once,
then run any recipe repeatedly.

**Tier 2 — local NATS (recommended for trying recipes):**

```bash
cd ../scripts
./mesh-init-data.sh   # writes sample data + configs to /tmp/hitorro-mesh-smoke
./mesh-up.sh          # boots nats + driver + 2 agents in the background

# When done:
./mesh-down.sh
```

Requires `nats-server` on `$PATH` and the two fat JARs built (see the
scripts' own [README](../scripts/README.md)).

**Alternatives:**

- Tier 3 (Docker compose): `cd ../docker && docker compose up -d`
- Tier 4 (Kubernetes): see `hitorro-mesh-core/docs/user-guide/mesh-user-guide.pdf`

## Switching environments

`http-client.env.json` defines four environments. IntelliJ shows a
dropdown in the top-right of the HTTP editor:

- `local-nats` — `http://localhost:8085` (default for Tier 2)
- `docker-compose` — same host, kept separate for readability
- `kubernetes-port-forward` — after `kubectl port-forward svc/... 8085:8085`
- `kubernetes-ingress` — customize `driverHost` for your deploy

## What's in the recipes

Nine sections covering every REST endpoint:

1. Cluster inspection (`/mesh/agents`, `/mesh/tables`, `/mesh/cluster`,
   `/actuator/health`, `/actuator/prometheus`)
2. Basic batch queries (WHERE, GROUP BY, AVG, DISTINCT, HAVING,
   ORDER BY + LIMIT)
3. Query planning + debugging (`/mesh/queries/explain`)
4. Query lifecycle (retries, timeouts, list active, cancel)
5. SSE streaming
6. Structured error responses (bad SQL, unregistered table)
7. Broadcast JOIN (requires `langs` registered)
8. Shuffle-hash JOIN (requires two distributed tables)
9. Windowed streaming aggregate (requires a streaming source)

Sections 7-9 note the extra setup they need.

## Response handler scripts

IntelliJ HTTP Client lets you add `> {% ... %}` blocks after a
request. These run JavaScript against the response — used in the
recipes for `client.test(...)` assertions. Failures show in the
Run tool window with a red X.

## Not IntelliJ? Copy-paste from `mesh-recipes.md`

Every recipe is also in the `.md` file, with a `bash` block showing
the equivalent `curl` command. Works in any editor + shell.
