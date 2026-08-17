# Local MinIO for HiTorro mesh dev

S3-compatible object storage the mesh can read/write against
without touching AWS. Everything you need to run the mesh with
datasets stored in MinIO instead of on local disk.

## Prereqs

- `docker` + `docker compose` (or the standalone `docker-compose`)
- Optional: native `mc` CLI (`brew install minio-mc`) — the sync
  script falls back to a dockerized `mc` if it isn't on your PATH

## Quickstart

```bash
# 1. Boot MinIO (S3 API on :9000, console on :9001)
./minio-up.sh

# 2. Sync every installed dataset from ~/.hitorro/datasets to MinIO
./minio-sync-datasets.sh

# 3. Tell the mesh to route s3:// URIs through MinIO + agents to
#    load from S3. minio-up.sh printed these; copy-paste them.
export HITORRO_STORAGE_S3_ENDPOINT="http://localhost:9000"
export HITORRO_STORAGE_S3_BUCKET="hitorro"
export HITORRO_STORAGE_S3_ACCESS_KEY="hitorro"
export HITORRO_STORAGE_S3_SECRET_KEY="hitorro-dev-only"
export HITORRO_DATASETS_S3_ENDPOINT="http://localhost:9000"

# 4. Restart the mesh — agents now read NDJson from MinIO
cd ..
./mesh-down.sh && ./mesh-up.sh
```

Verify:

```bash
curl http://localhost:8085/mesh/storage | jq
```

Should show `s3Backend.reachable: true` and every dataset with
`local: true, s3: true`.

Or open the UI at http://localhost:8085/ui — the Cluster tab now
shows a **Storage layer** card with backend + per-dataset presence.

## Stopping

```bash
./minio-down.sh              # stops container, preserves data
./minio-down.sh --wipe       # also removes the data volume
```

## Env overrides

All optional; defaults in parentheses:

| Var | Default | Purpose |
|---|---|---|
| `HITORRO_MINIO_S3_PORT` | 9000 | S3 API port |
| `HITORRO_MINIO_CONSOLE_PORT` | 9001 | Console UI port |
| `HITORRO_MINIO_ROOT_USER` | hitorro | root user |
| `HITORRO_MINIO_ROOT_PASSWORD` | hitorro-dev-only | root password |
| `HITORRO_MINIO_BUCKET` | hitorro | default bucket |

## What's in the bucket after sync

```
s3://hitorro/datasets/
├── iso-currencies/
│   ├── manifest.yaml
│   ├── data/currencies.ndjson.bz2
│   └── types/iso_currencies.json
├── geonames-cities15000/
│   ├── manifest.yaml
│   ├── data/cities.ndjson.bz2
│   └── types/geonames_cities15000.json
└── ...
```

The mesh's `mesh-init-data.sh` reads these and emits
`s3://hitorro/datasets/<id>/...` URIs in each agent's config when
`HITORRO_DATASETS_S3_ENDPOINT` is set, so the agent JVM loads the
NDJson from MinIO via basefile's `MinioProtocolAdapter`.

## K8s deployment

The `hitorro-mesh-k8s/helm/hitorro-mesh` chart ships an optional
MinIO subchart. Enable via `values.yaml`:

```yaml
minio:
  enabled: true
  rootUser: my-user
  rootPassword: change-me     # OVERRIDE in prod
  bucket: hitorro
  storage:
    size: 100Gi
```

The chart deploys a single-replica StatefulSet + Service + credentials
Secret + a one-shot Job that creates the bucket on install. For
distributed MinIO (erasure-coded multi-node), use the official MinIO
operator instead of this subchart.
