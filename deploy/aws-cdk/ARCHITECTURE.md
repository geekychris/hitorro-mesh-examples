# AWS deployment architecture

Full mapping between mesh runtime concepts and AWS services deployed
by this CDK.

- [1. Topology](#1-topology)
- [2. Service-to-AWS mapping](#2-service-to-aws-mapping)
- [3. Network layout](#3-network-layout)
- [4. State + storage](#4-state--storage)
- [5. IAM boundaries](#5-iam-boundaries)
- [6. Observability](#6-observability)
- [7. Failure modes + recovery](#7-failure-modes--recovery)
- [8. Cost model](#8-cost-model)
- [9. Deploy-sequence tradeoffs](#9-deploy-sequence-tradeoffs)

## 1. Topology

```mermaid
graph TB
  subgraph internet[Public Internet]
    users[Browsers / API clients]
  end
  subgraph aws[AWS account, one region]
    subgraph vpc[VPC 10.0.0.0/16]
      subgraph public[Public subnets<br/>with route to IGW]
        driverAlb["Driver ALB<br/>internet-facing"]
        fleetAlb["Fleet ALB<br/>internet-facing"]
        nat[NAT gateway]
      end
      subgraph priv[Private subnets<br/>egress via NAT only]
        subgraph cluster[ECS cluster<br/>CloudMap namespace mesh.local]
          nats["NATS Fargate<br/>nats.mesh.local"]
          driver["Driver Fargate<br/>driver.mesh.local"]
          agents["Agents Fargate Nx<br/>agent.mesh.local"]
          retrieval["Retrieval Fargate<br/>retrieval.mesh.local"]
          dms["DMS Fargate<br/>dms.mesh.local"]
        end
      end
    end
    subgraph regional[Regional services]
      efs["EFS<br/>per-service access points"]
      s3[S3 blob bucket]
      ecr[ECR - 5 repos]
      cw[CloudWatch Logs + Insights]
    end
  end
  users --> driverAlb
  users --> fleetAlb
  driverAlb -->|:8085| driver
  fleetAlb -->|:8095| retrieval
  fleetAlb -->|:8090| dms
  driver <-->|4222| nats
  agents <-->|4222| nats
  agents -->|EFS mount /agents| efs
  nats -->|EFS mount /nats| efs
  dms -->|EFS mount /dms| efs
  retrieval -->|EFS mount /retrieval| efs
  agents -->|s3 put/get| s3
  dms -->|s3 put/get| s3
  cluster -.->|pull images| ecr
  cluster -.->|logs, metrics| cw
```

## 2. Service-to-AWS mapping

Every hitorro concept has a specific AWS resource behind it:

| Mesh concept | AWS resource | Notes |
|---|---|---|
| **Mesh driver** — Spring Boot service that dispatches jobs, hosts pipeline REST + UI | ECS Fargate service (1 task) fronted by an internet-facing ALB | Stateless; scale horizontally by bumping `desiredCount` |
| **Mesh agent** — Spring Boot service that executes pipeline nodes, hosts KV state | ECS Fargate service (N tasks) — private subnets only | Stateful via EFS mount `/agents` (RocksDB + Lucene). Number set by `meshAgents` context |
| **NATS** — control-plane messaging + JetStream shuffle | ECS Fargate service (1 task), CloudMap DNS `nats.mesh.local` | JetStream state persisted to EFS `/nats`. HA cluster is a follow-up |
| **KV store** — RocksDB embedded in each agent | EFS access point `/agents`, mounted per-task | No standalone KV service — the "KV store" is the RocksDB files on EFS. Each agent gets its own subdir |
| **Lucene indexes** | EFS access points `/agents`, `/dms`, `/retrieval` | One volume, per-service subdir |
| **Content-addressed blobs** — DMS renditions, mail attachments, etc. | S3 bucket with Intelligent-Tiering lifecycle rule | `blob://sha256` URLs map to `s3://<bucket>/<sha256>` |
| **hitorro-fleet-retrieval** — search coordinator | ECS Fargate service, EFS mount `/retrieval` for Lucene | Behind fleet ALB at `/retrieval/*` |
| **hitorro-nosqldms** — DMS backend + React UI | ECS Fargate service, EFS mount `/dms` for JVS types + Lucene | Behind fleet ALB at `/` (catch-all) |
| **Container images** | 5 ECR repos: `hitorro-mesh-driver/agent/nats/retrieval/dms` | Tagged with git-sha + `latest` (or per-release) |
| **Service discovery** | AWS CloudMap namespace `mesh.local` | Every service resolves peers by DNS name — `nats.mesh.local`, `agent.mesh.local` (multi-A), etc. |
| **Logs** | CloudWatch Logs — 1 group per stack | 30-day retention by default |
| **Metrics** | CloudWatch Container Insights (auto-enabled on cluster) | CPU/mem per task, per service |
| **Secrets** (future — SMTP creds, Kafka creds) | AWS Secrets Manager | Referenced by env from ECS task defs |
| **TLS** (future) | ACM cert + ALB HTTPS listener on :443 | Not enabled by default — add a certificate + redirect from :80 |

### Where each service's memory sits

```mermaid
graph LR
  subgraph fargate[Fargate task]
    jvm[JVM heap]
    offheap[Off-heap / metaspace / thread stacks]
    rocks[RocksDB cache + memtables]
    lucene[Lucene search cache]
  end
  subgraph efs[EFS mount /data]
    rocksfiles[RocksDB SSTs]
    luceneindex[Lucene index segments]
  end
  jvm -->|hot data| rocks
  jvm -->|search results| lucene
  rocks -->|flush + compaction| rocksfiles
  lucene -->|commit| luceneindex
```

Right-size tasks: JVM heap should be ~50% of the task memory to leave
room for off-heap RocksDB/Lucene caches. The defaults follow this
rule (agent: 2048 MB task → 1500 MB heap via `-Xmx1500m`).

## 3. Network layout

```mermaid
graph TB
  subgraph vpc[VPC — 10.0.0.0/16]
    subgraph az1[AZ 1 - always present]
      pub1[Public 10.0.0.0/24<br/>ALBs + NAT]
      priv1[Private 10.0.4.0/22<br/>Fargate tasks]
    end
    subgraph az2[AZ 2 - only if singleAz false]
      pub2[Public 10.0.1.0/24<br/>ALBs + NAT]
      priv2[Private 10.0.8.0/22<br/>Fargate tasks]
    end
  end
  igw[Internet Gateway]
  igw --> pub1
  pub1 -->|NAT| priv1
  igw --> pub2
  pub2 -->|NAT| priv2
```

Security groups:

```mermaid
graph LR
  albSg[ALB SG<br/>ingress 0.0.0.0/0 :80]
  svcSg[Service SG<br/>ingress from ALB :8000-9000<br/>ingress from self any-port]
  efsSg[EFS SG - auto-created by construct<br/>ingress from ServiceSG :2049]
  users[Internet]
  users --> albSg
  albSg --> svcSg
  svcSg --> svcSg
  svcSg --> efsSg
```

**Everything on the app plane sits in private subnets.** ALBs are the
only public entry. Fargate tasks reach ECR + CloudWatch through the
NAT gateway. If you want no NAT (all-private VPC), add VPC endpoints
for ECR, CloudWatch Logs, S3, and Secrets Manager.

## 4. State + storage

### EFS layout

One `EFS.FileSystem` with per-service Access Points that chroot into
subdirs and run tasks as distinct POSIX uids so a compromised task
can't stomp another service's data.

```mermaid
graph LR
  efs["EFS root<br/>fs-abc123.efs.us-east-1.amazonaws.com"]
  natsAp["/nats<br/>uid 1001"]
  agentAp["/agents<br/>uid 1002"]
  retrievalAp["/retrieval<br/>uid 1003"]
  dmsAp["/dms<br/>uid 1004"]
  efs --> natsAp
  efs --> agentAp
  efs --> retrievalAp
  efs --> dmsAp
  natsSvc[NATS service] --> natsAp
  agentSvc[Agent service] --> agentAp
  retrievalSvc[Retrieval service] --> retrievalAp
  dmsSvc[DMS service] --> dmsAp
```

### S3 bucket layout

Flat namespace: `s3://<bucket>/<sha256>`. Content-addressed. No
directory hierarchy. Intelligent-Tiering after 30 days for cost.

```mermaid
graph LR
  dms[DMS] -->|put content_ref bytes| s3
  agents[Agents] -->|put pipeline output blobs| s3
  s3[S3 bucket]
  s3 -->|30d| it[Intelligent-Tiering]
```

## 5. IAM boundaries

Each service gets its own task role — least privilege:

```mermaid
graph LR
  subgraph roles[Task roles]
    natsR[NATS role<br/>no AWS API calls]
    driverR[Driver role<br/>no AWS API calls]
    agentR[Agent role<br/>S3 rw on blob bucket<br/>EFS mount]
    retrievalR[Retrieval role<br/>S3 read<br/>EFS mount]
    dmsR[DMS role<br/>S3 rw<br/>EFS mount]
  end
  subgraph aws[AWS resources]
    s3[Blob bucket]
    efs[EFS]
    cw[CloudWatch Logs]
    ecr[ECR pull]
  end
  natsR --> cw
  natsR --> efs
  driverR --> cw
  agentR --> cw
  agentR --> s3
  agentR --> efs
  retrievalR --> cw
  retrievalR --> s3
  retrievalR --> efs
  dmsR --> cw
  dmsR --> s3
  dmsR --> efs
  natsR -.->|via task-execution role| ecr
  driverR -.->|via task-execution role| ecr
  agentR -.->|via task-execution role| ecr
  retrievalR -.->|via task-execution role| ecr
  dmsR -.->|via task-execution role| ecr
```

`ecs-tasks.amazonaws.com` is the trust principal on every role.
CDK's `enableExecuteCommand: true` on each service adds the
`ssmmessages:*` policy needed for `aws ecs execute-command`.

## 6. Observability

```mermaid
graph LR
  subgraph tasks[Fargate tasks]
    stdout[stdout logs]
    metrics[CPU / mem / net metrics]
  end
  subgraph cw[CloudWatch]
    logs["Logs — one group per stack<br/>MeshLogs, FleetLogs"]
    insights["Container Insights — per-service<br/>CPU, mem, task count, restarts"]
    dash[Dashboards]
    alarms[Alarms]
  end
  stdout --> logs
  metrics --> insights
  logs --> dash
  insights --> dash
  insights --> alarms
```

Log groups:
- `/aws/ecs/mesh-<env>-mesh/MeshLogs` — NATS, driver, agents
- `/aws/ecs/mesh-<env>-fleet/FleetLogs` — retrieval, DMS

Container Insights is enabled by the network stack; it collects
per-task CPU/mem/network at 1-min resolution.

## 7. Failure modes + recovery

### Task crash
ECS restarts the task automatically. Rolling-update guarantees keep
service capacity at 100% during the restart.

### NAT gateway failure
Same-AZ tasks lose egress until AZ recovers. Fix: `singleAz: false`
gives you a NAT per AZ.

### AZ outage
`singleAz: true` — everything is down until AZ recovers.
`singleAz: false` — services survive but capacity halves; scale
`desiredCount` to compensate.

### EFS partial outage
EFS is multi-AZ automatically. A single mount-target failure just
redirects to another AZ.

### S3 outage
Full-region S3 outage is exceptionally rare; DMS + agents lose blob
writes until it clears. Documents remain readable from EFS-persisted
Lucene indexes.

### Corrupted Lucene index
Rebuild from the KV keyspaces via `hitorro-mesh-pipelines`' rebuild
job. Doc metadata is authoritative in KV; Lucene is a derived view.

### Recovery flow

```mermaid
sequenceDiagram
  autonumber
  participant Op as Operator
  participant CFN as CloudFormation
  participant ECS as ECS
  participant EFS
  Op->>CFN: cdk destroy mesh-dev-mesh
  CFN->>ECS: drain + stop services
  Note over EFS: EFS RemovalPolicy=RETAIN<br/>state survives
  Op->>CFN: cdk deploy mesh-dev-mesh
  CFN->>ECS: launch tasks
  ECS->>EFS: mount /agents, /nats
  Note over ECS: services rehydrate from EFS-persisted state<br/>zero data loss
```

## 8. Cost model

### Fixed costs (running 24/7 in `us-east-1`)

| Resource | ~$/mo |
|---|---|
| NAT gateway (1 in singleAz mode) | 32 |
| ALB (2 — driver + fleet) | 32 |
| EFS storage (first 100 GB burst) | 30 |
| CloudWatch Logs (~5 GB/mo ingestion + 30d retention) | 3 |
| **Fixed** | **~97** |

### Variable — per Fargate task

| Task | vCPU | Mem | ~$/mo |
|---|---|---|---|
| NATS | 0.25 | 512 MB | 9 |
| Driver | 0.5 | 1 GB | 18 |
| Agent (each) | 1.0 | 2 GB | 36 |
| Retrieval | 0.5 | 1 GB | 18 |
| DMS | 0.5 | 1 GB | 18 |

Default deployment (2 agents): ~$230/mo. Production preset (2 AZ, 4
agents, 2× memory): ~$500/mo.

### Cost knobs

```mermaid
graph LR
  cheap["Cheapest: singleAz=true<br/>meshAgents=1<br/>disable retrieval or DMS"]
  default["Default: singleAz=true<br/>meshAgents=2<br/>everything enabled"]
  prod["Prod: singleAz=false<br/>meshAgents=4<br/>2× memory<br/>ACM cert on ALB"]
  cheap -->|~$150 mo| box1[low env]
  default -->|~$230 mo| box2[dev / staging]
  prod -->|~$500-800 mo| box3[production]
```

## 9. Deploy-sequence tradeoffs

Why five stacks instead of one:

| Stack | Change frequency | Blast radius |
|---|---|---|
| Network | Low — VPC changes are risky | High — everything runs in it |
| Storage | Very low — data lives forever | Very high — deletion loses data |
| Registry | Never — image push updates content, not the resource | Low — deleting empties ECR |
| Mesh | High — new image tags, agent count | Medium — job execution stops |
| Fleet | High — new image tags | Low — retrieval + DMS |

Splitting lets you `cdk deploy` a new image tag on the mesh stack
without touching the VPC or S3 bucket — no accidental cascading
change to blast radius. And when you eventually want a
prod-vs-staging split, you deploy the whole set twice with different
`env=` context: `mesh-staging-*` and `mesh-prod-*` live side-by-side.
