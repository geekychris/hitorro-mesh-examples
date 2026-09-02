#!/usr/bin/env node
/*
 * CDK entry point. Instantiates the four stacks in dependency order:
 *   NetworkStack   → VPC + subnets + SGs
 *   StorageStack   → S3 blobs + EFS for stateful volumes
 *   RegistryStack  → ECR repos (one per service image)
 *   MeshStack      → NATS + driver + agents on ECS Fargate
 *   FleetStack     → hitorro-fleet-retrieval + hitorro-nosqldms
 *
 * Environment / knobs come from context (cdk.json or --context env=prod).
 */
import * as cdk from 'aws-cdk-lib';
import { NetworkStack }  from '../lib/network-stack';
import { StorageStack }  from '../lib/storage-stack';
import { RegistryStack } from '../lib/registry-stack';
import { MeshStack }     from '../lib/mesh-stack';
import { FleetStack }    from '../lib/fleet-stack';

const app = new cdk.App();

const env      = app.node.tryGetContext('env')      as string;
const region   = app.node.tryGetContext('region')   as string;
const account  = process.env.CDK_DEFAULT_ACCOUNT;
const imageTag = app.node.tryGetContext('imageTag') as string;

const cdkEnv = { account, region };
const tags   = { Project: 'hitorro-mesh', Env: env };

// Layered dependency: network → storage/registry → mesh → fleet
const network  = new NetworkStack(app, `mesh-${env}-network`, {
  env: cdkEnv, tags, singleAz: !!app.node.tryGetContext('singleAz'),
});

const storage  = new StorageStack(app, `mesh-${env}-storage`, {
  env: cdkEnv, tags, vpc: network.vpc,
});

const registry = new RegistryStack(app, `mesh-${env}-registry`, {
  env: cdkEnv, tags,
});

const mesh = new MeshStack(app, `mesh-${env}-mesh`, {
  env: cdkEnv, tags,
  vpc: network.vpc,
  cluster: network.cluster,
  albSg: network.albSg,
  serviceSg: network.serviceSg,
  efs: storage.stateFs,
  blobBucket: storage.blobBucket,
  driverRepo: registry.driverRepo,
  agentRepo:  registry.agentRepo,
  natsRepo:   registry.natsRepo,
  imageTag,
  agents:    Number(app.node.tryGetContext('meshAgents') ?? 2),
  driverMem: Number(app.node.tryGetContext('driverMem') ?? 1024),
  agentMem:  Number(app.node.tryGetContext('agentMem')  ?? 2048),
  natsMem:   Number(app.node.tryGetContext('natsMem')   ?? 512),
});

const fleet = new FleetStack(app, `mesh-${env}-fleet`, {
  env: cdkEnv, tags,
  vpc: network.vpc,
  cluster: network.cluster,
  albSg: network.albSg,
  serviceSg: network.serviceSg,
  efs: storage.stateFs,
  blobBucket: storage.blobBucket,
  retrievalRepo: registry.retrievalRepo,
  dmsRepo:       registry.dmsRepo,
  meshDriverAlbDns: mesh.driverAlbDns,
  imageTag,
  retrievalMem: Number(app.node.tryGetContext('retrievalMem') ?? 1024),
  dmsMem:       Number(app.node.tryGetContext('dmsMem')       ?? 1024),
  enableRetrieval: app.node.tryGetContext('enableRetrieval') !== false,
  enableDms:       app.node.tryGetContext('enableDms')       !== false,
});
fleet.addDependency(mesh);

app.synth();
