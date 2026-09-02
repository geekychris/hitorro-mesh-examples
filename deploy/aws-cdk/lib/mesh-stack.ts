/*
 * Mesh runtime:
 *   nats            — Fargate + EFS (JetStream state)
 *   driver          — Fargate behind an internet-facing ALB, port 8085
 *   agent (Nx)      — Fargate replicas, private only, EFS for KV+Lucene
 *
 * All three tasks join the cluster's CloudMap namespace "mesh.local"
 * so they can find each other by DNS: `nats.mesh.local:4222`, etc.
 */
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as efs from 'aws-cdk-lib/aws-efs';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3  from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

export interface MeshStackProps extends cdk.StackProps {
  vpc: ec2.Vpc;
  cluster: ecs.Cluster;
  albSg: ec2.SecurityGroup;
  serviceSg: ec2.SecurityGroup;
  efs: efs.FileSystem;
  blobBucket: s3.Bucket;
  driverRepo: ecr.Repository;
  agentRepo:  ecr.Repository;
  natsRepo:   ecr.Repository;
  imageTag: string;
  agents: number;
  driverMem: number;
  agentMem: number;
  natsMem: number;
}

export class MeshStack extends cdk.Stack {
  /** Public DNS of the driver's ALB — hand to the fleet stack so
   *  retrieval + DMS can call the driver's REST endpoints. */
  public readonly driverAlbDns: string;

  constructor(scope: Construct, id: string, props: MeshStackProps) {
    super(scope, id, props);

    const logGroup = new logs.LogGroup(this, 'MeshLogs', {
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Per-service EFS access points — chroots + separate posix uids
    const natsAp   = new efs.AccessPoint(this, 'NatsAp', {
      fileSystem: props.efs, path: '/nats',
      createAcl: { ownerUid: '1001', ownerGid: '1001', permissions: '755' },
      posixUser: { uid: '1001', gid: '1001' },
    });
    const agentAp  = new efs.AccessPoint(this, 'AgentAp', {
      fileSystem: props.efs, path: '/agents',
      createAcl: { ownerUid: '1002', ownerGid: '1002', permissions: '755' },
      posixUser: { uid: '1002', gid: '1002' },
    });

    // Task role — agents write to S3 for blob offload; NATS + driver don't need it.
    const agentTaskRole = new iam.Role(this, 'AgentTaskRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
    });
    props.blobBucket.grantReadWrite(agentTaskRole);

    // ---- NATS ------------------------------------------------------------
    const natsTd = new ecs.FargateTaskDefinition(this, 'NatsTd', {
      cpu: 256, memoryLimitMiB: props.natsMem,
      volumes: [{ name: 'jetstream', efsVolumeConfiguration: {
        fileSystemId: props.efs.fileSystemId,
        transitEncryption: 'ENABLED',
        authorizationConfig: { accessPointId: natsAp.accessPointId, iam: 'ENABLED' },
      }}],
    });
    const natsContainer = natsTd.addContainer('nats', {
      image: ecs.ContainerImage.fromEcrRepository(props.natsRepo, props.imageTag),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'nats', logGroup }),
      portMappings: [
        { containerPort: 4222, name: 'client', appProtocol: ecs.AppProtocol.grpc },
        { containerPort: 8222, name: 'monitor' },
      ],
      command: ['-js', '-sd', '/data', '-m', '8222'],
    });
    natsContainer.addMountPoints({ sourceVolume: 'jetstream', containerPath: '/data', readOnly: false });
    props.efs.grantRootAccess(natsTd.taskRole);

    const natsSvc = new ecs.FargateService(this, 'NatsSvc', {
      cluster: props.cluster, taskDefinition: natsTd, desiredCount: 1,
      securityGroups: [props.serviceSg],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      cloudMapOptions: { name: 'nats' },   // → nats.mesh.local
      enableExecuteCommand: true,          // for `aws ecs execute-command` debugging
    });

    // ---- Driver ---------------------------------------------------------
    const driverTd = new ecs.FargateTaskDefinition(this, 'DriverTd', {
      cpu: 512, memoryLimitMiB: props.driverMem,
    });
    driverTd.addContainer('driver', {
      image: ecs.ContainerImage.fromEcrRepository(props.driverRepo, props.imageTag),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'driver', logGroup }),
      portMappings: [{ containerPort: 8085, name: 'http' }],
      environment: {
        SPRING_PROFILES_ACTIVE:            'aws',
        'MESH_DRIVER_NATS_URL':            'nats://nats.mesh.local:4222',
        'HITORRO_FLEET_ENDPOINTS':         `http://retrieval.mesh.local:8095`,
        JAVA_OPTS:                         '-Xms256m -Xmx800m',
      },
      healthCheck: {
        command: ['CMD-SHELL', 'curl -f http://localhost:8085/actuator/health || exit 1'],
        interval: cdk.Duration.seconds(30),
        timeout:  cdk.Duration.seconds(5),
        retries: 3,
        startPeriod: cdk.Duration.seconds(60),
      },
    });
    const driverSvc = new ecs.FargateService(this, 'DriverSvc', {
      cluster: props.cluster, taskDefinition: driverTd, desiredCount: 1,
      securityGroups: [props.serviceSg],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      cloudMapOptions: { name: 'driver' },
      enableExecuteCommand: true,
    });

    // Public ALB in front of the driver
    const driverAlb = new elbv2.ApplicationLoadBalancer(this, 'DriverAlb', {
      vpc: props.vpc, internetFacing: true, securityGroup: props.albSg,
    });
    const driverListener = driverAlb.addListener('DriverHttp', {
      port: 80, open: true,
      defaultAction: elbv2.ListenerAction.fixedResponse(404, { messageBody: 'no route' }),
    });
    driverListener.addTargets('DriverTg', {
      port: 8085, protocol: elbv2.ApplicationProtocol.HTTP,
      targets: [driverSvc],
      healthCheck: { path: '/actuator/health', healthyHttpCodes: '200-299' },
    });
    this.driverAlbDns = driverAlb.loadBalancerDnsName;

    // ---- Agents ---------------------------------------------------------
    const agentTd = new ecs.FargateTaskDefinition(this, 'AgentTd', {
      cpu: 1024, memoryLimitMiB: props.agentMem,
      taskRole: agentTaskRole,
      volumes: [{ name: 'agent-state', efsVolumeConfiguration: {
        fileSystemId: props.efs.fileSystemId,
        transitEncryption: 'ENABLED',
        authorizationConfig: { accessPointId: agentAp.accessPointId, iam: 'ENABLED' },
      }}],
    });
    const agentContainer = agentTd.addContainer('agent', {
      image: ecs.ContainerImage.fromEcrRepository(props.agentRepo, props.imageTag),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'agent', logGroup }),
      portMappings: [{ containerPort: 8091, name: 'http' }],
      environment: {
        SPRING_PROFILES_ACTIVE: 'aws',
        'MESH_AGENT_NATS_URL':  'nats://nats.mesh.local:4222',
        'MESH_AGENT_HOME':      '/data',
        'BLOB_BUCKET':          props.blobBucket.bucketName,
        JAVA_OPTS:              '-Xms512m -Xmx1500m',
      },
      healthCheck: {
        command: ['CMD-SHELL', 'curl -f http://localhost:8091/actuator/health || exit 1'],
        interval: cdk.Duration.seconds(30),
        timeout:  cdk.Duration.seconds(5),
        retries: 3,
        startPeriod: cdk.Duration.seconds(90),
      },
    });
    agentContainer.addMountPoints({ sourceVolume: 'agent-state', containerPath: '/data', readOnly: false });
    props.efs.grantRootAccess(agentTd.taskRole);

    const agentSvc = new ecs.FargateService(this, 'AgentSvc', {
      cluster: props.cluster, taskDefinition: agentTd,
      desiredCount: props.agents,
      securityGroups: [props.serviceSg],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      cloudMapOptions: { name: 'agent' },   // → agent.mesh.local (multi-A record)
      enableExecuteCommand: true,
    });

    // Driver + agents must start AFTER NATS is healthy
    driverSvc.node.addDependency(natsSvc);
    agentSvc.node.addDependency(natsSvc);

    new cdk.CfnOutput(this, 'DriverAlbDns',  { value: this.driverAlbDns });
    new cdk.CfnOutput(this, 'DriverUiUrl',   { value: `http://${this.driverAlbDns}/` });
    new cdk.CfnOutput(this, 'MeshApiUrl',    { value: `http://${this.driverAlbDns}/mesh/` });
  }
}
