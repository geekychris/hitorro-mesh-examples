/*
 * Fleet services that hang off the mesh:
 *   retrieval — hitorro-fleet-retrieval, port 8095, EFS for Lucene
 *   dms       — hitorro-nosqldms, port 8090, EFS for Lucene + JVS types dir
 *
 * Both sit behind the same internet-facing ALB on different paths:
 *   /retrieval/* → retrieval TG
 *   /*           → DMS TG (root)
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

export interface FleetStackProps extends cdk.StackProps {
  vpc: ec2.Vpc;
  cluster: ecs.Cluster;
  albSg: ec2.SecurityGroup;
  serviceSg: ec2.SecurityGroup;
  efs: efs.FileSystem;
  blobBucket: s3.Bucket;
  retrievalRepo: ecr.Repository;
  dmsRepo:       ecr.Repository;
  meshDriverAlbDns: string;
  imageTag: string;
  retrievalMem: number;
  dmsMem: number;
  enableRetrieval: boolean;
  enableDms: boolean;
}

export class FleetStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: FleetStackProps) {
    super(scope, id, props);

    const logGroup = new logs.LogGroup(this, 'FleetLogs', {
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Public ALB — DMS UI + retrieval API sit behind one LB, different paths
    const alb = new elbv2.ApplicationLoadBalancer(this, 'FleetAlb', {
      vpc: props.vpc, internetFacing: true, securityGroup: props.albSg,
    });
    const listener = alb.addListener('Http', {
      port: 80, open: true,
      defaultAction: elbv2.ListenerAction.fixedResponse(404, { messageBody: 'no route' }),
    });

    if (props.enableRetrieval) {
      this.retrieval(props, alb, listener, logGroup);
    }
    if (props.enableDms) {
      this.dms(props, alb, listener, logGroup);
    }

    new cdk.CfnOutput(this, 'FleetAlbDns', { value: alb.loadBalancerDnsName });
    if (props.enableDms)       new cdk.CfnOutput(this, 'DmsUiUrl',        { value: `http://${alb.loadBalancerDnsName}/` });
    if (props.enableRetrieval) new cdk.CfnOutput(this, 'RetrievalApiUrl', { value: `http://${alb.loadBalancerDnsName}/retrieval/` });
  }

  // ---- retrieval ------------------------------------------------------
  private retrieval(props: FleetStackProps, alb: elbv2.ApplicationLoadBalancer,
                    listener: elbv2.ApplicationListener, logGroup: logs.LogGroup): void {
    const ap = new efs.AccessPoint(this, 'RetrievalAp', {
      fileSystem: props.efs, path: '/retrieval',
      createAcl: { ownerUid: '1003', ownerGid: '1003', permissions: '755' },
      posixUser: { uid: '1003', gid: '1003' },
    });

    const taskRole = new iam.Role(this, 'RetrievalTaskRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
    });
    props.blobBucket.grantRead(taskRole);

    const td = new ecs.FargateTaskDefinition(this, 'RetrievalTd', {
      cpu: 512, memoryLimitMiB: props.retrievalMem, taskRole,
      volumes: [{ name: 'idx', efsVolumeConfiguration: {
        fileSystemId: props.efs.fileSystemId,
        transitEncryption: 'ENABLED',
        authorizationConfig: { accessPointId: ap.accessPointId, iam: 'ENABLED' },
      }}],
    });
    const c = td.addContainer('retrieval', {
      image: ecs.ContainerImage.fromEcrRepository(props.retrievalRepo, props.imageTag),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'retrieval', logGroup }),
      portMappings: [{ containerPort: 8095, name: 'http' }],
      environment: {
        SPRING_PROFILES_ACTIVE:        'aws',
        'HITORRO_MESH_DRIVER_URL':     `http://${props.meshDriverAlbDns}`,
        'HITORRO_FLEET_RETRIEVAL_HOME': '/data',
        JAVA_OPTS:                     '-Xms256m -Xmx800m',
      },
      healthCheck: {
        command: ['CMD-SHELL', 'curl -f http://localhost:8095/actuator/health || exit 1'],
        interval: cdk.Duration.seconds(30),
        timeout:  cdk.Duration.seconds(5),
        retries: 3,
        startPeriod: cdk.Duration.seconds(60),
      },
    });
    c.addMountPoints({ sourceVolume: 'idx', containerPath: '/data', readOnly: false });
    props.efs.grantRootAccess(td.taskRole);

    const svc = new ecs.FargateService(this, 'RetrievalSvc', {
      cluster: props.cluster, taskDefinition: td, desiredCount: 1,
      securityGroups: [props.serviceSg],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      cloudMapOptions: { name: 'retrieval' },
      enableExecuteCommand: true,
    });

    listener.addTargets('RetrievalTg', {
      priority: 10,
      conditions: [elbv2.ListenerCondition.pathPatterns(['/retrieval/*', '/api/retrieval/*'])],
      port: 8095, protocol: elbv2.ApplicationProtocol.HTTP,
      targets: [svc],
      healthCheck: { path: '/actuator/health', healthyHttpCodes: '200-299' },
    });
  }

  // ---- DMS ------------------------------------------------------------
  private dms(props: FleetStackProps, alb: elbv2.ApplicationLoadBalancer,
              listener: elbv2.ApplicationListener, logGroup: logs.LogGroup): void {
    const ap = new efs.AccessPoint(this, 'DmsAp', {
      fileSystem: props.efs, path: '/dms',
      createAcl: { ownerUid: '1004', ownerGid: '1004', permissions: '755' },
      posixUser: { uid: '1004', gid: '1004' },
    });

    const taskRole = new iam.Role(this, 'DmsTaskRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
    });
    props.blobBucket.grantReadWrite(taskRole);

    const td = new ecs.FargateTaskDefinition(this, 'DmsTd', {
      cpu: 512, memoryLimitMiB: props.dmsMem, taskRole,
      volumes: [{ name: 'state', efsVolumeConfiguration: {
        fileSystemId: props.efs.fileSystemId,
        transitEncryption: 'ENABLED',
        authorizationConfig: { accessPointId: ap.accessPointId, iam: 'ENABLED' },
      }}],
    });
    const c = td.addContainer('dms', {
      image: ecs.ContainerImage.fromEcrRepository(props.dmsRepo, props.imageTag),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'dms', logGroup }),
      portMappings: [{ containerPort: 8090, name: 'http' }],
      environment: {
        SPRING_PROFILES_ACTIVE: 'aws',
        'DMS_HOME':             '/data',
        'BLOB_BUCKET':          props.blobBucket.bucketName,
        JAVA_OPTS:              '-Xms256m -Xmx800m',
      },
      healthCheck: {
        command: ['CMD-SHELL', 'curl -f http://localhost:8090/actuator/health || exit 1'],
        interval: cdk.Duration.seconds(30),
        timeout:  cdk.Duration.seconds(5),
        retries: 3,
        startPeriod: cdk.Duration.seconds(60),
      },
    });
    c.addMountPoints({ sourceVolume: 'state', containerPath: '/data', readOnly: false });
    props.efs.grantRootAccess(td.taskRole);

    const svc = new ecs.FargateService(this, 'DmsSvc', {
      cluster: props.cluster, taskDefinition: td, desiredCount: 1,
      securityGroups: [props.serviceSg],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      cloudMapOptions: { name: 'dms' },
      enableExecuteCommand: true,
    });

    // Default rule → DMS (catch-all). Retrieval registered at priority 10 above
    // so /retrieval/* routes there; everything else lands on DMS root.
    listener.addTargets('DmsTg', {
      priority: 100,
      conditions: [elbv2.ListenerCondition.pathPatterns(['/*'])],
      port: 8090, protocol: elbv2.ApplicationProtocol.HTTP,
      targets: [svc],
      healthCheck: { path: '/actuator/health', healthyHttpCodes: '200-299' },
    });
  }
}
