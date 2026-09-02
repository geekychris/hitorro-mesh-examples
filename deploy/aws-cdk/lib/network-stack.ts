/*
 * VPC, subnets, ECS cluster, and shared security groups.
 * Kept in its own stack so it survives redeploys of service stacks
 * — you don't want to bounce the NAT gateway every time you tweak
 * a task definition.
 */
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import { Construct } from 'constructs';

export interface NetworkStackProps extends cdk.StackProps {
  /** true = 1 AZ (cheaper — one NAT gateway, no cross-AZ EFS traffic).
   *  Set to false for production (2 AZs, HA). */
  singleAz: boolean;
}

export class NetworkStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;
  public readonly cluster: ecs.Cluster;
  /** Attached to the two ALBs (mesh driver, DMS UI) — public HTTP ingress. */
  public readonly albSg: ec2.SecurityGroup;
  /** Attached to every Fargate service — allows ingress from albSg + peer services. */
  public readonly serviceSg: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props: NetworkStackProps) {
    super(scope, id, props);

    // Public subnets host the ALBs + NAT gateway; private subnets host
    // the Fargate tasks. Two AZs by default; --context singleAz=true
    // for the cheapest dev setup.
    this.vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: props.singleAz ? 1 : 2,
      natGateways: props.singleAz ? 1 : 2,
      subnetConfiguration: [
        { name: 'public',  subnetType: ec2.SubnetType.PUBLIC,               cidrMask: 24 },
        { name: 'private', subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,  cidrMask: 22 },
      ],
    });

    // ECS cluster with CloudMap namespace for internal service DNS —
    // "nats.mesh.local", "driver.mesh.local", etc. Fargate services
    // register their tasks under that namespace so peers can discover
    // them without hard-coding IPs.
    this.cluster = new ecs.Cluster(this, 'Cluster', {
      vpc: this.vpc,
      defaultCloudMapNamespace: { name: 'mesh.local', useForServiceConnect: true },
      containerInsights: true,
    });

    // ALB SG — open :80 (HTTP) to the world; add :443 + a cert to go TLS.
    this.albSg = new ec2.SecurityGroup(this, 'AlbSg', {
      vpc: this.vpc, description: 'ALB ingress', allowAllOutbound: true,
    });
    this.albSg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(80), 'HTTP');

    // Service SG — ALB can reach service ports; services can reach each
    // other on any port (mesh + retrieval + dms talk to each other +
    // to NATS 4222 + JetStream 4223).
    this.serviceSg = new ec2.SecurityGroup(this, 'ServiceSg', {
      vpc: this.vpc, description: 'Fargate service', allowAllOutbound: true,
    });
    this.serviceSg.addIngressRule(this.albSg, ec2.Port.tcpRange(8000, 9000),
        'ALB → services on the app port range');
    this.serviceSg.addIngressRule(this.serviceSg, ec2.Port.allTraffic(),
        'services → services (mesh, NATS, retrieval joins)');

    new cdk.CfnOutput(this, 'VpcId',        { value: this.vpc.vpcId });
    new cdk.CfnOutput(this, 'ClusterArn',   { value: this.cluster.clusterArn });
    new cdk.CfnOutput(this, 'ClusterName',  { value: this.cluster.clusterName });
  }
}
