/*
 * Persistent state:
 *   - S3 bucket for content-addressed blobs (DMS renditions,
 *     large-file offload; anything the app fetches by sha256)
 *   - EFS filesystem, one shared file-system with per-service
 *     access points (mesh-agents, NATS JetStream, DMS Lucene,
 *     retrieval Lucene). Each service mounts its own subdir.
 */
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as efs from 'aws-cdk-lib/aws-efs';
import * as s3  from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

export interface StorageStackProps extends cdk.StackProps {
  vpc: ec2.Vpc;
}

export class StorageStack extends cdk.Stack {
  public readonly blobBucket: s3.Bucket;
  public readonly stateFs:    efs.FileSystem;

  constructor(scope: Construct, id: string, props: StorageStackProps) {
    super(scope, id, props);

    // Content-addressed blob store. Versioning off (content is
    // sha256-addressed → immutable by definition). Lifecycle rule
    // moves 30-day-old blobs to Intelligent-Tiering for cost.
    this.blobBucket = new s3.Bucket(this, 'BlobBucket', {
      encryption:       s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      versioned:        false,
      removalPolicy:    cdk.RemovalPolicy.RETAIN,     // never accidentally destroy content
      autoDeleteObjects: false,
      lifecycleRules: [{
        id: 'move-cold-to-intelligent-tiering',
        transitions: [{ storageClass: s3.StorageClass.INTELLIGENT_TIERING, transitionAfter: cdk.Duration.days(30) }],
      }],
    });

    // EFS for the stateful volumes. Bursting throughput mode + General
    // Purpose is the sensible default. Encryption enabled.
    // POSIX users/groups configured on the access points so each
    // service writes as a distinct uid — makes it hard for a
    // compromised task to trample another service's data.
    this.stateFs = new efs.FileSystem(this, 'StateFs', {
      vpc: props.vpc,
      lifecyclePolicy:      efs.LifecyclePolicy.AFTER_14_DAYS,   // move cold files to IA
      performanceMode:      efs.PerformanceMode.GENERAL_PURPOSE,
      throughputMode:       efs.ThroughputMode.BURSTING,
      encrypted:            true,
      removalPolicy:        cdk.RemovalPolicy.RETAIN,
    });

    new cdk.CfnOutput(this, 'BlobBucketName', { value: this.blobBucket.bucketName });
    new cdk.CfnOutput(this, 'EfsId',          { value: this.stateFs.fileSystemId });
  }

  /** Per-service EFS access point — chroots the mount to /{name}/ and
   *  runs the container process as uid 1000+offset so services don't
   *  step on each other's dirs. */
  public accessPoint(name: string, uid: number): efs.AccessPoint {
    return new efs.AccessPoint(this, `${name}Ap`, {
      fileSystem: this.stateFs,
      path: `/${name}`,
      createAcl: { ownerUid: String(uid), ownerGid: String(uid), permissions: '755' },
      posixUser: { uid: String(uid), gid: String(uid) },
    });
  }
}
