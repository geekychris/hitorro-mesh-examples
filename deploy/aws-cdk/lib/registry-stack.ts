/*
 * ECR repos, one per service image. Separated from the service stacks
 * so image pushes aren't blocked on service redeploys.
 *
 * Push new images with `scripts/build-and-push-images.sh` after
 * `cdk deploy mesh-<env>-registry` prints the repo URIs.
 */
import * as cdk from 'aws-cdk-lib';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import { Construct } from 'constructs';

export class RegistryStack extends cdk.Stack {
  public readonly driverRepo:    ecr.Repository;
  public readonly agentRepo:     ecr.Repository;
  public readonly natsRepo:      ecr.Repository;
  public readonly retrievalRepo: ecr.Repository;
  public readonly dmsRepo:       ecr.Repository;

  constructor(scope: Construct, id: string, props: cdk.StackProps) {
    super(scope, id, props);

    const mkRepo = (name: string) => new ecr.Repository(this, `${name}Repo`, {
      repositoryName: `hitorro-mesh-${name}`,
      imageScanOnPush: true,
      lifecycleRules: [
        // Keep last 10 tagged images (for rollback) — untag + purge older.
        { maxImageCount: 10, tagStatus: ecr.TagStatus.TAGGED,
          tagPatternList: ['*'] },
        // Purge untagged images after 7 days (typical: failed builds).
        { maxImageAge: cdk.Duration.days(7), tagStatus: ecr.TagStatus.UNTAGGED },
      ],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    this.driverRepo    = mkRepo('driver');
    this.agentRepo     = mkRepo('agent');
    this.natsRepo      = mkRepo('nats');
    this.retrievalRepo = mkRepo('retrieval');
    this.dmsRepo       = mkRepo('dms');

    new cdk.CfnOutput(this, 'DriverRepoUri',    { value: this.driverRepo.repositoryUri });
    new cdk.CfnOutput(this, 'AgentRepoUri',     { value: this.agentRepo.repositoryUri });
    new cdk.CfnOutput(this, 'NatsRepoUri',      { value: this.natsRepo.repositoryUri });
    new cdk.CfnOutput(this, 'RetrievalRepoUri', { value: this.retrievalRepo.repositoryUri });
    new cdk.CfnOutput(this, 'DmsRepoUri',       { value: this.dmsRepo.repositoryUri });
  }
}
