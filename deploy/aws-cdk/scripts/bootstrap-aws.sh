#!/usr/bin/env bash
# One-time CDK bootstrap. Creates the toolkit stack (S3 bucket +
# roles) CDK needs to deploy anything.
set -euo pipefail
REGION=${REGION:-us-east-1}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo "bootstrapping cdk for $ACCOUNT/$REGION"
npx cdk bootstrap "aws://$ACCOUNT/$REGION"
