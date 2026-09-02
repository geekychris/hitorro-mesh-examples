#!/usr/bin/env bash
# Build every service Docker image and push to the matching ECR repo.
#
# Prereqs:
#   * cdk deploy mesh-<env>-registry has printed the repo URIs
#   * aws cli authenticated for the target account
#   * docker running
#   * every Spring Boot fat jar has been built via mvn (see below)
#
# Usage:
#   ENV=dev REGION=us-east-1 TAG=v1 ./build-and-push-images.sh
#
# Env vars (defaults in parens):
#   ENV     (dev)          — CDK env context, used to look up ECR repos
#   REGION  (us-east-1)    — target AWS region
#   TAG     (latest)       — image tag to push (both this + git sha)
#   HITORRO (../../..)     — path to the hitorro workspace root
set -euo pipefail
cd "$(dirname "$0")/.."

ENV=${ENV:-dev}
REGION=${REGION:-us-east-1}
TAG=${TAG:-latest}
HITORRO=${HITORRO:-$(cd ../../.. && pwd)}
GIT_SHA=$(git -C "$HITORRO" rev-parse --short HEAD 2>/dev/null || echo unknown)

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REG="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"

echo "Building for env=$ENV region=$REGION tag=$TAG (sha=$GIT_SHA) → $REG"
aws ecr get-login-password --region "$REGION" \
    | docker login --username AWS --password-stdin "$REG"

_build_push() {
    local svc=$1 dockerfile=$2 jar_src=$3 jar_dst=$4
    local repo="hitorro-mesh-$svc"
    local uri="$REG/$repo"
    if [ -n "$jar_src" ]; then
        [ -f "$jar_src" ] || { echo "missing jar: $jar_src" >&2; exit 1; }
        cp "$jar_src" "docker/$jar_dst"
    fi
    echo "→ build $svc"
    docker build -t "$uri:$TAG" -t "$uri:$GIT_SHA" -f "docker/$dockerfile" docker/
    echo "→ push $svc"
    docker push "$uri:$TAG"
    docker push "$uri:$GIT_SHA"
    [ -n "$jar_src" ] && rm -f "docker/$jar_dst"
}

_build_push nats       nats.Dockerfile      ""                                                                                        ""
_build_push driver     driver.Dockerfile    "$HITORRO/hitorro-mesh-driver-app/target/hitorro-mesh-driver-app-3.0.1.jar"                hitorro-mesh-driver-app.jar
_build_push agent      agent.Dockerfile     "$HITORRO/hitorro-mesh-agent-app/target/hitorro-mesh-agent-app-3.0.1.jar"                  hitorro-mesh-agent-app.jar
_build_push retrieval  retrieval.Dockerfile "$HITORRO/hitorro-fleet-retrieval/target/hitorro-fleet-retrieval-3.0.1.jar"                hitorro-fleet-retrieval.jar
_build_push dms        dms.Dockerfile       "$HITORRO/hitorro-dms/hitorro-nosqldms-spring-boot/target/hitorro-nosqldms-spring-boot-0.1.0-app.jar" hitorro-nosqldms-spring-boot.jar

echo "done. images pushed as $TAG + $GIT_SHA"
