# Straight pull-through of the official nats image. Kept as our own
# tag so ECR IAM applies uniformly. The mesh-stack passes
# `-js -sd /data -m 8222` as CMD so JetStream state lands on EFS.
FROM nats:2.10-alpine
