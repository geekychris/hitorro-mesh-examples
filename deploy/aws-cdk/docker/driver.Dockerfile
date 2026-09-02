# Multi-stage: assemble the fat jar first, then ship on a slim runtime.
# The fat jar is expected to have been built already by
# `mvn -pl hitorro-mesh-driver-app package` at the mesh-repo root —
# the build.sh script bind-mounts it in.
FROM eclipse-temurin:21-jre-alpine
RUN apk add --no-cache curl bash
WORKDIR /app
COPY hitorro-mesh-driver-app.jar /app/app.jar
EXPOSE 8085
ENV JAVA_OPTS="-Xms256m -Xmx800m"
ENTRYPOINT ["/bin/bash", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
