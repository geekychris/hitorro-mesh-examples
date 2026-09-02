FROM eclipse-temurin:21-jre-alpine
RUN apk add --no-cache curl bash
WORKDIR /app
COPY hitorro-mesh-agent-app.jar /app/app.jar
EXPOSE 8091
ENV JAVA_OPTS="-Xms512m -Xmx1500m"
ENTRYPOINT ["/bin/bash", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
