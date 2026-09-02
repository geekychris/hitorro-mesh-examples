FROM eclipse-temurin:21-jre-alpine
RUN apk add --no-cache curl bash
WORKDIR /app
COPY hitorro-fleet-retrieval.jar /app/app.jar
EXPOSE 8095
ENV JAVA_OPTS="-Xms256m -Xmx800m"
ENTRYPOINT ["/bin/bash", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
