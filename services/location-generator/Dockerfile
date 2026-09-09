# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY flink/pom.xml ./flink/pom.xml
COPY schemas ./schemas
COPY services ./services
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -pl services/location-generator -am clean package -Dmaven.test.skip=true

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
RUN useradd --system --user-group --home-dir /app app
COPY --from=build /workspace/services/location-generator/target/flowfleet-location-generator-boot.jar app.jar
USER app
EXPOSE 8090
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0"
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=12 \
    CMD bash -c ': < /dev/tcp/127.0.0.1/8090' || exit 1
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
