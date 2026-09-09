# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Build stage: compile the multi-module build and package the API fat-jar.
# Context is the repo root (see docker-compose.yml / `make build`).
# ---------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY flink/pom.xml ./flink/pom.xml
COPY services ./services

RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -pl services/api -am clean package -Dmaven.test.skip=true

# ---------------------------------------------------------------------------
# Runtime stage: JRE only, non-root.
# ---------------------------------------------------------------------------
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

RUN useradd --system --user-group --home-dir /app app
COPY --from=build /workspace/services/api/target/flowfleet-api-boot.jar app.jar
USER app

EXPOSE 8080
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0"
# /dev/tcp is a bash builtin — no curl/wget needed in the image.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=12 \
    CMD bash -c ': < /dev/tcp/127.0.0.1/8080' || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
