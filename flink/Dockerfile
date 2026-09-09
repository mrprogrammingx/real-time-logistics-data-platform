# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY schemas ./schemas
COPY services ./services
COPY flink ./flink
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -pl flink -am clean package -DskipTests

# Flink 1.20 on a Java 17 runtime (the job jar is compiled to Java 17 bytecode).
FROM flink:1.20-java17
COPY --from=build /workspace/flink/target/flowfleet-flink-jobs.jar \
     /opt/flink/usrlib/flowfleet-flink-jobs.jar
