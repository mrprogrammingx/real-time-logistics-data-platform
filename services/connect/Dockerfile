# syntax=docker/dockerfile:1
# Kafka Connect (distributed) with the Debezium PostgreSQL connector baked in.
# The Avro converter + Schema Registry client ship with cp-kafka-connect already.
FROM confluentinc/cp-kafka-connect:7.8.0

ARG DEBEZIUM_VERSION=3.0.8.Final

USER root
# The plugin tarball unpacks to debezium-connector-postgres/<jars>, which is exactly the
# per-plugin directory layout Connect's plugin.path expects.
RUN set -eux; \
    curl -fsSL \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
      | tar -xz -C /usr/share/confluent-hub-components/
USER appuser

ENV CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components
