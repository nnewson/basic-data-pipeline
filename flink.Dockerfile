FROM flink:2.3.0-scala_2.12

# Keep this tied to the Flink runtime version. The 4.0.0-2.0 connector is the
# SQL Kafka connector line built for Flink 2.x.
ARG KAFKA_CONNECTOR_VERSION=4.0.0-2.0

USER root

# The stock Flink image has the JVM runtime, but the PyFlink job also needs a
# Python executable and a few Python modules available inside the container.
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-ruamel.yaml python3-typing-extensions wget && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    wget -P /opt/flink/lib "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" && \
    rm -rf /var/lib/apt/lists/*

USER flink

WORKDIR /opt/pipeline
