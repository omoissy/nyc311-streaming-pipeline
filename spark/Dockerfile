# syntax=docker/dockerfile:1

# Spark runtime with the Kafka source and Cassandra connector baked in, so the driver
# and workers never resolve packages from Maven at startup.
FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

ARG SPARK_VERSION=3.5.5
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG CASSANDRA_CONNECTOR_VERSION=3.5.1
ARG MAVEN=https://repo1.maven.org/maven2

USER root

ADD --chmod=644 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
    ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
    ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar \
    ${MAVEN}/com/datastax/spark/spark-cassandra-connector-assembly_${SCALA_VERSION}/${CASSANDRA_CONNECTOR_VERSION}/spark-cassandra-connector-assembly_${SCALA_VERSION}-${CASSANDRA_CONNECTOR_VERSION}.jar \
    /opt/spark/jars/

# Named volumes inherit this ownership, so the non-root spark user can write checkpoints.
RUN mkdir -p /opt/spark/work /opt/spark-checkpoints \
 && chown -R spark:spark /opt/spark/work /opt/spark-checkpoints

COPY --chown=spark:spark app/ /opt/spark-apps/

USER spark
WORKDIR /opt/spark/work-dir
