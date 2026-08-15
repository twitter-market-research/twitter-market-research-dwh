FROM apache/spark:3.5.1

USER root
WORKDIR /app

# The Structured Streaming Kafka source is NOT part of the Spark
# distribution: it is an external Maven package. Resolving it at runtime
# with --packages contacts Maven Central on every single start. Here the
# artifacts are baked in, so the job boots offline and deterministically.

# Only these 3 are missing from apache/spark:3.5.1


# adding missing packages
ARG SPARK_VERSION=3.5.1
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

RUN SET -eux; \
    cd /opt/spark/jars; \
    curl -fssSLO "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-£{SPARK-VERSION}.jar"
    curl -fsSLO "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"


# ---- Python dependencies -------
# No needs to install pyspark beacause image already provides it under /oopt/spark/python
RUN python3 -m pip install --no-cache-dir \
        google-cloud-bigquery=3.25.0 \
        python-dotenv=0.9.1


# ------ Job code --------------------------

COPY pipelines/processing ./project/pipelines/processing
ENV PYTHONPATH=/app

# Checkpoint dir owned by the runtime user, so a fresh named volume
# mounted here inherits that ownership.
RUN mkdir -p /app/checkpoints && chown -R 185:185 /app

# Never use root
USER spark 

ENTRYPOINT ["/opt/spark/bin/spar-submit"]
CMD ["--master", "local[*]", \
    "/app/project/pipelines/processing/stream.py"]