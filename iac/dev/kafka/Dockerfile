# Broker Kafka + agent JMX Prometheus.
#
# Étend l'image Confluent en y ajoutant le "javaagent" jmx_prometheus : un
# petit .jar Java greffé DANS le process Kafka, qui lit les métriques JMX
# internes et les expose en HTTP (/metrics) au format Prometheus.
#
# On active ensuite l'agent côté docker-compose via la variable KAFKA_OPTS :
#   -javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=<port>:<config>
FROM confluentinc/cp-kafka:7.5.0

# Version de l'agent (Maven Central). Bump volontaire = commit explicite.
ARG JMX_EXPORTER_VERSION=0.20.0
ARG JMX_AGENT_URL=https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar

USER root
RUN mkdir -p /opt/jmx-exporter \
    && curl -fsSL -o /opt/jmx-exporter/jmx_prometheus_javaagent.jar "${JMX_AGENT_URL}"
USER appuser
