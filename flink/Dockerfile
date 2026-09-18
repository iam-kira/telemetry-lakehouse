# Flink 1.20 (the 1.x LTS line) + the Kafka SQL connector, for the SQL client.
# The connector sits in connectors/, not lib/: SQL sessions load it with --jar, and the
# Java job ships its own shaded copy — so no job ever sees two versions of it.
FROM flink:1.20.5-scala_2.12-java17

ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/connectors/

# Pre-create the state dir owned by flink: a fresh named volume mounted here copies this
# ownership. Without it the volume is root-owned and every checkpoint fails to write.
RUN mkdir -p /opt/flink/state && chown flink:flink /opt/flink/state
