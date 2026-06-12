FROM flink:1.18.0-scala_2.12-java11

# Install Python 3 + full JDK headers (pemja, the PyFlink C bridge, compiles from
# source and needs $JAVA_HOME/include — the base image ships only a JRE).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        openjdk-11-jdk-headless build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# Make python3 the default
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Detect the JDK include path (arch-specific) and build PyFlink against it.
# JAVA_HOME is overridden only for this RUN so the runtime keeps the Flink JRE.
RUN JDK_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")" && \
    echo "Building pemja against JAVA_HOME=$JDK_HOME" && \
    JAVA_HOME="$JDK_HOME" pip3 install --no-cache-dir apache-flink==1.18.0

# ── Connector JARs ──────────────────────────────────────────────────────────
# Kafka SQL connector (includes all transitive Kafka deps)
RUN curl -fL \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" \
    -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar

# JDBC connector (Flink ↔ PostgreSQL)
RUN curl -fL \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar" \
    -o /opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar

# PostgreSQL JDBC driver
RUN curl -fL \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar" \
    -o /opt/flink/lib/postgresql-42.7.1.jar

# Copy the Flink job
COPY job.py /opt/flink/usrlib/job.py
