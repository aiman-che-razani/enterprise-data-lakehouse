# ═══════════════════════════════════════════════════════════════════════════════
# RetailNexus Spark Image
# Base: python:3.12-slim + Apache Spark 3.5.3 binary from official Apache archive
# Adds: Delta Lake 3.2.0, Hadoop AWS, Python deps
# ═══════════════════════════════════════════════════════════════════════════════
FROM python:3.12-slim

USER root

# ─── System deps ──────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    curl \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_VERSION=3.5.3
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# ─── Install Apache Spark from official archive ───────────────────────────────
# Using the Apache archive directly avoids third-party image availability issues.
RUN wget -q -O /tmp/spark.tgz \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && wget -q -O /tmp/spark.tgz.sha512 \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz.sha512" \
    && echo "$(cut -d' ' -f1 /tmp/spark.tgz.sha512)  /tmp/spark.tgz" | sha512sum -c - \
    && tar -xzf /tmp/spark.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm /tmp/spark.tgz /tmp/spark.tgz.sha512 \
    && mkdir -p ${SPARK_HOME}/logs ${SPARK_HOME}/work

# ─── Download Delta Lake + Hadoop AWS JARs ────────────────────────────────────
ARG DELTA_VERSION=3.2.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.665
ARG SCALA_VERSION=2.12

ENV SPARK_JARS_DIR=${SPARK_HOME}/jars

# Every JAR is verified against the SHA-1 Maven Central publishes next to it.
RUN set -eu; \
    for url in \
      "https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; do \
        f="${SPARK_JARS_DIR}/$(basename "$url")"; \
        wget -q -O "$f" "$url"; \
        echo "$(wget -q -O - "$url.sha1")  $f" | sha1sum -c -; \
    done

# ─── Python dependencies ──────────────────────────────────────────────────────
# SPARK_HOME env var directs pip-installed pyspark to use the binary above,
# ensuring the Delta + Hadoop JARs in /opt/spark/jars are on the classpath.
COPY pyproject.toml /tmp/pyproject.toml

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        pyspark==3.5.3 \
        delta-spark==3.2.0 \
        "pyarrow>=16.0.0" \
        "pandas>=2.2.0" \
        "pydantic>=2.7.0" \
        "pydantic-settings>=2.3.0" \
        "structlog>=24.2.0" \
        "boto3>=1.34.0" \
        "prometheus-client>=0.20.0" \
        "tenacity>=8.3.0" \
        "confluent-kafka>=2.4.0" \
        "python-dotenv>=1.0.0"

# ─── Spark config ─────────────────────────────────────────────────────────────
COPY configs/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# ─── Application source ───────────────────────────────────────────────────────
# Jobs import `src.*` and `spark.*` (spark.schemas, spark.streaming), so /opt/spark must be on the path
# with directories literally named `src` and `spark`.
COPY src/ /opt/spark/src/
COPY spark/ /opt/spark/spark/

ENV PYTHONPATH=/opt/spark:${PYTHONPATH}

# ─── Non-root runtime ─────────────────────────────────────────────────────────
RUN useradd -u 1001 -m -s /bin/bash spark \
    && chown -R spark:spark ${SPARK_HOME}/logs ${SPARK_HOME}/work
USER spark

LABEL org.opencontainers.image.title="RetailNexus Spark"
LABEL org.opencontainers.image.description="PySpark + Delta Lake image for RetailNexus Lakehouse"
LABEL org.opencontainers.image.version="3.5.3"
