# ═══════════════════════════════════════════════════════════════════════════════
# RetailNexus Airflow Image
# Base: apache/airflow 2.9.3 (Python 3.12)
# Adds: AWS, Spark, Kafka providers + custom operators
# ═══════════════════════════════════════════════════════════════════════════════
FROM apache/airflow:2.9.3-python3.12

# ─── Java runtime ─────────────────────────────────────────────────────────────
# SparkSubmitOperator shells out to spark-submit (client mode), which needs a JVM.
USER root
RUN apt-get update     && apt-get install -y --no-install-recommends default-jre-headless     && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# ─── Airflow providers & custom deps ──────────────────────────────────────────
# Install as airflow user (avoids permission issues)
USER airflow

RUN pip install --no-cache-dir \
    # AWS integration
    apache-airflow-providers-amazon==8.24.0 \
    # Spark submit operator
    apache-airflow-providers-apache-spark==4.9.0 \
    # Kafka sensors & operators
    apache-airflow-providers-apache-kafka==1.3.1 \
    # HTTP operator (REST API ingestion)
    apache-airflow-providers-http==4.10.1 \
    # Postgres operator (warehouse loads)
    apache-airflow-providers-postgres==5.11.1 \
    # Core platform deps
    "pydantic>=2.7.0" \
    "pydantic-settings>=2.3.0" \
    "structlog>=24.2.0" \
    "boto3>=1.34.0" \
    "tenacity>=8.3.0" \
    "prometheus-client>=0.20.0" \
    "confluent-kafka>=2.4.0" \
    "python-dotenv>=1.0.0"

# The Spark provider drags in the newest pyspark; the submit client must match the cluster (3.5.3).
RUN pip install --no-cache-dir "pyspark==3.5.3"

# Pre-fetch the packages the DAGs pass to spark-submit (keep in sync with _DELTA_JARS in airflow/dags/*).
# Without this, parallel tasks resolve them from Maven Central into one shared Ivy cache at run time,
# race on it ("io problem while parsing ivy file") and depend on Central being reachable.
RUN echo 'print("ivy cache warmed")' > /tmp/warm.py \
    && spark-submit --master "local[1]" \
        --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        /tmp/warm.py \
    && rm /tmp/warm.py

# ─── Application source (read-only reference) ─────────────────────────────────
# Full mounts are done via docker-compose volumes; this bakes in a snapshot
# for image-level testing and CI.
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root spark/ /opt/airflow/spark/
COPY --chown=airflow:root airflow/plugins/ /opt/airflow/plugins/

ENV PYTHONPATH=/opt/airflow/src:/opt/airflow/spark:${PYTHONPATH}

# Labels
LABEL org.opencontainers.image.title="RetailNexus Airflow"
LABEL org.opencontainers.image.description="Apache Airflow with AWS/Spark/Kafka providers for RetailNexus Lakehouse"
LABEL org.opencontainers.image.version="2.9.3"
