"""
SparkSession factory for the Bronze ingestion layer.

Configures:
  - Delta Lake extensions and catalog
  - S3A connector (MinIO for dev, real S3 for prod)
  - Adaptive Query Execution (AQE)
  - Kryo serializer
  - Dynamic partition overwrite
  - Off-heap memory for columnar processing
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.common.config.settings import Settings, get_settings
from src.common.logging import get_logger

logger = get_logger(__name__)


def _with_delta_jars(builder: SparkSession.Builder) -> SparkSession.Builder:
    """Put the Delta Lake jars on the classpath when running from a plain Python process.

    The Delta extension/catalog are configured above, but without the matching jars Spark fails with
    ``ClassNotFoundException: DeltaCatalog`` on the first query - and, worse, leaves a broken session
    behind for the rest of the process. Under ``spark-submit`` the jars come from ``--packages`` (the
    Airflow DAGs and the Makefile pass them). That case is recognisable because the jar itself is on
    ``sys.path``, so the ``delta`` module is imported from *inside* a ``.jar``; its pip helper would
    then fail for lack of ``delta-spark`` package metadata, so it must not be called.
    """
    try:
        import delta
    except ImportError:  # delta-spark not installed: rely on --packages / the cluster image
        return builder
    if ".jar" in (getattr(delta, "__file__", None) or ""):
        return builder
    try:
        return delta.configure_spark_with_delta_pip(builder)
    except Exception as exc:  # metadata backports raise their own PackageNotFoundError
        if type(exc).__name__ != "PackageNotFoundError":
            raise
        return builder


def s3a_conf(settings: Settings) -> dict[str, str]:
    """S3A settings for the session.

    With ``S3_ENDPOINT_URL`` set (local MinIO) the endpoint, path-style access and the
    configured static keys are used. Without it (AWS) NO credentials are configured here:
    the default provider chain (task / instance role, env) supplies them.
    """
    conf = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.committer.name": "magic",
        "spark.hadoop.fs.s3a.committer.magic.enabled": "true",
    }
    if settings.s3_endpoint_url:
        conf.update(
            {
                "spark.hadoop.fs.s3a.endpoint": settings.s3_endpoint_url,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.access.key": settings.aws_access_key_id,
                "spark.hadoop.fs.s3a.secret.key": settings.aws_secret_access_key.get_secret_value(),
            }
        )
    else:
        conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
    return conf


def create_bronze_spark_session(
    app_name: str = "RetailNexus-Bronze-Ingestor",
    settings: Settings | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """
    Build and return a SparkSession tuned for Bronze ingestion.

    Args:
        app_name:   Spark application name (shown in Spark UI).
        settings:   Inject a custom Settings instance (useful in tests).
        extra_conf: Additional Spark conf overrides (merged last).

    Returns:
        A fully configured SparkSession with Delta Lake support.
    """
    s = settings or get_settings()

    builder = (
        SparkSession.builder.appName(app_name)
        # ── Delta Lake extensions ─────────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── Delta Lake options ────────────────────────────────────────────────
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        # ── AQE + performance ─────────────────────────────────────────────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config(
            "spark.sql.autoBroadcastJoinThreshold",
            str(256 * 1024 * 1024),  # 256 MB
        )
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))  # 128 MB
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # ── Serialization ─────────────────────────────────────────────────────
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        # ── Memory ────────────────────────────────────────────────────────────
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1g")
        # ── Logging ───────────────────────────────────────────────────────────
        .config("spark.eventLog.enabled", "false")
    )

    for key, value in s3a_conf(s).items():
        builder = builder.config(key, value)

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    builder = _with_delta_jars(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "spark_session_created",
        app_name=app_name,
        master=spark.sparkContext.master,
        version=spark.version,
    )
    return spark
