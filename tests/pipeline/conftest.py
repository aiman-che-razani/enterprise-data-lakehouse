"""Shared Spark session for the pipeline tests.

One Delta-configured session for the whole directory: separate per-module sessions built with
``getOrCreate()`` hand each other's (differently configured) session back, and a session that names
``DeltaCatalog`` without the Delta jars on the classpath fails every query.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from delta import configure_spark_with_delta_pip

    # Earlier tests in the same process may have left a session - or just a SparkContext - behind
    # without the Delta jars; getOrCreate() would reuse it and jars cannot be added afterwards.
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    stale = SparkSession.getActiveSession()
    if stale is not None:
        stale.stop()
    if SparkContext._active_spark_context is not None:
        SparkContext._active_spark_context.stop()
    # spark.jars.packages is only resolved when the JVM is launched, so a JVM started by an earlier
    # (non-Delta) test must be shut down for the next session to get a fresh one with the packages.
    gateway = SparkContext._gateway
    if gateway is not None:
        proc = getattr(gateway, "proc", None)
        gateway.shutdown()
        if proc is not None:
            proc.kill()
        SparkContext._gateway = None
        SparkContext._jvm = None

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")  # jars are served to the driver's own executors
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    )
    # No skip-on-failure here: a pipeline suite that silently skips everything hides real breakage.
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
