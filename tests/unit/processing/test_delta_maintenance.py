"""Layer-aware Delta maintenance job: table lists, ZORDER selection, CLI, and the DAG wiring."""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from spark.jobs.maintenance import delta_maintenance as dm
from src.processing.gold.domain_meta import GOLD_DIM_ORDER, GOLD_FACT_ORDER
from src.processing.silver.domain_meta import SILVER_PROCESSING_ORDER

_ROOT = Path(__file__).resolve().parents[3]


class TestTables:
    def test_silver_and_bronze_use_all_domains(self):
        assert dm.tables_for("bronze") == dm.tables_for("silver") == list(SILVER_PROCESSING_ORDER)

    def test_gold_is_all_dims_and_facts(self):
        assert dm.tables_for("gold") == list(GOLD_DIM_ORDER) + list(GOLD_FACT_ORDER)

    def test_zorder_only_where_configured(self):
        assert dm.zorder_cols("gold", "fact_sales") == [
            "sk_customer_id",
            "sk_product_id",
            "sk_date_id",
        ]
        assert dm.zorder_cols("gold", "dim_date") == []
        assert dm.zorder_cols("silver", "orders") == []


class TestCli:
    def test_all_with_layer(self):
        args = dm.parse_args(["--layer", "gold", "--all"])
        assert args.all and args.layer == "gold"

    def test_dag_style_arguments_are_accepted(self):
        args = dm.parse_args(["--layer", "silver", "--all", "--vacuum", "--vacuum-hours", "72"])
        assert args.vacuum_hours == 72

    def test_unknown_table_rejected(self):
        with pytest.raises(SystemExit):
            dm.parse_args(["--layer", "gold", "--table", "nope"])

    def test_table_and_all_are_mutually_exclusive(self):
        with pytest.raises(SystemExit):
            dm.parse_args(["--layer", "gold", "--all", "--table", "dim_date"])

    def test_layer_required(self):
        with pytest.raises(SystemExit):
            dm.parse_args(["--all"])


class TestMaintainTable:
    def test_absent_table_is_skipped_not_failed(self):
        spark = MagicMock()
        spark.read.format.return_value.load.side_effect = RuntimeError("missing")
        assert (
            dm.maintain_table(spark, "gold", "fact_sales", "s3a://b/g/fact_sales", 72)["status"]
            == "absent"
        )
        spark.sql.assert_not_called()

    def test_optimize_with_zorder_then_vacuum(self):
        spark = MagicMock()
        result = dm.maintain_table(spark, "gold", "fact_sales", "s3a://b/gold/fact_sales", 72)
        statements = [c.args[0] for c in spark.sql.call_args_list]
        assert result["status"] == "success"
        assert statements[0] == (
            "OPTIMIZE delta.`s3a://b/gold/fact_sales` ZORDER BY (sk_customer_id, sk_product_id, sk_date_id)"
        )
        assert statements[1] == "VACUUM delta.`s3a://b/gold/fact_sales` RETAIN 72 HOURS"

    def test_skip_vacuum(self):
        spark = MagicMock()
        dm.maintain_table(spark, "silver", "orders", "s3a://b/silver/orders", None)
        assert [c.args[0] for c in spark.sql.call_args_list] == [
            "OPTIMIZE delta.`s3a://b/silver/orders`"
        ]

    def test_failure_is_reported(self):
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("boom")
        result = dm.maintain_table(spark, "silver", "orders", "s3a://b/silver/orders", 72)
        assert result["status"] == "failed" and "boom" in result["error"]


class TestDagWiring:
    def test_every_spark_application_referenced_by_dags_exists(self):
        """DAGs referenced spark/jobs/{silver,gold}/optimize_*.py, which never existed."""
        for dag in (_ROOT / "airflow" / "dags").glob("*.py"):
            text = dag.read_text(encoding="utf-8")
            for app in re.findall(r'application\s*=\s*"((?:spark|scripts)/[^"{}]+\.py)"', text):
                assert (_ROOT / app).is_file(), f"{dag.name} references missing {app}"

    def test_maintenance_dag_uses_the_generic_job(self):
        text = (_ROOT / "airflow" / "dags" / "pipeline_maintenance_dag.py").read_text(
            encoding="utf-8"
        )
        assert "spark/jobs/maintenance/delta_maintenance.py" in text
        assert '"--layer", layer' in text


class TestBronzeRetention:
    """Bronze's Delta log/deleted-file retention is 7 years; VACUUM must not undercut it."""

    def test_default_matches_the_table_property_set_at_ingest(self):
        assert dm.DEFAULT_VACUUM_HOURS["bronze"] == dm.BRONZE_RETENTION_HOURS >= 7 * 365 * 24
        ingestor = (_ROOT / "src" / "processing" / "bronze" / "batch_ingestor.py").read_text(
            encoding="utf-8"
        )
        # Delta rejects "years"; the ingest property is the same span expressed in days
        days = dm.BRONZE_RETENTION_HOURS // 24
        assert f'"delta.deletedFileRetentionDuration", "interval {days} days"' in ingestor
        assert f'"delta.logRetentionDuration", "interval {days} days"' in ingestor

    def test_short_bronze_vacuum_is_refused(self):
        with pytest.raises(SystemExit):
            dm.parse_args(["--layer", "bronze", "--all", "--vacuum-hours", "168"])

    def test_bronze_optimize_only_is_still_allowed(self):
        assert dm.parse_args(
            ["--layer", "bronze", "--all", "--skip-vacuum", "--vacuum-hours", "168"]
        ).skip_vacuum

    def test_silver_and_gold_keep_short_windows(self):
        assert (
            dm.parse_args(["--layer", "silver", "--all", "--vacuum-hours", "72"]).vacuum_hours == 72
        )

    def test_dag_passes_the_full_bronze_window(self):
        text = (_ROOT / "airflow" / "dags" / "pipeline_maintenance_dag.py").read_text(
            encoding="utf-8"
        )
        assert '"bronze": 7 * 365 * 24 + 48' in text
        assert dm.BRONZE_RETENTION_HOURS <= 7 * 365 * 24 + 48

    def test_standalone_bronze_job_default_is_not_seven_days(self):
        text = (_ROOT / "spark" / "jobs" / "bronze" / "optimize_bronze.py").read_text(
            encoding="utf-8"
        )
        assert "default=168" not in text and "7 * 365 * 24" in text

    def test_s3_lifecycle_never_expires_bronze_before_seven_years(self):
        tf = (_ROOT / "terraform" / "modules" / "s3" / "variables.tf").read_text(encoding="utf-8")
        assert "default     = 2555" in tf  # bronze_expiration_days
        for env in ("dev", "staging", "prod"):
            tfvars = (_ROOT / "terraform" / "environments" / env / f"{env}.tfvars").read_text(
                encoding="utf-8"
            )
            assert (
                "bronze_expiration_days" not in tfvars
            ), f"{env} must not shorten Bronze retention"
