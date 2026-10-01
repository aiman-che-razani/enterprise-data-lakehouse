"""
dim_employee — Gold employee dimension.

Source: Silver employees (is_current = True).
PII note: raw email was hashed in Silver → Gold carries email_hash only.
          first_name / last_name are keyed hashes (first_name_hash / last_name_hash): the dimension supports joins
          and de-duplication, not display of names.

Columns:
  sk_employee_id  — surrogate key
  sk_store_id     — FK to dim_store
  sk_manager_id   — self-referential FK to dim_employee (nullable)
  nk_employee_id
  first_name_hash, last_name_hash, email_hash
  department, job_title, level
  hire_date, termination_date
  tenure_years, total_compensation
  is_active
  effective_from, effective_to
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import add_sk, null_sk


class DimEmployeeBuilder(BaseGoldBuilder):
    table_name = "dim_employee"
    is_dim = True

    def build_dim(self) -> DataFrame:
        silver = self.read_silver_current("employees")

        select_cols = [
            "employee_id",
            "first_name_hash",
            "last_name_hash",
            "department",
            "job_title",
            "level",
            "store_id",
            "manager_id",
            "hire_date",
            "tenure_years",
            "base_salary",
            "bonus_pct",
            "total_compensation",
            "is_active",
            "effective_from",
            "effective_to",
        ]
        if "email_hash" in silver.columns:
            select_cols.append("email_hash")
        if "termination_date" in silver.columns:
            select_cols.append("termination_date")

        dim = (
            silver.select(*select_cols)
            .withColumnRenamed("employee_id", "nk_employee_id")
            .withColumnRenamed("store_id", "_raw_store_id")
            .withColumnRenamed("manager_id", "_raw_manager_id")
        )

        dim = add_sk(dim, "sk_employee_id", "nk_employee_id")
        dim = add_sk(dim, "sk_store_id", "_raw_store_id")

        # Self-referential SK — null for top-level employees (no manager)
        dim = dim.withColumn(
            "sk_manager_id",
            F.when(
                F.col("_raw_manager_id").isNotNull(),
                F.xxhash64(F.col("_raw_manager_id").cast("string")),
            ).otherwise(null_sk()),
        )

        return dim.drop("_raw_store_id", "_raw_manager_id")
