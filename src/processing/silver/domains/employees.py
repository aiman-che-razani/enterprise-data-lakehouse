"""
Silver processor for the employees dimension (SCD Type 2 + PII tokenisation).

SCD2 columns: department, job_title, level, manager_id, store_id,
              base_salary, bonus_pct, is_active

PII handling (from SILVER_PII_HANDLING):
  email → email_hash (SHA-256, applied by PiiTokenizer)

Derived Silver columns:
  total_compensation  — base_salary * (1 + bonus_pct)
  tenure_years        — years since hire_date
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class EmployeesSilverProcessor(BaseSilverProcessor):
    domain = "employees"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        if "base_salary" in df.columns and "bonus_pct" in df.columns:
            df = df.withColumn(
                "total_compensation",
                F.round(
                    F.col("base_salary") * (F.lit(1.0) + F.col("bonus_pct")),
                    2,
                ),
            )

        if "hire_date" in df.columns:
            df = df.withColumn(
                "tenure_years",
                F.round(
                    F.datediff(F.current_date(), F.col("hire_date")) / 365.25,
                    1,
                ),
            )

        return df
