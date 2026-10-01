"""
Silver processor for the finance General Ledger fact table.

The validator enforces double-entry balance (debit_amount == credit_amount ± 0.01).
Silver adds account classification for easier Gold aggregation.

Derived Silver columns:
  account_type       — "revenue" / "cogs" / "opex" / "asset" / "liability"
                       derived from gl_account code prefix (4xxx, 5xxx, 6xxx, 1xxx, 2xxx)
  is_revenue         — account_type == "revenue"
  is_expense         — account_type in ("cogs", "opex")
  fiscal_quarter     — "Q1" / "Q2" / "Q3" / "Q4" from transaction_date month
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class FinanceSilverProcessor(BaseSilverProcessor):
    domain = "finance"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        if "gl_account" in df.columns:
            # GL account codes: 1xxx=asset, 2xxx=liability, 4xxx=revenue, 5xxx=cogs, 6xxx=opex
            prefix = F.substring(F.col("gl_account").cast("string"), 1, 1)
            df = (
                df.withColumn(
                    "account_type",
                    F.when(prefix == "4", "revenue")
                    .when(prefix == "5", "cogs")
                    .when(prefix == "6", "opex")
                    .when(prefix == "1", "asset")
                    .when(prefix == "2", "liability")
                    .otherwise("other"),
                )
                .withColumn(
                    "is_revenue",
                    F.col("account_type") == "revenue",
                )
                .withColumn(
                    "is_expense",
                    F.col("account_type").isin("cogs", "opex"),
                )
            )

        if "transaction_date" in df.columns:
            month = F.month(F.col("transaction_date"))
            df = df.withColumn(
                "fiscal_quarter",
                F.when(month <= 3, "Q1")
                .when(month <= 6, "Q2")
                .when(month <= 9, "Q3")
                .otherwise("Q4"),
            )

        return df
