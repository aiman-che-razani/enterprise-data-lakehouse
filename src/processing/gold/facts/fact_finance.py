"""
fact_finance — Gold General Ledger fact table.

Grain: one row per GL transaction (nk_transaction_id).
Each row is a balanced double-entry record (debit == credit within 0.01).

Sources: Silver finance

Dimension FKs:
  sk_date_id  ← YYYYMMDD(transaction_date)
  sk_store_id ← xxhash64(store_id) if present, else null

Measures:
  debit_amount, credit_amount

Degenerate dims:
  transaction_type, account_type, gl_account
  is_revenue, is_expense, fiscal_quarter
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import date_to_sk, null_sk


class FactFinanceBuilder(BaseGoldBuilder):
    table_name = "fact_finance"
    is_dim = False

    def build_fact(self) -> DataFrame:
        silver = self.read_silver("finance")

        has_store = "store_id" in silver.columns

        fact = self._with_primary_account(silver).withColumn(
            "sk_date_id", date_to_sk("transaction_date")
        )

        if has_store:
            fact = fact.withColumn(
                "sk_store_id",
                F.when(
                    F.col("store_id").isNotNull(),
                    F.xxhash64(F.col("store_id").cast("string")).cast(LongType()),
                ).otherwise(null_sk()),
            )
        else:
            fact = fact.withColumn("sk_store_id", null_sk())

        select_cols: list[str | Column] = [
            "sk_date_id",
            "sk_store_id",
            F.col("transaction_id").alias("nk_transaction_id"),
            "transaction_type",
            "gl_account",
            "account_type",
            "debit_amount",
            "credit_amount",
            "is_revenue",
            "is_expense",
            "fiscal_quarter",
            "transaction_date",
        ]
        if "description" in silver.columns:
            select_cols.append("description")
        if "posted_at" in silver.columns:
            select_cols.append("posted_at")

        return fact.select(*select_cols)

    @staticmethod
    def _with_primary_account(silver: DataFrame) -> DataFrame:
        """Silver is a double-entry journal (a debit and a credit account per row); the fact reports one.

        The primary account is the income-statement side of the entry (revenue, COGS or expense, i.e.
        codes 4000+), falling back to the debit side for pure balance-sheet movements. Its type comes from
        the chart of accounts' leading digit: 1 asset, 2 liability, 3 equity, 4 revenue, 5 cogs, 6+ expense.
        """
        if "gl_account" in silver.columns:
            return silver

        def pnl(code: str) -> Column:
            return F.substring(F.col(code), 1, 1).isin("4", "5", "6", "7", "8", "9")

        primary = (
            F.when(pnl("debit_account_code"), F.col("debit_account_code"))
            .when(pnl("credit_account_code"), F.col("credit_account_code"))
            .otherwise(F.col("debit_account_code"))
        )
        lead = F.substring(primary, 1, 1)
        account_type = (
            F.when(lead == "1", "asset")
            .when(lead == "2", "liability")
            .when(lead == "3", "equity")
            .when(lead == "4", "revenue")
            .when(lead == "5", "cogs")
            .otherwise("expense")
        )
        return (
            silver.withColumn("gl_account", primary)
            .withColumn("account_type", account_type)
            .withColumn("is_revenue", F.col("account_type") == "revenue")
            .withColumn("is_expense", F.col("account_type").isin("cogs", "expense"))
        )
