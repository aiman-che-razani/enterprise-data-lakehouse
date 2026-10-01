"""
dim_date — Gold calendar dimension.

Generated entirely in Spark from a date range (no Silver source).
Grain: one row per calendar day from DIM_DATE_START to DIM_DATE_END.

Columns:
  sk_date_id        — YYYYMMDD integer (e.g. 20250601) — standard warehouse convention
  full_date         — DateType
  day_of_week       — 1=Sunday ... 7=Saturday (Spark dayofweek convention)
  day_name          — "Monday", "Tuesday", …
  day_of_month      — 1–31
  day_of_year       — 1–366
  week_of_year      — ISO week number
  month             — 1–12
  month_name        — "January", …
  quarter           — 1–4
  year
  is_weekend        — Saturday or Sunday
  is_leap_year
  fiscal_quarter    — "Q1"/"Q2"/"Q3"/"Q4" (calendar-aligned fiscal year)
  fiscal_year       — same as calendar year (calendar-aligned)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.domain_meta import DIM_DATE_END, DIM_DATE_START


class DimDateBuilder(BaseGoldBuilder):
    table_name = "dim_date"
    is_dim = True

    def build_dim(self) -> DataFrame:
        spark = self.spark

        # Generate one row per day between DIM_DATE_START and DIM_DATE_END
        date_df = spark.sql(
            f"""
            SELECT sequence(
                to_date('{DIM_DATE_START}'),
                to_date('{DIM_DATE_END}'),
                interval 1 day
            ) AS dates
            """
        ).selectExpr("explode(dates) AS full_date")

        dim = (
            date_df.withColumn(
                "sk_date_id",
                (
                    F.year("full_date") * 10000
                    + F.month("full_date") * 100
                    + F.dayofmonth("full_date")
                ).cast(IntegerType()),
            )
            .withColumn("day_of_week", F.dayofweek("full_date"))
            .withColumn("day_name", F.date_format("full_date", "EEEE"))
            .withColumn("day_of_month", F.dayofmonth("full_date"))
            .withColumn("day_of_year", F.dayofyear("full_date"))
            .withColumn("week_of_year", F.weekofyear("full_date"))
            .withColumn("month", F.month("full_date"))
            .withColumn("month_name", F.date_format("full_date", "MMMM"))
            .withColumn("quarter", F.quarter("full_date"))
            .withColumn("year", F.year("full_date"))
            .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))  # 1=Sun, 7=Sat
            .withColumn(
                "is_leap_year",
                (F.year("full_date") % 4 == 0)
                & ((F.year("full_date") % 100 != 0) | (F.year("full_date") % 400 == 0)),
            )
            .withColumn(
                "fiscal_quarter",
                F.when(F.month("full_date") <= 3, "Q1")
                .when(F.month("full_date") <= 6, "Q2")
                .when(F.month("full_date") <= 9, "Q3")
                .otherwise("Q4"),
            )
            .withColumn("fiscal_year", F.year("full_date"))
        )

        return dim
