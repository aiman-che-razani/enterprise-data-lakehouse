"""
Snowflake warehouse loader.

Uses the official Snowflake Spark connector.
Connector coordinates: net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.5
                       net.snowflake:snowflake-jdbc:3.14.5

Write strategy:
  - Dimensions: OVERWRITE (Snowflake replaces table content atomically)
  - Facts: APPEND (Snowflake handles dedup via Streams + Tasks or MERGE in Gold)

Column naming: Snowflake stores identifiers in UPPERCASE by default.
The connector maps Spark column names directly, so lowercase DDL requires
the connector's "sfColumnNameCaseSensitive" option set to "false".

Credentials: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.loading.base_loader import BaseWarehouseLoader

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class SnowflakeLoader(BaseWarehouseLoader):
    target = "snowflake"

    @property
    def _sf_options(self) -> dict[str, str]:
        wh = self.wh
        return {
            "sfURL": f"{wh.snowflake_account}.snowflakecomputing.com",
            "sfUser": wh.snowflake_user or "",
            "sfPassword": wh.snowflake_password or "",
            "sfDatabase": wh.snowflake_database,
            "sfSchema": wh.snowflake_schema,
            "sfWarehouse": wh.snowflake_warehouse,
            "sfRole": wh.snowflake_role,
            "sfColumnNameCaseSensitive": "false",
        }

    def _write(self, df: DataFrame, table_name: str, write_mode: str) -> None:
        sf_mode = "overwrite" if write_mode == "overwrite" else "append"
        (
            df.write.format("net.snowflake.spark.snowflake")
            .options(**self._sf_options)
            .option("dbtable", table_name)
            .mode(sf_mode)
            .save()
        )
