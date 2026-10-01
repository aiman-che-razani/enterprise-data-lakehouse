"""Small Delta Lake helpers shared by the Silver and Gold writers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column


def merge_update_set(columns: list[str], partition_cols: list[str]) -> dict[str, str | Column]:
    """Columns a MERGE may overwrite on a matched row: everything except the partition columns.

    ``_gold_date`` / ``_silver_date`` are the partition columns of most tables. Updating them on every
    match would move each existing row into today's partition on every run, rewriting files in two
    partitions for no data change; leaving them alone keeps them meaning "the day this row first
    landed in this layer".
    """
    return {c: f"source.`{c}`" for c in columns if c not in partition_cols}
