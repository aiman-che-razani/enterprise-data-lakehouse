"""Shared exception types."""

from __future__ import annotations


class WatermarkError(RuntimeError):
    """A pipeline checkpoint (``pipeline.watermarks``) could not be read or written.

    Silver and Gold raise this instead of logging a warning: their writes are idempotent
    (MERGE / overwrite), so failing the task and letting Airflow retry is safe — whereas
    carrying on with a stale checkpoint reprocesses from an old Bronze version, or lets
    Gold state diverge from the watermarks, with nothing alerting anyone.
    """
