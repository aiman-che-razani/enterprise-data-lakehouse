"""Pydantic v2 response models for pipeline status endpoints."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class WatermarkResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    domain: str
    bronze_last_ingest: datetime | None = None
    silver_last_run: datetime | None = None
    gold_last_run: datetime | None = None
    redshift_last_load: datetime | None = None
    snowflake_last_load: datetime | None = None
    bigquery_last_load: datetime | None = None


class SlaMissResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: str | None = None
    domain: str
    layer: str | None = None
    metric: str
    actual_value: float | None = None
    sla_threshold: float | None = None
    message: str | None = None
    detected_at: datetime


class DomainStatusResponse(BaseModel):
    """Per-domain freshness + SLA pass/fail rolled up from watermarks."""

    domain: str
    bronze_freshness_hours: float | None = None
    silver_freshness_hours: float | None = None
    gold_freshness_hours: float | None = None
    bronze_sla_ok: bool = True
    silver_sla_ok: bool = True
    gold_sla_ok: bool = True
    has_warehouse_load: bool = False


class PipelineStatusResponse(BaseModel):
    """Overall pipeline health — all domains."""

    as_of: datetime
    total_domains: int
    domains_ok: int
    domains_warn: int
    sla_miss_count_24h: int
    domains: list[DomainStatusResponse]


class PipelineRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: str
    dag_id: str | None = None
    layer: str | None = None
    status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    rows_written: int | None = None
    error_message: str | None = None
