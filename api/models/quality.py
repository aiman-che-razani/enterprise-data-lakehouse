"""Pydantic v2 response models for data quality endpoints."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict


class CheckStatusEnum(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    ERROR = "error"


class CheckResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: str
    domain: str
    layer: str
    check_name: str
    column_name: str | None = None
    status: CheckStatusEnum
    metric_value: float | None = None
    threshold: float | None = None
    message: str | None = None
    rows_checked: int | None = None
    run_at: datetime


class QualityReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: str
    domain: str
    layer: str
    total_checks: int
    passed: int
    failed: int
    warnings: int
    errors: int
    pass_rate: float
    run_at: datetime


class QualitySummaryResponse(BaseModel):
    """Aggregated DQ metrics per layer for the last N hours."""

    layer: str
    period_hours: float
    total_checks: int
    passed: int
    failed: int
    warnings: int
    errors: int
    pass_rate: float
    domains_checked: int


class QualityTrendPoint(BaseModel):
    """Single data point for a pass-rate trend series."""

    run_at: datetime
    layer: str
    domain: str
    pass_rate: float
    failed: int


class PaginatedChecks(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[CheckResultResponse]
