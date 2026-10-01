"""
Data quality endpoints.

Routes:
  GET /quality/summary           — aggregated pass rates by layer
  GET /quality/reports           — per-domain DQ report rows
  GET /quality/checks            — individual check results (paginated)
  GET /quality/trend             — pass-rate time series for chart panels
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from api.dependencies import AuthDep, DbDep, require_scope
from api.models.quality import (
    CheckStatusEnum,
    PaginatedChecks,
    QualityReportResponse,
    QualitySummaryResponse,
    QualityTrendPoint,
)
from api.repositories.quality_repo import QualityRepository
from api.services.quality_service import QualityService

router = APIRouter(
    tags=["quality"],
    dependencies=[Depends(require_scope("read:quality"))],
)

_LAYER = Query(pattern="^(bronze|silver|gold)$", description="Filter by layer")


@router.get(
    "/summary",
    response_model=list[QualitySummaryResponse],
    summary="Aggregated DQ pass rates per layer",
)
def quality_summary(
    conn: DbDep,
    _auth: AuthDep,
    hours: Annotated[float, Query(ge=1, le=720, description="Look-back window")] = 24.0,
) -> list[QualitySummaryResponse]:
    return QualityService(QualityRepository(conn)).summary(hours=hours)


@router.get(
    "/reports",
    response_model=list[QualityReportResponse],
    summary="Per-domain DQ report rows",
)
def list_reports(
    conn: DbDep,
    _auth: AuthDep,
    domain: Annotated[str | None, Query(description="Filter by domain")] = None,
    layer: Annotated[str | None, _LAYER] = None,
    hours: Annotated[float, Query(ge=1, le=720)] = 24.0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
) -> list[QualityReportResponse]:
    return QualityService(QualityRepository(conn)).list_reports(
        domain=domain, layer=layer, hours=hours, limit=limit
    )


@router.get(
    "/checks",
    response_model=PaginatedChecks,
    summary="Individual DQ check results (paginated)",
)
def list_checks(
    conn: DbDep,
    _auth: AuthDep,
    domain: Annotated[str | None, Query()] = None,
    layer: Annotated[str | None, _LAYER] = None,
    status: Annotated[
        CheckStatusEnum | None,
        Query(description="Filter by check status: passed|failed|warning|error"),
    ] = None,
    hours: Annotated[float, Query(ge=1, le=720)] = 24.0,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> PaginatedChecks:
    return QualityService(QualityRepository(conn)).list_checks(
        domain=domain,
        layer=layer,
        status=status.value if status else None,
        hours=hours,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/trend",
    response_model=list[QualityTrendPoint],
    summary="DQ pass-rate time series for charting",
)
def quality_trend(
    conn: DbDep,
    _auth: AuthDep,
    domain: Annotated[str | None, Query()] = None,
    layer: Annotated[str | None, _LAYER] = None,
    hours: Annotated[float, Query(ge=1, le=720)] = 48.0,
    limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
) -> list[QualityTrendPoint]:
    return QualityService(QualityRepository(conn)).trend(
        domain=domain, layer=layer, hours=hours, limit=limit
    )
