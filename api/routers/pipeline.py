"""
Pipeline status and watermark endpoints.

All routes require a valid X-API-Key header.

Routes:
  GET /pipeline/status              — full status rollup across all 16 domains
  GET /pipeline/watermarks          — all watermark rows
  GET /pipeline/watermarks/{domain} — single domain watermark
  GET /pipeline/runs                — recent pipeline runs
  GET /pipeline/sla-misses          — SLA miss events
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from api.dependencies import AuthDep, DbDep, require_scope
from api.models.pipeline import (
    PipelineRunResponse,
    PipelineStatusResponse,
    SlaMissResponse,
    WatermarkResponse,
)
from api.repositories.pipeline_repo import PipelineRepository
from api.services.pipeline_service import PipelineService

router = APIRouter(
    tags=["pipeline"],
    dependencies=[Depends(require_scope("read:pipeline"))],
)

_LAYER = Query(pattern="^(bronze|silver|gold|warehouse)$", description="Filter by layer")


# ─────────────────────────────────────────────────────────────────────────────
@router.get(
    "/status",
    response_model=PipelineStatusResponse,
    summary="Overall pipeline health across all 16 domains",
)
def pipeline_status(conn: DbDep, _auth: AuthDep) -> PipelineStatusResponse:
    return PipelineService(PipelineRepository(conn)).get_pipeline_status()


@router.get(
    "/watermarks",
    response_model=list[WatermarkResponse],
    summary="All domain watermarks (last ingest/process timestamps)",
)
def list_watermarks(conn: DbDep, _auth: AuthDep) -> list[WatermarkResponse]:
    return PipelineService(PipelineRepository(conn)).list_watermarks()


@router.get(
    "/watermarks/{domain}",
    response_model=WatermarkResponse,
    summary="Watermark for a single domain",
)
def get_watermark(domain: str, conn: DbDep, _auth: AuthDep) -> WatermarkResponse:
    result = PipelineService(PipelineRepository(conn)).get_watermark(domain)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Domain '{domain}' not found in pipeline.watermarks",
        )
    return result


@router.get(
    "/runs",
    response_model=list[PipelineRunResponse],
    summary="Recent pipeline run records",
)
def list_runs(
    conn: DbDep,
    _auth: AuthDep,
    layer: Annotated[str | None, _LAYER] = None,
    status_: Annotated[
        str | None, Query(alias="status", description="Filter by run status")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> list[PipelineRunResponse]:
    return PipelineService(PipelineRepository(conn)).list_runs(
        layer=layer, status=status_, limit=limit
    )


@router.get(
    "/sla-misses",
    response_model=list[SlaMissResponse],
    summary="SLA miss events within the look-back window",
)
def list_sla_misses(
    conn: DbDep,
    _auth: AuthDep,
    hours: Annotated[float, Query(ge=0.5, le=720, description="Look-back window in hours")] = 24.0,
    domain: Annotated[str | None, Query()] = None,
    layer: Annotated[str | None, _LAYER] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
) -> list[SlaMissResponse]:
    return PipelineService(PipelineRepository(conn)).list_sla_misses(
        hours=hours, domain=domain, layer=layer, limit=limit
    )
