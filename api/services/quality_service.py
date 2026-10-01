"""Business logic layer for data quality endpoints."""

from __future__ import annotations

from typing import TYPE_CHECKING

from api.models.quality import (
    CheckResultResponse,
    PaginatedChecks,
    QualityReportResponse,
    QualitySummaryResponse,
    QualityTrendPoint,
)

if TYPE_CHECKING:
    from api.repositories.quality_repo import QualityRepository


class QualityService:
    def __init__(self, repo: QualityRepository) -> None:
        self.repo = repo

    def list_checks(
        self,
        domain: str | None = None,
        layer: str | None = None,
        status: str | None = None,
        hours: float = 24.0,
        limit: int = 100,
        offset: int = 0,
    ) -> PaginatedChecks:
        total, rows = self.repo.list_checks(
            domain=domain,
            layer=layer,
            status=status,
            hours=hours,
            limit=limit,
            offset=offset,
        )
        return PaginatedChecks(
            total=total,
            limit=limit,
            offset=offset,
            items=[CheckResultResponse(**r) for r in rows],
        )

    def list_reports(
        self,
        domain: str | None = None,
        layer: str | None = None,
        hours: float = 24.0,
        limit: int = 200,
    ) -> list[QualityReportResponse]:
        rows = self.repo.list_reports(domain=domain, layer=layer, hours=hours, limit=limit)
        return [QualityReportResponse(**r) for r in rows]

    def summary(self, hours: float = 24.0) -> list[QualitySummaryResponse]:
        rows = self.repo.summary_by_layer(hours=hours)
        return [QualitySummaryResponse(**r) for r in rows]

    def trend(
        self,
        domain: str | None = None,
        layer: str | None = None,
        hours: float = 48.0,
        limit: int = 1000,
    ) -> list[QualityTrendPoint]:
        rows = self.repo.trend(domain=domain, layer=layer, hours=hours, limit=limit)
        return [QualityTrendPoint(**r) for r in rows]
