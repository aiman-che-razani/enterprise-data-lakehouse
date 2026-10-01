"""Business logic layer for pipeline status endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from api.models.pipeline import (
    DomainStatusResponse,
    PipelineRunResponse,
    PipelineStatusResponse,
    SlaMissResponse,
    WatermarkResponse,
)

if TYPE_CHECKING:
    from api.repositories.pipeline_repo import PipelineRepository

# Per-domain SLA thresholds in hours (Bronze layer baseline)
_DOMAIN_SLA_HOURS: dict[str, float] = {
    "clickstream": 2.0,
    "iot_sensors": 2.0,
    "orders": 26.0,
    "payments": 26.0,
    "shipments": 26.0,
    "order_lines": 26.0,
    "customers": 26.0,
    "products": 26.0,
    "suppliers": 26.0,
    "stores": 26.0,
    "inventory": 26.0,
    "employees": 48.0,
    "returns": 48.0,
    "reviews": 48.0,
    "finance": 48.0,
    "campaigns": 48.0,
}
# Silver/Gold get ~4h more headroom than Bronze SLA
_SILVER_BUFFER = 4.0
_GOLD_BUFFER = 8.0


class PipelineService:
    def __init__(self, repo: PipelineRepository) -> None:
        self.repo = repo

    # ── Watermarks ────────────────────────────────────────────────────────────
    def list_watermarks(self) -> list[WatermarkResponse]:
        rows = self.repo.list_watermarks()
        return [WatermarkResponse(**r) for r in rows]

    def get_watermark(self, domain: str) -> WatermarkResponse | None:
        row = self.repo.get_watermark(domain)
        return WatermarkResponse(**row) if row else None

    # ── SLA misses ────────────────────────────────────────────────────────────
    def list_sla_misses(
        self,
        hours: float = 24.0,
        domain: str | None = None,
        layer: str | None = None,
        limit: int = 200,
    ) -> list[SlaMissResponse]:
        rows = self.repo.list_sla_misses(hours=hours, domain=domain, layer=layer, limit=limit)
        return [SlaMissResponse(**r) for r in rows]

    # ── Pipeline runs ─────────────────────────────────────────────────────────
    def list_runs(
        self,
        layer: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[PipelineRunResponse]:
        rows = self.repo.list_runs(layer=layer, status=status, limit=limit)
        return [PipelineRunResponse(**r) for r in rows]

    # ── Status rollup ─────────────────────────────────────────────────────────
    def get_pipeline_status(self) -> PipelineStatusResponse:
        watermarks = self.repo.list_watermarks()
        sla_misses = self.repo.count_sla_misses(hours=24.0)
        now = datetime.now(UTC)
        domain_statuses: list[DomainStatusResponse] = []

        for wm in watermarks:
            domain = wm["domain"]
            bz_sla = _DOMAIN_SLA_HOURS.get(domain, 26.0)
            sv_sla = bz_sla + _SILVER_BUFFER
            gd_sla = bz_sla + _GOLD_BUFFER
            bz_age = self.repo.freshness_hours(wm["bronze_last_ingest"])
            sv_age = self.repo.freshness_hours(wm["silver_last_run"])
            gd_age = self.repo.freshness_hours(wm["gold_last_run"])
            has_load = any(
                wm.get(c) is not None
                for c in ("redshift_last_load", "snowflake_last_load", "bigquery_last_load")
            )
            domain_statuses.append(
                DomainStatusResponse(
                    domain=domain,
                    bronze_freshness_hours=bz_age,
                    silver_freshness_hours=sv_age,
                    gold_freshness_hours=gd_age,
                    bronze_sla_ok=bz_age is None or bz_age <= bz_sla,
                    silver_sla_ok=sv_age is None or sv_age <= sv_sla,
                    gold_sla_ok=gd_age is None or gd_age <= gd_sla,
                    has_warehouse_load=has_load,
                )
            )

        domains_ok = sum(
            1 for d in domain_statuses if d.bronze_sla_ok and d.silver_sla_ok and d.gold_sla_ok
        )
        domains_warn = len(domain_statuses) - domains_ok

        return PipelineStatusResponse(
            as_of=now,
            total_domains=len(domain_statuses),
            domains_ok=domains_ok,
            domains_warn=domains_warn,
            sla_miss_count_24h=sla_misses,
            domains=domain_statuses,
        )
