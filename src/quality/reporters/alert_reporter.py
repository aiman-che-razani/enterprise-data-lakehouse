"""Alert reporter — logs critical DQ failures at ERROR level.

In production this class can be extended to send PagerDuty/Slack/SNS alerts.
For now it provides structured log output and raises on fatal failures when
*fail_on_critical* is set.
"""

from __future__ import annotations

from typing import Any

from src.common.logging import get_logger
from src.quality.checks.base_check import CheckResult, CheckStatus

logger = get_logger(__name__)

_CRITICAL_CHECKS = frozenset(
    {
        "row_count.minimum",  # table is empty or below floor
        "table.load",  # cannot read table at all
        "uniqueness",  # PK violation
    }
)


class AlertReporter:
    """Report DQ alerts via structured logs (and optionally raise).

    Parameters
    ----------
    fail_on_critical: raise RuntimeError if any FAILED critical check is found.
    """

    def __init__(self, fail_on_critical: bool = False) -> None:
        self.fail_on_critical = fail_on_critical

    # ------------------------------------------------------------------
    def report(self, results: list[CheckResult]) -> None:
        critical_failures: list[CheckResult] = []

        for r in results:
            if r.status == CheckStatus.FAILED:
                log_fn = logger.error
                if r.check_name in _CRITICAL_CHECKS:
                    critical_failures.append(r)
            elif r.status == CheckStatus.WARNING:
                log_fn = logger.warning
            elif r.status == CheckStatus.ERROR:
                log_fn = logger.error
            else:
                continue  # PASSED — no alert

            log_fn(
                "dq_alert",
                domain=r.domain,
                layer=r.layer,
                check=r.check_name,
                column=r.column_name,
                status=r.status.value,
                metric=r.metric_value,
                threshold=r.threshold,
                message=r.message,
            )

        if critical_failures and self.fail_on_critical:
            summary = "; ".join(
                f"{r.domain}/{r.check_name}: {r.message}" for r in critical_failures
            )
            raise RuntimeError(f"{len(critical_failures)} critical DQ check(s) FAILED: {summary}")

    # ------------------------------------------------------------------
    def summary(self, results: list[CheckResult]) -> dict[str, Any]:
        counts = {s.value: 0 for s in CheckStatus}
        for r in results:
            counts[r.status.value] += 1
        total = len(results)
        pass_rate = counts["passed"] / total if total else 0.0
        return {
            "total": total,
            "passed": counts["passed"],
            "failed": counts["failed"],
            "warnings": counts["warning"],
            "errors": counts["error"],
            "pass_rate": round(pass_rate, 4),
        }
