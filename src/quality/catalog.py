"""
The catalogue of data-quality checks the platform defines, derived from the per-domain configs.

Each engine (`bronze_dq`, `silver_dq`, `gold_dq`) turns a `DomainCheckConfig` into concrete checks at
run time. This module enumerates the same checks statically - one entry per (layer, table, check, column)
- so "how many checks do we have, and where?" has a checkable answer without a Spark session.

    python -m src.quality.catalog            # summary table
    python -m src.quality.catalog --list     # every check
"""

from __future__ import annotations

import sys
from collections import Counter
from dataclasses import dataclass

from src.quality.domain_config import (
    BRONZE_CONFIGS,
    GOLD_CONFIGS,
    SILVER_CONFIGS,
    DomainCheckConfig,
)

_CONFIGS: dict[str, dict[str, DomainCheckConfig]] = {
    "bronze": BRONZE_CONFIGS,
    "silver": SILVER_CONFIGS,
    "gold": GOLD_CONFIGS,
}


@dataclass(frozen=True)
class DefinedCheck:
    layer: str
    table: str
    check: str  # row_count | completeness | freshness | uniqueness | value_range | referential
    column: str | None = None
    severity: str = "standard"  # "critical" checks halt the pipeline (see AlertReporter)


_CRITICAL = {"row_count", "uniqueness"}


def _for_config(layer: str, table: str, cfg: DomainCheckConfig) -> list[DefinedCheck]:
    def mk(check: str, column: str | None = None) -> DefinedCheck:
        return DefinedCheck(
            layer, table, check, column, "critical" if check in _CRITICAL else "standard"
        )

    out = [mk("row_count")]
    out += [mk("completeness", c) for columns, _rate in cfg.completeness_plan(0.0) for c in columns]
    if cfg.timestamp_col:
        out.append(mk("freshness", cfg.timestamp_col))
    if cfg.pk_col:
        out.append(mk("uniqueness", cfg.pk_col))
    out += [mk("value_range", col) for col in cfg.numeric_ranges]
    out += [mk("referential", fk_col) for fk_col, _parent, _pk in cfg.fk_checks]
    if layer == "gold" and table.startswith("fact_"):
        out.append(
            mk("completeness", "sk_date_id")
        )  # extra guard the Gold engine adds on every fact
    return out


def defined_checks() -> list[DefinedCheck]:
    checks: list[DefinedCheck] = []
    for layer, configs in _CONFIGS.items():
        for table, cfg in configs.items():
            checks.extend(_for_config(layer, table, cfg))
    return checks


def summary() -> dict[str, dict[str, int]]:
    """layer -> {check kind -> count, "total": n, "tables": n}"""
    checks = defined_checks()
    out: dict[str, dict[str, int]] = {}
    for layer in _CONFIGS:
        mine = [c for c in checks if c.layer == layer]
        kinds = Counter(c.check for c in mine)
        out[layer] = {**kinds, "total": len(mine), "tables": len({c.table for c in mine})}
    return out


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if "--list" in args:
        for c in defined_checks():
            print(f"{c.layer:7} {c.table:18} {c.check:13} {c.column or '-':28} {c.severity}")
        return 0
    s = summary()
    kinds = ["row_count", "completeness", "freshness", "uniqueness", "value_range", "referential"]
    print(f"{'layer':8}{'tables':>7}" + "".join(f"{k:>14}" for k in kinds) + f"{'TOTAL':>8}")
    for layer, row in s.items():
        print(
            f"{layer:8}{row['tables']:>7}"
            + "".join(f"{row.get(k, 0):>14}" for k in kinds)
            + f"{row['total']:>8}"
        )
    print(
        f"{'ALL':8}{sum(r['tables'] for r in s.values()):>7}{'':>{14 * len(kinds)}}{sum(r['total'] for r in s.values()):>8}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
