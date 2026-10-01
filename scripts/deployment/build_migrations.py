"""
Generate each warehouse's table-creation migration from its ddl/gold_schema.sql.

`warehouse/<target>/ddl/gold_schema.sql` is the human-edited snapshot of the Gold schema;
`warehouse/<target>/migrations/V002__create_gold_tables.sql` is what a migration runner
(Flyway, Liquibase, SnowSQL, bq) applies. Keeping the second generated from the first means
they cannot drift — `tests/unit/test_warehouse_schema.py` fails if they do.

Usage:
    python scripts/deployment/build_migrations.py            # rewrite the V002 files
    python scripts/deployment/build_migrations.py --check    # exit 1 if any is stale (CI)

For a later schema change, add V003__*.sql by hand (ALTER TABLE ...) and update ddl/ to match;
never edit an applied migration.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGETS = ("redshift", "snowflake", "bigquery")
NAME = "V002__create_gold_tables.sql"

HEADER = (
    "-- Migration V002: create the Gold star-schema tables ({target}).\n"
    "-- GENERATED from ddl/gold_schema.sql by scripts/deployment/build_migrations.py — do not edit.\n"
    "-- Idempotent: every statement is CREATE ... IF NOT EXISTS, so re-running never drops data.\n\n"
)


def render(target: str) -> str:
    ddl = (ROOT / "warehouse" / target / "ddl" / "gold_schema.sql").read_text(encoding="utf-8")
    return HEADER.format(target=target) + ddl.replace("\r\n", "\n")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--check", action="store_true", help="do not write; fail if a migration is out of date"
    )
    args = p.parse_args(argv)

    stale = []
    for target in TARGETS:
        path = ROOT / "warehouse" / target / "migrations" / NAME
        want = render(target)
        have = path.read_text(encoding="utf-8").replace("\r\n", "\n") if path.exists() else None
        if have != want:
            stale.append(target)
            if not args.check:
                with open(path, "w", encoding="utf-8", newline="\n") as fh:
                    fh.write(want)
                print(f"wrote {path.relative_to(ROOT)}")
    if args.check and stale:
        print(
            f"stale migrations: {', '.join(stale)} — run scripts/deployment/build_migrations.py",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
