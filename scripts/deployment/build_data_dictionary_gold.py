"""
Regenerate the column tables of the Gold section of docs/data-dictionary/DATA_DICTIONARY.md
from warehouse/redshift/ddl/gold_schema.sql (the three warehouses are held identical by tests).

Column names, types and nullability come from the DDL; descriptions are kept from the existing
document when the column still exists (the prose around each table is left alone), and derived
for keys / audit columns otherwise. A test fails if the committed document is out of date.

    python scripts/deployment/build_data_dictionary_gold.py           # rewrite
    python scripts/deployment/build_data_dictionary_gold.py --check   # CI: exit 1 if stale
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "data-dictionary" / "DATA_DICTIONARY.md"
DDL = ROOT / "warehouse" / "redshift" / "ddl" / "gold_schema.sql"
PG_SQL = ROOT / "scripts" / "setup" / "init_postgres.sql"

AUDIT = {
    "_gold_processed_at": "Gold build time (UTC)",
    "_gold_date": "Gold build date",
    "_batch_id": "Batch that produced the row",
    "_pipeline_version": "Pipeline version",
}
HEADER = "| Column | Type | Null | Key | Description |\n|---|---|---|---|---|\n"


def parse_ddl(sql: str) -> dict[str, list[tuple[str, str, bool, bool]]]:
    """table -> [(column, type, not_null, is_pk)] in DDL order."""
    tables: dict[str, list[tuple[str, str, bool, bool]]] = {}
    for m in re.finditer(r"CREATE TABLE IF NOT EXISTS gold\.(\w+)\s*\((.*?)\n\)", sql, re.S):
        body = m.group(2)
        pk = re.search(r"PRIMARY KEY \((\w+)\)", body)
        pk_col = pk.group(1) if pk else None
        cols = []
        for line in body.split("\n"):
            line = line.strip().rstrip(",")
            if not line or line.startswith(("--", "PRIMARY", "UNIQUE", "FOREIGN")):
                continue
            name, typ = line.split()[0], line.split()[1]
            m2 = re.match(r"(DECIMAL|VARCHAR|NUMERIC)\s*\(([^)]*)\)", " ".join(line.split()[1:]))
            if m2:
                typ = f"{m2.group(1)}({m2.group(2).replace(' ', '')})"
            cols.append((name, typ, "NOT NULL" in line.upper(), name == pk_col))
        tables[m.group(1)] = cols
    return tables


def old_descriptions(section: str) -> dict[str, str]:
    """column -> description, from an existing markdown column table (description = last cell)."""
    out = {}
    for line in section.splitlines():
        if not line.startswith("| `"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        name, desc = cells[0].strip("`"), cells[-1]
        if desc and desc != "—":
            out[name] = desc
    return out


# Descriptions that contradicted the DDL or the builders and must not be inherited from the old text.
OVERRIDES = {
    (
        "fact_sales",
        "sk_store_id",
    ): "FK -> `dim_store`; orders without a store hash to one shared key (never NULL)",
    ("fact_shipments", "sk_store_id"): "FK -> `dim_store` (NULL when the order has no store)",
    (
        "fact_shipments",
        "sk_date_id",
    ): "FK -> `dim_date`: ship date, or the record-created date while unshipped",
}


def describe(name: str, old: dict[str, str], table: str = "") -> str:
    if (table, name) in OVERRIDES:
        return OVERRIDES[(table, name)]
    if name in old:
        return old[name]
    if name in AUDIT:
        return AUDIT[name]
    if name.startswith("sk_"):
        return (
            "Surrogate key (xxhash64 of the natural key)"
            if not name.endswith("date_id")
            else "Date key (YYYYMMDD)"
        )
    if name.startswith("nk_"):
        return "Natural key from Silver"
    return "—"


def render_table(cols, old, table: str = "") -> str:
    rows = []
    for name, typ, not_null, is_pk in cols:
        key = (
            "🔑"
            if is_pk
            else ("SK" if name.startswith("sk_") else ("NK" if name.startswith("nk_") else "—"))
        )
        rows.append(
            f"| `{name}` | {typ.lower()} | {'N' if not_null else 'Y'} | {key} | {describe(name, old, table)} |"
        )
    return HEADER + "\n".join(rows) + "\n"


# Corrections to prose that contradicted the DDL / code.
PROSE = [
    ("| `day_of_week` | integer | — | 1=Monday … 7=Sunday |", None),  # handled via table rebuild
    ("**Range:** 2020-01-01 → 2030-12-31 (3,653 rows)", None),
]


def build(doc: str) -> str:
    tables = parse_ddl(DDL.read_text(encoding="utf-8"))
    start = doc.index("## Gold Dimensional Model")
    end = doc.index("## Pipeline Metadata Tables")
    gold = doc[start:end]

    # shared audit columns: four, not three
    gold = re.sub(
        r"All Gold tables share:\n\n\| Column.*?\n\n",
        "All Gold tables share:\n\n| Column | Type | Description |\n|---|---|---|\n"
        + "".join(
            f"| `{c}` | {'timestamp' if c.endswith('_at') else 'date' if c.endswith('_date') else 'string'} | {d} |\n"
            for c, d in AUDIT.items()
        )
        + "\n",
        gold,
        count=1,
        flags=re.S,
    )

    parts = re.split(r"(?m)^(#### \w+\n)", gold)
    out = [parts[0]]
    for i in range(1, len(parts), 2):
        heading, body = parts[i], parts[i + 1]
        table = heading.strip().removeprefix("#### ")
        if table in tables:
            m = re.search(r"(?m)^\| Column.*?\n(?:\|.*\n)+", body)
            assert m, f"no column table under {heading!r}"
            new_table = render_table(tables[table], old_descriptions(m.group(0)), table)
            body = body[: m.start()] + new_table + body[m.end() :]
        out += [heading, body]
    gold = "".join(out)

    # facts described the surrogate/natural key names that do not exist; the DDL is authoritative
    gold = gold.replace("FY2025-Q2", "Q2")
    gold = re.sub(r"1=Monday.{0,3}7=Sunday", "1=Sunday … 7=Saturday (Spark `dayofweek`)", gold)
    return doc[:start] + gold + render_pg()


PG_ORDER = [
    "pipeline.watermarks",
    "pipeline.runs",
    "pipeline.sla_misses",
    "quality.check_results",
    "quality.reports",
    "quality.contract_violations",
    "catalog.tables",
    "catalog.columns",
    "catalog.lineage",
    "api.api_keys",
]
PG_NOTES = {
    "pipeline.watermarks": "One row per domain (16) and per Gold table (14). Each layer writes only its own columns.",
    "pipeline.runs": "Airflow run audit log; `status` is lower-case.",
    "pipeline.sla_misses": "Written by the DQ freshness check and the SLA monitor DAG.",
    "quality.check_results": "`status` is a lower-case CheckStatus (passed / failed / warning / error).",
    "quality.reports": "One row per (run, domain, layer); `pass_rate` is a fraction in [0, 1]. Unique on that key.",
    "quality.contract_violations": "Written by the Silver data-contract enforcer.",
    "catalog.tables": "`table_name` is layer-qualified (`silver.orders`); unique with `schema_name`.",
    "catalog.columns": "Replaced wholesale for a table on every catalog sync.",
    "catalog.lineage": "Table-level edges; unique on (source_table, target_table, job_name).",
    "api.api_keys": "Only the SHA-256 hash of a key is stored. `permissions` holds scopes (`read:pipeline`, `read:quality`, `read:catalog`, or `*`).",
}


def parse_pg(sql: str) -> dict[str, list[tuple[str, str, bool, str]]]:
    """table -> [(column, type, not_null, comment)] for every CREATE TABLE in the Postgres init script."""
    tables = {}
    for m in re.finditer(r"CREATE TABLE IF NOT EXISTS ([\w.]+)\s*\((.*?)\n\);", sql, re.S):
        cols = []
        for line in m.group(2).split("\n"):
            code, _, comment = line.partition("--")
            code = code.strip().rstrip(",")
            if not code or code.upper().startswith(
                ("PRIMARY", "UNIQUE", "CONSTRAINT", "CHECK", "FOREIGN")
            ):
                continue
            parts = code.split()
            typ = parts[1].lower()
            m2 = re.match(r"(\w+\s*\([^)]*\))", " ".join(parts[1:]))
            if m2:
                typ = m2.group(1).lower().replace(" ", "")
            cols.append(
                (
                    parts[0],
                    typ,
                    "NOT NULL" in code.upper() or "PRIMARY KEY" in code.upper(),
                    comment.strip(),
                )
            )
        tables[m.group(1)] = cols
    return tables


def render_pg() -> str:
    tables = parse_pg(PG_SQL.read_text(encoding="utf-8"))
    out = [
        "## Pipeline Metadata Tables\n",
        "\nGenerated from `scripts/setup/init_postgres.sql` (the single definition of the operational schema).\n",
    ]
    for name in PG_ORDER:
        out.append(
            f"\n### {name}\n\n{PG_NOTES[name]}\n\n| Column | Type | Null | Notes |\n|---|---|---|---|\n"
        )
        out.append(
            "".join(
                f"| `{c}` | {t} | {'N' if nn else 'Y'} | {note or '—'} |\n"
                for c, t, nn, note in tables[name]
            )
        )
    return "".join(out)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--check", action="store_true")
    args = p.parse_args(argv)
    current = DOC.read_text(encoding="utf-8").replace("\r\n", "\n")
    new = build(current)
    if args.check:
        if new != current:
            print(
                "DATA_DICTIONARY.md Gold section is stale - run scripts/deployment/build_data_dictionary_gold.py",
                file=sys.stderr,
            )
            return 1
        return 0
    if new != current:
        with open(DOC, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(new)
        print("rewrote", DOC.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    sys.exit(main())
