"""Generate Kibana saved objects (ndjson) for the lakehouse log indices.

    python scripts/monitoring/build_kibana_objects.py [output]   # default: the committed file

A test regenerates the objects and compares them with the committed ndjson - edit this file, not the ndjson.
Column order follows scope -> layer -> domain -> check -> message.
"""

import json
import sys
from pathlib import Path

DEFAULT_OUT = (
    Path(__file__).resolve().parents[2]
    / "monitoring"
    / "elk"
    / "kibana"
    / "lakehouse_saved_objects.ndjson"
)

DV_ID = "lakehouse-logs"


def search(sid, title, description, query, columns, sort=None):
    return {
        "id": sid,
        "type": "search",
        "attributes": {
            "title": title,
            "description": description,
            "columns": columns,
            "sort": sort or [["@timestamp", "desc"]],
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {
                        "query": {"language": "kuery", "query": query},
                        "filter": [],
                        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    }
                )
            },
        },
        "references": [
            {
                "id": DV_ID,
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern",
            }
        ],
    }


objects = [
    {
        "id": DV_ID,
        "type": "index-pattern",
        "attributes": {
            "title": "lakehouse-*",
            "timeFieldName": "@timestamp",
            "name": "Lakehouse logs",
        },
        "references": [],
    },
    search(
        "lakehouse-dq-alerts",
        "DQ alerts: failed and warning checks",
        "Every non-passing data-quality check logged by the DQ runner (event dq_alert), newest first.",
        'event : "dq_alert"',
        [
            "environment",
            "layer",
            "domain",
            "check",
            "column",
            "message",
            "status",
            "metric",
            "threshold",
        ],
    ),
    search(
        "lakehouse-dq-critical",
        "DQ alerts: failed checks (ERROR level)",
        "dq_alert events logged at ERROR: FAILED or errored checks. Critical checks (row count, uniqueness) halt the pipeline.",
        'event : "dq_alert" and level : "ERROR"',
        ["environment", "layer", "domain", "check", "column", "message"],
    ),
    search(
        "lakehouse-contract-violations",
        "Data contract violations",
        "Silver data-contract violations (event contract_violation); severity error fails the run in enforce mode.",
        'event : "contract_violation"',
        ["environment", "domain", "rule", "column", "severity", "message"],
    ),
    search(
        "lakehouse-errors",
        "All ERROR logs",
        "Every ERROR or CRITICAL log line from any service.",
        'level : "ERROR" or level : "CRITICAL"',
        ["environment", "service", "logger", "event", "error"],
    ),
    search(
        "lakehouse-pipeline-failures",
        "Pipeline job failures",
        "Failure events from the Bronze, Silver, Gold, warehouse, maintenance, catalog and DQ jobs, including checkpoint (watermark) errors.",
        'event : ("silver_failed" or "bronze_job_partial_failure" or "gold_build_failed" or "warehouse_load_failed" or "watermark_write_failed" or "watermark_read_failed" or "maintenance_failed" or "catalog_sync_failed" or "dq_run_error")',
        ["environment", "layer", "domain", "table", "event", "error"],
    ),
]

panels = []
refs = []
for i, sid in enumerate(
    [
        "lakehouse-dq-alerts",
        "lakehouse-dq-critical",
        "lakehouse-contract-violations",
        "lakehouse-errors",
        "lakehouse-pipeline-failures",
    ]
):
    name = f"panel_{i}"
    panels.append(
        {
            "version": "8.13.4",
            "type": "search",
            "gridData": {
                "x": 0 if i % 2 == 0 else 24,
                "y": (i // 2) * 15,
                "w": 24,
                "h": 15,
                "i": str(i),
            },
            "panelIndex": str(i),
            "embeddableConfig": {},
            "panelRefName": name,
        }
    )
    refs.append({"name": name, "type": "search", "id": sid})

objects.append(
    {
        "id": "lakehouse-data-quality-logs",
        "type": "dashboard",
        "attributes": {
            "title": "Lakehouse: data quality and errors",
            "description": "DQ alerts, contract violations and pipeline failures shipped via Logstash",
            "hits": 0,
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps(
                {"useMargins": True, "syncColors": False, "hidePanelTitles": False}
            ),
            "timeRestore": True,
            "timeFrom": "now-24h",
            "timeTo": "now",
            "refreshInterval": {"pause": False, "value": 60000},
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {"query": {"query": "", "language": "kuery"}, "filter": []}
                )
            },
        },
        "references": refs,
    }
)

out = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUT
with open(out, "w", encoding="utf-8", newline="\n") as f:
    for o in objects:
        f.write(json.dumps(o) + "\n")
print("wrote", len(objects), "objects")
