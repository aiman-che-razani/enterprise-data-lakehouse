"""Generate monitoring/grafana/dashboards/data_quality.json.

    python scripts/monitoring/build_data_quality_dashboard.py [output]   # default: the committed file

A test regenerates the dashboard and compares it with the committed JSON, so edit *this* file
(not the JSON) and re-run it. Thresholds mirror monitoring/prometheus/rules.
"""

import json
import sys
from pathlib import Path

DEFAULT_OUT = (
    Path(__file__).resolve().parents[2]
    / "monitoring"
    / "grafana"
    / "dashboards"
    / "data_quality.json"
)

DS = {"type": "prometheus", "uid": "prometheus"}
THRESH_PASS = {
    "mode": "absolute",
    "steps": [
        {"color": "red", "value": None},
        {"color": "orange", "value": 0.75},
        {"color": "yellow", "value": 0.9},
        {"color": "green", "value": 0.98},
    ],
}
_id = [0]


def nid():
    _id[0] += 1
    return _id[0]


def row(title, y):
    return {
        "collapsed": False,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
        "id": nid(),
        "title": title,
        "type": "row",
    }


def target(expr, legend, instant=False, ref="A"):
    t = {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref}
    if instant:
        t["instant"] = True
    return t


def stat(title, expr, x, y, w=4, h=4, thresholds=THRESH_PASS, unit="percentunit", legend=""):
    return {
        "datasource": DS,
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": thresholds,
                "unit": unit,
                "min": 0,
            }
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": nid(),
        "options": {
            "colorMode": "background",
            "graphMode": "none",
            "justifyMode": "center",
            "reduceOptions": {"calcs": ["lastNotNull"]},
        },
        "targets": [target(expr, legend)],
        "title": title,
        "type": "stat",
    }


def bargauge(title, expr, x, y, w=8, h=10, thresholds=THRESH_PASS, unit="percentunit", vmax=1):
    return {
        "datasource": DS,
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": thresholds,
                "unit": unit,
                "min": 0,
                "max": vmax,
            }
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": nid(),
        "options": {
            "displayMode": "gradient",
            "orientation": "horizontal",
            "reduceOptions": {"calcs": ["lastNotNull"]},
            "showUnfilled": True,
        },
        "targets": [target(expr, "{{domain}}", instant=True)],
        "title": title,
        "type": "bargauge",
    }


def timeseries(title, exprs, x, y, w=12, h=8, unit="percentunit"):
    return {
        "datasource": DS,
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {"lineWidth": 2, "fillOpacity": 8, "showPoints": "never"},
                "unit": unit,
            }
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": nid(),
        "options": {
            "legend": {
                "displayMode": "table",
                "placement": "bottom",
                "calcs": ["lastNotNull", "min"],
            }
        },
        "targets": [target(e, lg, ref=chr(65 + i)) for i, (e, lg) in enumerate(exprs)],
        "title": title,
        "type": "timeseries",
    }


panels = []
y = 0
panels.append(row("Quality score by layer (24h)", y))
y += 1
for i, layer in enumerate(["bronze", "silver", "gold"]):
    panels.append(
        stat(
            f"{layer.title()} pass rate",
            f'retailnexus_dq_pass_rate{{layer="{layer}"}}',
            x=i * 4,
            y=y,
            legend=layer,
        )
    )
panels.append(
    stat(
        "Failed checks (latest run)",
        "sum(retailnexus_dq_domain_failed_checks)",
        x=12,
        y=y,
        thresholds={
            "mode": "absolute",
            "steps": [
                {"color": "green", "value": None},
                {"color": "orange", "value": 1},
                {"color": "red", "value": 10},
            ],
        },
        unit="short",
    )
)
panels.append(
    stat(
        "SLA misses (24h)",
        "retailnexus_sla_misses_24h",
        x=16,
        y=y,
        thresholds={
            "mode": "absolute",
            "steps": [
                {"color": "green", "value": None},
                {"color": "orange", "value": 1},
                {"color": "red", "value": 21},
            ],
        },
        unit="short",
    )
)
panels.append(
    stat(
        "Domains below 90%",
        "count(retailnexus_dq_domain_pass_rate < 0.9) or vector(0)",
        x=20,
        y=y,
        thresholds={
            "mode": "absolute",
            "steps": [
                {"color": "green", "value": None},
                {"color": "orange", "value": 1},
                {"color": "red", "value": 4},
            ],
        },
        unit="short",
    )
)
y += 4

panels.append(row("Score per domain (latest run)", y))
y += 1
for i, layer in enumerate(["bronze", "silver", "gold"]):
    panels.append(
        bargauge(
            f"{layer.title()} — pass rate by domain",
            f'retailnexus_dq_domain_pass_rate{{layer="{layer}"}}',
            x=i * 8,
            y=y,
        )
    )
y += 10

panels.append(row("Failures per domain (latest run)", y))
y += 1
for i, layer in enumerate(["bronze", "silver", "gold"]):
    panels.append(
        bargauge(
            f"{layer.title()} — failed checks",
            f'retailnexus_dq_domain_failed_checks{{layer="{layer}"}}',
            x=i * 8,
            y=y,
            h=8,
            thresholds={
                "mode": "absolute",
                "steps": [
                    {"color": "green", "value": None},
                    {"color": "orange", "value": 1},
                    {"color": "red", "value": 5},
                ],
            },
            unit="short",
            vmax=20,
        )
    )
y += 8

panels.append(row("Trends", y))
y += 1
panels.append(
    timeseries(
        "Pass rate by layer",
        [("retailnexus_dq_pass_rate", "{{layer}}")],
        x=0,
        y=y,
    )
)
panels.append(
    timeseries(
        "Pass rate by domain (filtered by $layer)",
        [('retailnexus_dq_domain_pass_rate{layer=~"$layer"}', "{{layer}}/{{domain}}")],
        x=12,
        y=y,
    )
)

dashboard = {
    "annotations": {"list": []},
    "description": "RetailNexus Enterprise Data Lakehouse - data quality scores per layer and domain",
    "editable": True,
    "fiscalYearStartMonth": 0,
    "graphTooltip": 1,
    "id": None,
    "links": [],
    "refresh": "1m",
    "schemaVersion": 38,
    "tags": ["retailnexus", "data-quality", "lakehouse"],
    "templating": {
        "list": [
            {
                "allValue": ".*",
                "current": {"selected": True, "text": "All", "value": "$__all"},
                "datasource": DS,
                "definition": "label_values(retailnexus_dq_domain_pass_rate, layer)",
                "includeAll": True,
                "multi": True,
                "name": "layer",
                "query": {
                    "query": "label_values(retailnexus_dq_domain_pass_rate, layer)",
                    "refId": "layers",
                },
                "refresh": 2,
                "type": "query",
            }
        ]
    },
    "time": {"from": "now-24h", "to": "now"},
    "timezone": "utc",
    "title": "RetailNexus Data Quality",
    "uid": "retailnexus-data-quality",
    "version": 1,
    "panels": panels,
}

out = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUT
with open(out, "w", encoding="utf-8", newline="\n") as f:
    json.dump(dashboard, f, indent=2)
    f.write("\n")
print("wrote", out, len(panels), "panels")
