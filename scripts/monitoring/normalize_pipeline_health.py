"""
One-off normaliser for monitoring/grafana/dashboards/pipeline_health.json (hand-maintained,
unlike data_quality.json which is generated). Idempotent: running it again changes nothing.

    python scripts/monitoring/normalize_pipeline_health.py
"""

import json
from pathlib import Path

PATH = (
    Path(__file__).resolve().parents[2]
    / "monitoring"
    / "grafana"
    / "dashboards"
    / "pipeline_health.json"
)
DS = {"type": "prometheus", "uid": "prometheus"}

PASS_STEPS = [
    {"color": "red", "value": None},
    {"color": "orange", "value": 0.75},
    {"color": "yellow", "value": 0.9},
    {"color": "green", "value": 0.98},
]

# Every domain's Bronze freshness as % of *its own* SLA, so one threshold scale fits all rows.
# SLA classes mirror monitoring/prometheus/rules/pipeline_alerts.yml.
SLA_CLASSES = [
    ("A", 'domain=~"clickstream|iot_sensors"', 2),
    ("B", 'domain=~"employees|returns|reviews|finance|campaigns"', 48),
    ("C", 'domain!~"clickstream|iot_sensors|employees|returns|reviews|finance|campaigns"', 26),
]

TITLES = {
    1: "SLA misses (24h)",
    2: "DQ pass rate by layer (24h)",
    3: "API request rate",
    4: "API 5xx error rate",
    10: "Bronze freshness (% of each domain's SLA)",
    20: "API latency percentiles",
    21: "API throughput by status code",
    30: "DQ pass rate over time",
    100: "Pipeline overview",
    101: "Domain freshness",
    102: "API performance",
    103: "Data quality trend",
}

d = json.loads(PATH.read_text(encoding="utf-8"))
d["tags"] = ["retailnexus", "pipeline", "lakehouse"]

for p in d["panels"]:
    p["title"] = TITLES[p["id"]]
    if p["type"] == "row":
        continue
    p["datasource"] = DS
    for i, t in enumerate(p.get("targets", [])):
        t["datasource"] = DS
        t["refId"] = chr(65 + i)
        t["expr"] = t["expr"].replace('job="retailnexus-api"', 'job="fastapi"')
        if "legendFormat" in t:
            t["legendFormat"] = t["legendFormat"].replace("{{ ", "{{").replace(" }}", "}}")

    if p["id"] == 1:  # SLA misses: warning alert above 5, critical above 20
        p["fieldConfig"]["defaults"]["thresholds"]["steps"] = [
            {"color": "green", "value": None},
            {"color": "orange", "value": 1},
            {"color": "red", "value": 21},
        ]
        p["fieldConfig"]["defaults"].pop(
            "mappings", None
        )  # the "SLA BREACH" text contradicted the scale
    if p["id"] == 2:  # was a `gauge` with bar-gauge options
        p["type"] = "bargauge"
        p["fieldConfig"]["defaults"].update(
            {
                "thresholds": {"mode": "absolute", "steps": PASS_STEPS},
                "unit": "percentunit",
                "min": 0,
                "max": 1,
            }
        )
        p["options"] = {
            "displayMode": "gradient",
            "orientation": "horizontal",
            "reduceOptions": {"calcs": ["lastNotNull"]},
            "showUnfilled": True,
        }
        p["targets"][0]["instant"] = True
    if p["id"] == 4:  # grouped status label; warning alert fires at 5%
        p["targets"][0][
            "expr"
        ] = 'sum(rate(http_requests_total{job="fastapi",status="5xx"}[5m])) / sum(rate(http_requests_total{job="fastapi"}[5m]))'
        p["fieldConfig"]["defaults"].update(
            {
                "unit": "percentunit",
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                        {"color": "orange", "value": 0.05},
                        {"color": "red", "value": 0.15},
                    ],
                },
            }
        )
    if p["id"] == 10:  # table with one threshold for every domain -> bar gauge in % of SLA
        p["type"] = "bargauge"
        p["fieldConfig"] = {
            "defaults": {
                "color": {"mode": "thresholds"},
                "unit": "percent",
                "min": 0,
                "max": 150,
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                        {"color": "orange", "value": 80},
                        {"color": "red", "value": 100},
                    ],
                },
            }
        }
        p["options"] = {
            "displayMode": "gradient",
            "orientation": "horizontal",
            "reduceOptions": {"calcs": ["lastNotNull"]},
            "showUnfilled": True,
        }
        p.pop("transformations", None)
        p["targets"] = [
            {
                "datasource": DS,
                "refId": ref,
                "instant": True,
                "legendFormat": "{{domain}}",
                "expr": f"100 * retailnexus_pipeline_bronze_freshness_hours{{{sel}}} / {sla}",
            }
            for ref, sel, sla in SLA_CLASSES
        ]

# Overview row: four tiles fill the 24-column grid.
tiles = sorted(
    (p for p in d["panels"] if p["type"] != "row" and p["gridPos"]["y"] == 1),
    key=lambda p: p["gridPos"]["x"],
)
for i, p in enumerate(tiles):
    p["gridPos"].update({"x": i * 6, "w": 6})
d["panels"][-1]["gridPos"]["w"] = 24

with open(PATH, "w", encoding="utf-8", newline="\n") as fh:
    fh.write(json.dumps(d, indent=2) + "\n")
print("normalised", PATH.name)
