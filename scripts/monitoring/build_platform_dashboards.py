"""
Generate the platform dashboards: API health, Kafka health, Spark jobs and Infrastructure.

    python scripts/monitoring/build_platform_dashboards.py [output-dir]   # default: monitoring/grafana/dashboards

A test regenerates them and compares with the committed JSON, so edit this file (not the JSON) and
re-run it. Metric names were checked against the real exporters:
  * FastAPI           prometheus-fastapi-instrumentator (+ retailnexus_* gauges in api/main.py)
  * Spark master      PrometheusServlet (configs/spark/metrics.properties): metrics_master_*_Value
  * Kafka             danielqsj/kafka-exporter: kafka_brokers, kafka_consumergroup_lag, kafka_topic_*
  * MinIO             /minio/v2/metrics/cluster: minio_cluster_capacity_usable_*, minio_cluster_health_status
  * Containers        cAdvisor: container_cpu_usage_seconds_total, container_memory_working_set_bytes
  * Airflow           StatsD -> statsd-exporter: airflow_scheduler_heartbeat, airflow_ti_failures
"""

import json
import sys
from pathlib import Path

DEFAULT_OUT = Path(__file__).resolve().parents[2] / "monitoring" / "grafana" / "dashboards"
DS = {"type": "prometheus", "uid": "prometheus"}

GREEN_ORANGE_RED = lambda o, r: {  # noqa: E731
    "mode": "absolute",
    "steps": [
        {"color": "green", "value": None},
        {"color": "orange", "value": o},
        {"color": "red", "value": r},
    ],
}
UP_DOWN = {
    "mode": "absolute",
    "steps": [{"color": "red", "value": None}, {"color": "green", "value": 1}],
}
FREE_SPACE = {
    "mode": "absolute",
    "steps": [
        {"color": "red", "value": None},
        {"color": "orange", "value": 0.15},
        {"color": "green", "value": 0.3},
    ],
}


class Builder:
    def __init__(self, uid, title, description, tags):
        self.uid, self.title, self.description, self.tags = uid, title, description, tags
        self.panels, self._id, self._y = [], 0, 0

    def _nid(self):
        self._id += 1
        return self._id

    def row(self, title):
        self.panels.append(
            {
                "collapsed": False,
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": self._y},
                "id": self._nid(),
                "title": title,
                "type": "row",
            }
        )
        self._y += 1

    @staticmethod
    def _targets(queries, instant):
        out = []
        for i, q in enumerate(queries):
            expr, legend = q if isinstance(q, tuple) else (q, "")
            t = {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": chr(65 + i)}
            if instant:
                t["instant"] = True
            out.append(t)
        return out

    def stat(
        self, title, expr, x, w=4, h=4, thresholds=None, unit="short", vmin=None, mappings=None
    ):
        d = {
            "color": {"mode": "thresholds"},
            "thresholds": thresholds or GREEN_ORANGE_RED(1, 5),
            "unit": unit,
        }
        if vmin is not None:
            d["min"] = vmin
        if mappings:
            d["mappings"] = mappings
        self.panels.append(
            {
                "datasource": DS,
                "fieldConfig": {"defaults": d},
                "gridPos": {"h": h, "w": w, "x": x, "y": self._y},
                "id": self._nid(),
                "options": {
                    "colorMode": "background",
                    "graphMode": "none",
                    "justifyMode": "center",
                    "reduceOptions": {"calcs": ["lastNotNull"]},
                },
                "targets": self._targets([expr], False),
                "title": title,
                "type": "stat",
            }
        )
        return self

    def bargauge(self, title, queries, x, w=12, h=8, thresholds=None, unit="short", vmax=None):
        d = {
            "color": {"mode": "thresholds"},
            "thresholds": thresholds or GREEN_ORANGE_RED(1, 5),
            "unit": unit,
            "min": 0,
        }
        if vmax is not None:
            d["max"] = vmax
        self.panels.append(
            {
                "datasource": DS,
                "fieldConfig": {"defaults": d},
                "gridPos": {"h": h, "w": w, "x": x, "y": self._y},
                "id": self._nid(),
                "options": {
                    "displayMode": "gradient",
                    "orientation": "horizontal",
                    "reduceOptions": {"calcs": ["lastNotNull"]},
                    "showUnfilled": True,
                },
                "targets": self._targets(queries, True),
                "title": title,
                "type": "bargauge",
            }
        )
        return self

    def timeseries(self, title, queries, x, w=12, h=8, unit="short"):
        self.panels.append(
            {
                "datasource": DS,
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "unit": unit,
                        "custom": {"lineWidth": 2, "fillOpacity": 8, "showPoints": "never"},
                    }
                },
                "gridPos": {"h": h, "w": w, "x": x, "y": self._y},
                "id": self._nid(),
                "options": {
                    "legend": {
                        "displayMode": "table",
                        "placement": "bottom",
                        "calcs": ["lastNotNull", "max"],
                    }
                },
                "targets": self._targets(queries, False),
                "title": title,
                "type": "timeseries",
            }
        )
        return self

    def advance(self, h):
        self._y += h

    def build(self):
        return {
            "annotations": {"list": []},
            "description": self.description,
            "editable": True,
            "fiscalYearStartMonth": 0,
            "graphTooltip": 1,
            "id": None,
            "links": [],
            "refresh": "1m",
            "schemaVersion": 38,
            "tags": self.tags,
            "templating": {"list": []},
            "time": {"from": "now-6h", "to": "now"},
            "timezone": "utc",
            "title": self.title,
            "uid": self.uid,
            "version": 1,
            "panels": self.panels,
        }


def api_health():
    b = Builder(
        "retailnexus-api-health",
        "RetailNexus API health",
        "FastAPI availability, traffic, errors and latency",
        ["retailnexus", "api", "lakehouse"],
    )
    b.row("Service overview")
    b.stat(
        "API up",
        'up{job="fastapi"}',
        0,
        thresholds=UP_DOWN,
        mappings=[{"type": "value", "options": {"0": {"text": "DOWN"}, "1": {"text": "UP"}}}],
        vmin=0,
    )
    b.stat(
        "Request rate",
        'sum(rate(http_requests_total{job="fastapi"}[5m]))',
        4,
        unit="reqps",
        thresholds=GREEN_ORANGE_RED(50, 200),
    )
    b.stat(
        "Server error rate (5xx)",
        'sum(rate(http_requests_total{job="fastapi",status="5xx"}[5m])) / sum(rate(http_requests_total{job="fastapi"}[5m]))',
        8,
        unit="percentunit",
        thresholds=GREEN_ORANGE_RED(0.05, 0.15),
    )
    b.stat(
        "Latency p99",
        'histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{job="fastapi"}[5m])))',
        12,
        unit="s",
        thresholds=GREEN_ORANGE_RED(1, 2),
    )
    b.stat(
        "Rate-limited or rejected (1h)",
        'sum(increase(http_requests_total{job="fastapi",status="4xx"}[1h]))',
        16,
        thresholds=GREEN_ORANGE_RED(50, 500),
    )
    b.stat(
        "In-flight requests",
        "sum(retailnexus_api_inprogress)",
        20,
        thresholds=GREEN_ORANGE_RED(20, 50),
    )
    b.advance(4)
    b.row("Traffic")
    b.timeseries(
        "Throughput by status class",
        [('sum by (status) (rate(http_requests_total{job="fastapi"}[5m]))', "{{status}}")],
        0,
        unit="reqps",
    )
    b.timeseries(
        "Requests by endpoint",
        [('sum by (handler) (rate(http_requests_total{job="fastapi"}[5m]))', "{{handler}}")],
        12,
        unit="reqps",
    )
    b.advance(8)
    b.row("Latency")
    b.timeseries(
        "Latency percentiles",
        [
            (
                f'histogram_quantile({q}, sum by (le) (rate(http_request_duration_seconds_bucket{{job="fastapi"}}[5m])))',
                f"p{int(q * 100)}",
            )
            for q in (0.5, 0.95, 0.99)
        ],
        0,
        unit="s",
    )
    b.timeseries(
        "Latency p95 by endpoint",
        [
            (
                'histogram_quantile(0.95, sum by (le, handler) (rate(http_request_duration_seconds_bucket{job="fastapi"}[5m])))',
                "{{handler}}",
            )
        ],
        12,
        unit="s",
    )
    b.advance(8)
    return b.build()


def kafka_health():
    b = Builder(
        "retailnexus-kafka-health",
        "RetailNexus Kafka health",
        "Broker availability, consumer-group lag and topic throughput",
        ["retailnexus", "kafka", "lakehouse"],
    )
    grp = 'consumergroup=~"lakehouse.*|retailnexus.*"'
    b.row("Cluster overview")
    b.stat("Brokers", "kafka_brokers", 0, thresholds=UP_DOWN, vmin=0)
    b.stat(
        "Total consumer lag",
        f"sum(kafka_consumergroup_lag{{{grp}}})",
        4,
        thresholds=GREEN_ORANGE_RED(10000, 100000),
    )
    b.stat(
        "Under-replicated partitions",
        "sum(kafka_topic_partition_under_replicated_partition)",
        8,
        thresholds=GREEN_ORANGE_RED(1, 3),
    )
    b.stat(
        "Topics with partitions",
        "count(kafka_topic_partitions)",
        12,
        thresholds={"mode": "absolute", "steps": [{"color": "blue", "value": None}]},
    )
    b.stat(
        "Consumer group members",
        f"sum(kafka_consumergroup_members{{{grp}}})",
        16,
        thresholds=UP_DOWN,
        vmin=0,
    )
    b.stat("Kafka exporter up", 'up{job="kafka"}', 20, thresholds=UP_DOWN, vmin=0)
    b.advance(4)
    b.row("Consumer lag")
    b.bargauge(
        "Lag by consumer group and topic (latest)",
        [
            (
                f"sum by (consumergroup, topic) (kafka_consumergroup_lag{{{grp}}})",
                "{{consumergroup}} / {{topic}}",
            )
        ],
        0,
        thresholds=GREEN_ORANGE_RED(10000, 100000),
    )
    b.timeseries(
        "Lag over time",
        [(f"sum by (consumergroup) (kafka_consumergroup_lag{{{grp}}})", "{{consumergroup}}")],
        12,
    )
    b.advance(8)
    b.row("Throughput")
    b.timeseries(
        "Messages in per topic",
        [("sum by (topic) (rate(kafka_topic_partition_current_offset[5m]))", "{{topic}}")],
        0,
        unit="ops",
    )
    b.timeseries(
        "Messages consumed per group",
        [
            (
                f"sum by (consumergroup) (rate(kafka_consumergroup_current_offset{{{grp}}}[5m]))",
                "{{consumergroup}}",
            )
        ],
        12,
        unit="ops",
    )
    b.advance(8)
    return b.build()


def spark_jobs():
    b = Builder(
        "retailnexus-spark-jobs",
        "RetailNexus Spark jobs",
        "Spark master, workers and application queue",
        ["retailnexus", "spark", "lakehouse"],
    )
    b.row("Cluster overview")
    b.stat("Master up", 'up{job="spark-master"}', 0, thresholds=UP_DOWN, vmin=0)
    b.stat("Alive workers", "metrics_master_aliveWorkers_Value", 4, thresholds=UP_DOWN, vmin=0)
    b.stat(
        "Registered workers",
        "metrics_master_workers_Value",
        8,
        thresholds={"mode": "absolute", "steps": [{"color": "blue", "value": None}]},
    )
    b.stat(
        "Running applications",
        "metrics_master_apps_Value",
        12,
        thresholds={"mode": "absolute", "steps": [{"color": "blue", "value": None}]},
    )
    b.stat(
        "Waiting applications",
        "metrics_master_waitingApps_Value",
        16,
        thresholds=GREEN_ORANGE_RED(1, 3),
    )
    b.stat(
        "Workers lost",
        "metrics_master_workers_Value - metrics_master_aliveWorkers_Value",
        20,
        thresholds=GREEN_ORANGE_RED(1, 2),
    )
    b.advance(4)
    b.row("Capacity over time")
    b.timeseries(
        "Workers",
        [
            ("metrics_master_workers_Value", "registered"),
            ("metrics_master_aliveWorkers_Value", "alive"),
        ],
        0,
    )
    b.timeseries(
        "Applications",
        [("metrics_master_apps_Value", "running"), ("metrics_master_waitingApps_Value", "waiting")],
        12,
    )
    b.advance(8)
    b.row("Pipeline output freshness")
    b.bargauge(
        "Bronze data age by domain (hours)",
        [("retailnexus_pipeline_bronze_freshness_hours", "{{domain}}")],
        0,
        w=24,
        h=10,
        thresholds=GREEN_ORANGE_RED(26, 48),
        unit="h",
        vmax=72,
    )
    b.advance(10)
    return b.build()


def infrastructure():
    b = Builder(
        "retailnexus-infrastructure",
        "RetailNexus infrastructure",
        "Scrape targets, containers, object storage and the Airflow scheduler",
        ["retailnexus", "infrastructure", "lakehouse"],
    )
    ctr = 'name=~"lakehouse-.*"'
    b.row("Availability")
    b.stat(
        "Targets up",
        "sum(up)",
        0,
        thresholds={"mode": "absolute", "steps": [{"color": "blue", "value": None}]},
    )
    b.stat("Targets down", "count(up == 0) or vector(0)", 4, thresholds=GREEN_ORANGE_RED(1, 2))
    b.stat(
        "MinIO free space",
        "minio_cluster_capacity_usable_free_bytes / minio_cluster_capacity_usable_total_bytes",
        8,
        unit="percentunit",
        thresholds=FREE_SPACE,
        vmin=0,
    )
    b.stat("MinIO healthy", "minio_cluster_health_status", 12, thresholds=UP_DOWN, vmin=0)
    b.stat(
        "MinIO drives online", "minio_cluster_drive_online_total", 16, thresholds=UP_DOWN, vmin=0
    )
    b.stat(
        "Airflow scheduler heartbeats/min",
        "rate(airflow_scheduler_heartbeat[5m]) * 60",
        20,
        thresholds=UP_DOWN,
        vmin=0,
    )
    b.advance(4)
    b.bargauge(
        "Scrape target status (1 = up)",
        [("up", "{{job}}")],
        0,
        w=24,
        h=6,
        thresholds=UP_DOWN,
        vmax=1,
    )
    b.advance(6)
    b.row("Containers")
    b.timeseries(
        "CPU by container",
        [(f"sum by (name) (rate(container_cpu_usage_seconds_total{{{ctr}}}[5m]))", "{{name}}")],
        0,
        unit="percentunit",
    )
    b.timeseries(
        "Memory by container",
        [(f"sum by (name) (container_memory_working_set_bytes{{{ctr}}})", "{{name}}")],
        12,
        unit="bytes",
    )
    b.advance(8)
    b.row("Object storage and orchestration")
    b.timeseries(
        "MinIO usable capacity",
        [
            ("minio_cluster_capacity_usable_free_bytes", "free"),
            ("minio_cluster_capacity_usable_total_bytes", "total"),
        ],
        0,
        unit="bytes",
    )
    b.timeseries(
        "Airflow task outcomes (per hour)",
        [
            ("sum(increase(airflow_ti_successes[1h]))", "succeeded"),
            ("sum(increase(airflow_ti_failures[1h]))", "failed"),
        ],
        12,
    )
    b.advance(8)
    return b.build()


DASHBOARDS = {
    "api_health.json": api_health,
    "kafka_health.json": kafka_health,
    "spark_jobs.json": spark_jobs,
    "infrastructure.json": infrastructure,
}


def main() -> None:
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUT
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, fn in DASHBOARDS.items():
        with open(out_dir / name, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(fn(), fh, indent=2)
            fh.write("\n")
        print("wrote", out_dir / name)


if __name__ == "__main__":
    main()
