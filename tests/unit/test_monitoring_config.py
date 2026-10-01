"""Static validation of monitoring configuration: Prometheus rules, Alertmanager, Grafana, Kibana, compose mounts."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_RULES = sorted((_ROOT / "monitoring" / "prometheus" / "rules").glob("*.yml"))
_DASHBOARDS = sorted((_ROOT / "monitoring" / "grafana" / "dashboards").glob("*.json"))
_API_MAIN = (_ROOT / "api" / "main.py").read_text(encoding="utf-8")

# Custom gauges exported by the API — the only retailnexus_* series that exist.
_API_METRICS = set(re.findall(r'"(retailnexus_\w+)"', _API_MAIN))
_METRIC_RE = re.compile(r"\bretailnexus_\w+")


def _rules() -> list[tuple[str, dict]]:
    out = []
    for path in _RULES:
        for group in yaml.safe_load(path.read_text(encoding="utf-8"))["groups"]:
            for rule in group["rules"]:
                out.append((path.name, rule))
    return out


class TestPrometheusRules:
    def test_rule_files_present(self):
        assert {p.name for p in _RULES} >= {"pipeline_alerts.yml", "data_quality_alerts.yml"}

    @pytest.mark.parametrize(
        "fname,rule", _rules(), ids=lambda v: v if isinstance(v, str) else v.get("alert")
    )
    def test_rule_is_well_formed(self, fname, rule):
        assert rule["alert"] and rule["expr"]
        assert rule["labels"]["severity"] in {"warning", "critical", "info"}
        assert rule["annotations"]["summary"]

    def test_alert_names_are_unique(self):
        names = [r["alert"] for _, r in _rules()]
        assert len(names) == len(set(names))

    def test_custom_metrics_referenced_by_rules_exist(self):
        for fname, rule in _rules():
            unknown = set(_METRIC_RE.findall(rule["expr"])) - _API_METRICS
            assert (
                not unknown
            ), f"{fname}:{rule['alert']} uses undefined metric(s) {sorted(unknown)}"

    def test_api_exports_domain_dq_gauges(self):
        assert {
            "retailnexus_dq_domain_pass_rate",
            "retailnexus_dq_domain_failed_checks",
        } <= _API_METRICS

    def test_prometheus_targets_alertmanager(self):
        cfg = yaml.safe_load(
            (_ROOT / "configs" / "prometheus" / "prometheus.yml").read_text(encoding="utf-8")
        )
        targets = cfg["alerting"]["alertmanagers"][0]["static_configs"][0]["targets"]
        assert targets == ["alertmanager:9093"]


class TestAlertmanager:
    @pytest.mark.parametrize("fname", ["alertmanager.yml", "alertmanager.prod.yml"])
    def test_routes_reference_defined_receivers(self, fname):
        cfg = yaml.safe_load((_ROOT / "monitoring" / "alerts" / fname).read_text(encoding="utf-8"))
        defined = {r["name"] for r in cfg["receivers"]}
        used = {cfg["route"]["receiver"]}

        def collect(route):
            for sub in route.get("routes", []):
                used.add(sub["receiver"])
                collect(sub)

        collect(cfg["route"])
        assert used <= defined, f"undefined receivers: {used - defined}"

    def test_prod_config_reads_secrets_from_files_not_inline(self):
        text = (_ROOT / "monitoring" / "alerts" / "alertmanager.prod.yml").read_text(
            encoding="utf-8"
        )
        assert "api_url_file" in text and "routing_key_file" in text
        assert "hooks.slack.com" not in text and "api_url:" not in text

    def test_critical_alerts_page_oncall(self):
        cfg = yaml.safe_load(
            (_ROOT / "monitoring" / "alerts" / "alertmanager.prod.yml").read_text(encoding="utf-8")
        )
        paging = [r for r in cfg["route"]["routes"] if r["receiver"] == "pagerduty-oncall"]
        assert paging and "critical" in str(paging[0]["matchers"])


class TestGrafana:
    def test_datasource_uid_is_pinned(self):
        cfg = yaml.safe_load(
            (
                _ROOT / "configs" / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
            ).read_text(encoding="utf-8")
        )
        assert cfg["datasources"][0]["uid"] == "prometheus"

    def test_dashboards_exist(self):
        assert {p.name for p in _DASHBOARDS} >= {
            "pipeline_health.json",
            "data_quality.json",
            "api_health.json",
            "kafka_health.json",
            "spark_jobs.json",
            "infrastructure.json",
        }

    @pytest.mark.parametrize("path", _DASHBOARDS, ids=lambda p: p.name)
    def test_dashboard_is_provisionable(self, path):
        dash = json.loads(path.read_text(encoding="utf-8"))
        assert "__inputs" not in dash, "file provisioning does not resolve __inputs placeholders"
        assert dash["uid"] and dash["title"]
        ids = [p["id"] for p in dash["panels"]]
        assert len(ids) == len(set(ids)), "duplicate panel ids"

    @pytest.mark.parametrize("path", _DASHBOARDS, ids=lambda p: p.name)
    def test_panels_use_the_provisioned_datasource_and_known_metrics(self, path):
        dash = json.loads(path.read_text(encoding="utf-8"))
        raw = path.read_text(encoding="utf-8")
        assert "${DS_PROMETHEUS}" not in raw and "${datasource}" not in raw
        for panel in dash["panels"]:
            if panel["type"] == "row":
                continue
            assert panel["datasource"]["uid"] == "prometheus", panel["title"]
            for t in panel.get("targets", []):
                assert t["datasource"]["uid"] == "prometheus"
                unknown = set(_METRIC_RE.findall(t["expr"])) - _API_METRICS
                assert (
                    not unknown
                ), f"{path.name}/{panel['title']}: undefined metric(s) {sorted(unknown)}"

    def test_dashboard_uids_are_unique(self):
        uids = [json.loads(p.read_text(encoding="utf-8"))["uid"] for p in _DASHBOARDS]
        assert len(uids) == len(set(uids))


class TestKibana:
    _objs = [
        json.loads(line)
        for line in (_ROOT / "monitoring" / "elk" / "kibana" / "lakehouse_saved_objects.ndjson")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]

    def test_references_resolve(self):
        ids = {(o["type"], o["id"]) for o in self._objs}
        for o in self._objs:
            for ref in o["references"]:
                assert (ref["type"], ref["id"]) in ids, f"{o['id']} -> {ref}"

    def test_data_view_matches_logstash_index_pattern(self):
        dv = next(o for o in self._objs if o["type"] == "index-pattern")
        pipeline = (_ROOT / "monitoring" / "elk" / "logstash" / "pipeline.conf").read_text(
            encoding="utf-8"
        )
        assert dv["attributes"]["title"] == "lakehouse-*"
        assert 'index      => "lakehouse-%{service}-' in pipeline

    def test_dq_alert_search_matches_the_logged_event_name(self):
        reporter = (_ROOT / "src" / "quality" / "reporters" / "alert_reporter.py").read_text(
            encoding="utf-8"
        )
        assert '"dq_alert"' in reporter
        search = next(o for o in self._objs if o["id"] == "lakehouse-dq-alerts")
        source = json.loads(search["attributes"]["kibanaSavedObjectMeta"]["searchSourceJSON"])
        assert source["query"]["query"] == 'event : "dq_alert"'

    def test_contract_search_matches_the_logged_event_name(self):
        assert '"contract_violation"' in (
            _ROOT / "src" / "quality" / "contracts" / "enforcer.py"
        ).read_text(encoding="utf-8")

    def test_index_template_and_ilm_are_valid_json(self):
        for name in ("index-template.json", "ilm-policy.json"):
            json.loads((_ROOT / "configs" / "elasticsearch" / name).read_text(encoding="utf-8"))
        tpl = json.loads(
            (_ROOT / "configs" / "elasticsearch" / "index-template.json").read_text(
                encoding="utf-8"
            )
        )
        assert tpl["template"]["settings"]["index.lifecycle.name"] == "lakehouse-logs"
        assert "lakehouse-logs" in (_ROOT / "scripts" / "setup" / "setup_elk.sh").read_text(
            encoding="utf-8"
        )


class TestComposeMounts:
    def test_every_host_path_mounted_by_compose_exists(self):
        compose = yaml.safe_load((_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        missing = []
        for name, svc in compose["services"].items():
            for vol in svc.get("volumes", []):
                if not isinstance(vol, str):
                    continue
                host = vol.split(":")[0]
                if host.startswith("./") and not (_ROOT / host).exists():
                    missing.append(f"{name}: {host}")
        assert not missing, f"compose mounts non-existent host paths: {missing}"

    def test_grafana_mounts_the_committed_dashboards(self):
        compose = yaml.safe_load((_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        vols = compose["services"]["grafana"]["volumes"]
        assert "./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro" in vols

    def test_alertmanager_service_defined(self):
        compose = yaml.safe_load((_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        assert "alertmanager" in compose["services"] and "alertmanager_data" in compose["volumes"]


class TestAlertQuality:
    """Alert wording, runbooks and label plumbing (previously untested and partly wrong)."""

    _rules = _rules()
    _troubleshooting = (_ROOT / "docs" / "troubleshooting" / "TROUBLESHOOTING.md").read_text(
        encoding="utf-8"
    )
    _scrape_jobs = {
        j["job_name"]
        for j in yaml.safe_load(
            (_ROOT / "configs" / "prometheus" / "prometheus.yml").read_text(encoding="utf-8")
        )["scrape_configs"]
    }

    @pytest.mark.parametrize(
        "fname,rule", _rules, ids=lambda v: v if isinstance(v, str) else v.get("alert")
    )
    def test_summary_is_one_line_without_severity_prefix(self, fname, rule):
        summary = rule["annotations"]["summary"]
        assert "\n" not in summary.strip()
        assert not summary.lower().startswith(("critical", "warning", "info")), summary

    @pytest.mark.parametrize(
        "fname,rule", _rules, ids=lambda v: v if isinstance(v, str) else v.get("alert")
    )
    def test_has_description_with_next_step_and_runbook(self, fname, rule):
        ann = rule["annotations"]
        assert ann.get("description", "").strip(), "missing description"
        assert (
            "Next:" in ann["description"]
            or "Check" in ann["description"]
            or "check" in ann["description"]
        )
        assert ann["runbook"].startswith("docs/troubleshooting/TROUBLESHOOTING.md#alert-")

    @pytest.mark.parametrize(
        "fname,rule", _rules, ids=lambda v: v if isinstance(v, str) else v.get("alert")
    )
    def test_runbook_anchor_exists_and_matches_the_alert_name(self, fname, rule):
        anchor = rule["annotations"]["runbook"].split("#", 1)[1]
        assert anchor == f"alert-{rule['alert'].lower()}"
        assert f'<a id="{anchor}"></a>' in self._troubleshooting

    def test_only_warning_and_critical_severities(self):
        assert {r["labels"]["severity"] for _, r in self._rules} <= {"warning", "critical"}
        assert "info" not in (
            _ROOT / "monitoring" / "prometheus" / "rules" / "pipeline_alerts.yml"
        ).read_text(encoding="utf-8").split("groups:")[0].replace("severity   critical", "")

    def test_job_labels_match_real_scrape_jobs(self):
        """APIDown/APIHighErrorRate selected job="retailnexus-api" - no such job - so they could never fire."""
        for _fname, rule in self._rules:
            for job in re.findall(r'job="([^"]+)"', rule["expr"]):
                assert (
                    job in self._scrape_jobs
                ), f"{rule['alert']}: unknown job {job!r} (have {sorted(self._scrape_jobs)})"

    def test_api_error_rate_uses_the_grouped_status_label(self):
        # Instrumentator runs with should_group_status_codes=True, so the label is "5xx", not a regex of digits.
        rule = next(r for _, r in self._rules if r["alert"] == "APIHighErrorRate")
        assert 'status="5xx"' in rule["expr"] and 'status=~"5.."' not in rule["expr"]

    def test_layer_and_domain_alerts_share_thresholds_and_durations(self):
        by_name = {r["alert"]: r for _, r in self._rules}
        for layer_rule, domain_rule in (
            ("DQPassRateCritical", "DomainDQPassRateCritical"),
            ("DQPassRateWarning", "DomainDQPassRateLow"),
        ):
            assert by_name[layer_rule]["for"] == by_name[domain_rule]["for"], (
                layer_rule,
                domain_rule,
            )

            def threshold(r):
                return float(re.search("<\\s*(0\\.\\d+)", by_name[r]["expr"]).group(1))

            assert threshold(layer_rule) == threshold(domain_rule), (layer_rule, domain_rule)

    def test_dashboard_thresholds_mirror_the_alert_thresholds(self):
        dash = json.loads(
            (_ROOT / "monitoring" / "grafana" / "dashboards" / "data_quality.json").read_text(
                encoding="utf-8"
            )
        )
        steps = next(p for p in dash["panels"] if p["type"] == "stat")["fieldConfig"]["defaults"][
            "thresholds"
        ]["steps"]
        assert [s["value"] for s in steps[1:]] == [0.75, 0.9, 0.98]


class TestGeneratedArtifacts:
    """data_quality.json and the Kibana ndjson are generated; the committed copies must be current."""

    @staticmethod
    def _run(script: str, out: Path) -> None:
        import subprocess
        import sys

        subprocess.run(
            [sys.executable, str(_ROOT / "scripts" / "monitoring" / script), str(out)],
            check=True,
            capture_output=True,
        )

    def test_data_quality_dashboard_is_current(self, tmp_path):
        self._run("build_data_quality_dashboard.py", tmp_path / "dq.json")
        committed = json.loads(
            (_ROOT / "monitoring" / "grafana" / "dashboards" / "data_quality.json").read_text(
                encoding="utf-8"
            )
        )
        assert (
            json.loads((tmp_path / "dq.json").read_text(encoding="utf-8")) == committed
        ), "run scripts/monitoring/build_data_quality_dashboard.py"

    def test_kibana_objects_are_current(self, tmp_path):
        self._run("build_kibana_objects.py", tmp_path / "k.ndjson")
        committed = (
            _ROOT / "monitoring" / "elk" / "kibana" / "lakehouse_saved_objects.ndjson"
        ).read_text(encoding="utf-8")
        assert (tmp_path / "k.ndjson").read_text(
            encoding="utf-8"
        ) == committed, "run scripts/monitoring/build_kibana_objects.py"

    def test_every_event_a_kibana_search_filters_on_is_actually_logged(self):
        source = "\n".join(
            p.read_text(encoding="utf-8", errors="ignore")
            for root in ("src", "spark", "airflow", "api")
            for p in (_ROOT / root).rglob("*.py")
        )
        events = set()
        for line in (
            (_ROOT / "monitoring" / "elk" / "kibana" / "lakehouse_saved_objects.ndjson")
            .read_text(encoding="utf-8")
            .splitlines()
        ):
            obj = json.loads(line)
            if obj["type"] == "search":
                query = json.loads(obj["attributes"]["kibanaSavedObjectMeta"]["searchSourceJSON"])[
                    "query"
                ]["query"]
                if query.startswith("event"):
                    events |= set(re.findall(r'"([a-z_]+)"', query.split(":", 1)[1]))
        assert events, "no event filters found"
        missing = {e for e in events if f'"{e}"' not in source}
        assert not missing, f"Kibana filters on events nothing logs: {sorted(missing)}"

    def test_saved_search_columns_start_with_scope_then_layer_domain_check_message(self):
        order = [
            "environment",
            "service",
            "layer",
            "domain",
            "table",
            "check",
            "rule",
            "column",
            "severity",
            "message",
            "error",
        ]
        for line in (
            (_ROOT / "monitoring" / "elk" / "kibana" / "lakehouse_saved_objects.ndjson")
            .read_text(encoding="utf-8")
            .splitlines()
        ):
            obj = json.loads(line)
            if obj["type"] == "search":
                cols = [c for c in obj["attributes"]["columns"] if c in order]
                assert cols == sorted(
                    cols, key=order.index
                ), f"{obj['id']}: {obj['attributes']['columns']}"
                assert obj["attributes"]["description"], f"{obj['id']} has no description"


class TestDashboardConventions:
    _dashes = {p.name: json.loads(p.read_text(encoding="utf-8")) for p in _DASHBOARDS}
    _scrape_jobs = TestAlertQuality._scrape_jobs

    @pytest.mark.parametrize("name", sorted(_dashes))
    def test_panel_queries_use_real_scrape_jobs(self, name):
        for panel in self._dashes[name]["panels"]:
            for t in panel.get("targets", []):
                for job in re.findall(r'job="([^"]+)"', t["expr"]):
                    assert job in self._scrape_jobs, f"{name}/{panel['title']}: job {job!r}"

    @pytest.mark.parametrize("name", sorted(_dashes))
    def test_every_target_has_a_ref_id_and_panels_have_titles(self, name):
        for panel in self._dashes[name]["panels"]:
            assert panel["title"] and panel["title"][0].isupper()
            refs = [t.get("refId") for t in panel.get("targets", [])]
            assert all(refs) and len(refs) == len(set(refs)), panel["title"]

    @pytest.mark.parametrize("name", sorted(_dashes))
    def test_titles_use_sentence_case(self, name):
        for panel in self._dashes[name]["panels"]:
            words = re.sub(r"\(.*?\)", "", panel["title"]).replace("—", " ").split()
            shouting = [
                w
                for w in words[1:]
                if w[0].isupper()
                and w.upper() != w
                and w not in {"SLA", "DQ", "API", "Bronze", "Silver", "Gold"}
            ]
            assert not shouting, f"{name}: '{panel['title']}' is not sentence case ({shouting})"

    @pytest.mark.parametrize("name", sorted(_dashes))
    def test_legends_have_no_padding_spaces(self, name):
        for panel in self._dashes[name]["panels"]:
            for t in panel.get("targets", []):
                assert "{{ " not in t.get("legendFormat", "") and " }}" not in t.get(
                    "legendFormat", ""
                )

    @pytest.mark.parametrize("name", sorted(_dashes))
    def test_overview_row_fills_the_grid(self, name):
        tiles = [
            p for p in self._dashes[name]["panels"] if p["type"] != "row" and p["gridPos"]["y"] == 1
        ]
        assert sum(p["gridPos"]["w"] for p in tiles) == 24, [p["gridPos"] for p in tiles]

    def test_pass_rate_panels_use_the_four_step_scale(self):
        ph = {
            p["id"]: p for p in self._dashes["pipeline_health.json"]["panels"] if p["type"] != "row"
        }
        steps = ph[2]["fieldConfig"]["defaults"]["thresholds"]["steps"]
        assert ph[2]["type"] == "bargauge" and [s["value"] for s in steps] == [
            None,
            0.75,
            0.9,
            0.98,
        ]

    def test_sla_miss_tiles_agree_across_dashboards_and_with_the_alerts(self):
        ph = next(p for p in self._dashes["pipeline_health.json"]["panels"] if p["id"] == 1)
        dq = next(
            p
            for p in self._dashes["data_quality.json"]["panels"]
            if p.get("title") == "SLA misses (24h)"
        )

        def steps(p):
            return [s["value"] for s in p["fieldConfig"]["defaults"]["thresholds"]["steps"]]

        assert steps(ph) == steps(dq) == [None, 1, 21]  # red once SLAMissesCritical (>20) fires
        assert "mappings" not in ph["fieldConfig"]["defaults"]

    def test_freshness_is_shown_against_each_domains_own_sla(self):
        panel = next(p for p in self._dashes["pipeline_health.json"]["panels"] if p["id"] == 10)
        exprs = {t["expr"] for t in panel["targets"]}
        assert any("/ 2" in e and "clickstream" in e for e in exprs)
        assert any("/ 48" in e and "employees" in e for e in exprs)
        assert any("/ 26" in e for e in exprs)
        alert_text = (
            _ROOT / "monitoring" / "prometheus" / "rules" / "pipeline_alerts.yml"
        ).read_text(encoding="utf-8")
        assert "> 2\n" in alert_text and "> 26\n" in alert_text and "> 48\n" in alert_text

    def test_dashboard_tags_do_not_overlap_confusingly(self):
        assert "data-quality" not in self._dashes["pipeline_health.json"]["tags"]
        assert "data-quality" in self._dashes["data_quality.json"]["tags"]


# ─────────────────────────────────────────────────────────────────────────────
# Exporters, scrape jobs and the metrics that alerts / dashboards may use
# ─────────────────────────────────────────────────────────────────────────────
# Names verified against the real exporters (see scripts/monitoring/build_platform_dashboards.py).
_EXTERNAL_METRICS = {
    # prometheus-fastapi-instrumentator
    "http_requests_total",
    "http_request_duration_seconds_bucket",
    # kafka-exporter (danielqsj)
    "kafka_brokers",
    "kafka_consumergroup_lag",
    "kafka_consumergroup_members",
    "kafka_consumergroup_current_offset",
    "kafka_topic_partitions",
    "kafka_topic_partition_current_offset",
    "kafka_topic_partition_under_replicated_partition",
    # Spark PrometheusServlet (master)
    "metrics_master_aliveWorkers_Value",
    "metrics_master_workers_Value",
    "metrics_master_apps_Value",
    "metrics_master_waitingApps_Value",
    # MinIO cluster metrics
    "minio_cluster_capacity_usable_free_bytes",
    "minio_cluster_capacity_usable_total_bytes",
    "minio_cluster_health_status",
    "minio_cluster_drive_online_total",
    # cAdvisor
    "container_cpu_usage_seconds_total",
    "container_memory_working_set_bytes",
    # Airflow via statsd-exporter
    "airflow_scheduler_heartbeat",
    "airflow_ti_successes",
    "airflow_ti_failures",
    # Prometheus built-in
    "up",
}
_PROMQL_WORDS = {
    "sum",
    "rate",
    "increase",
    "count",
    "absent",
    "vector",
    "histogram_quantile",
    "or",
    "by",
    "le",
    "and",
    "on",
    "without",
    "job",
    "status",
    "handler",
    "topic",
    "consumergroup",
    "domain",
    "layer",
    "name",
    "group_left",
    "group_right",
    "avg",
    "max",
    "min",
    "topk",
    "bottomk",
    "__rate_interval",
}


def _metric_names(expr: str) -> set[str]:
    """Identifiers in a PromQL expression that look like metric names (not functions/labels/keywords)."""
    expr = re.sub(r"\{[^}]*\}", "", expr)  # drop label matchers
    expr = re.sub(r'"[^"]*"', "", expr)
    names = set(re.findall(r"[a-zA-Z_:][a-zA-Z0-9_:]*", expr))
    return {
        n
        for n in names
        if n not in _PROMQL_WORDS and not n.isdigit() and not re.fullmatch(r"[mshdwy]", n)
    }


class TestExportersAndMetrics:
    _compose = yaml.safe_load((_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    _prom = yaml.safe_load(
        (_ROOT / "configs" / "prometheus" / "prometheus.yml").read_text(encoding="utf-8")
    )

    def test_every_scrape_target_is_a_compose_service(self):
        services = set(self._compose["services"])
        for job in self._prom["scrape_configs"]:
            for sc in job["static_configs"]:
                for target in sc["targets"]:
                    host = target.split(":")[0]
                    assert (
                        host in services or host == "localhost"
                    ), f"job {job['job_name']}: {host} is not a compose service"

    def test_scrape_jobs_cover_the_six_dashboards(self):
        jobs = {j["job_name"] for j in self._prom["scrape_configs"]}
        assert {"fastapi", "spark-master", "kafka", "minio", "cadvisor", "airflow"} <= jobs

    def test_exporter_services_are_pinned(self):
        for name in ("kafka-exporter", "statsd-exporter", "cadvisor"):
            image = self._compose["services"][name]["image"]
            assert ":" in image and not image.endswith(":latest"), image

    def test_spark_exposes_metrics_and_airflow_emits_statsd(self):
        vols = self._compose["services"]["spark-master"]["volumes"]
        assert "./configs/spark/metrics.properties:/opt/spark/conf/metrics.properties:ro" in vols
        props = (_ROOT / "configs" / "spark" / "metrics.properties").read_text(encoding="utf-8")
        assert "PrometheusServlet" in props and "/metrics/master/prometheus" in props
        text = (_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        assert "AIRFLOW__METRICS__STATSD_ON" in text and "statsd-exporter" in text

    def test_minio_metrics_are_scrapeable_without_a_token(self):
        assert (
            self._compose["services"]["minio"]["environment"]["MINIO_PROMETHEUS_AUTH_TYPE"]
            == "public"
        )

    def test_spark_path_uses_the_slash_the_ui_redirects_to(self):
        job = next(j for j in self._prom["scrape_configs"] if j["job_name"] == "spark-master")
        assert job["metrics_path"] == "/metrics/master/prometheus/"

    def test_alerts_and_dashboards_only_use_known_metrics(self):
        known = _API_METRICS | _EXTERNAL_METRICS
        exprs = [(f"alert {r['alert']}", r["expr"]) for _, r in _rules()]
        for path in _DASHBOARDS:
            for panel in json.loads(path.read_text(encoding="utf-8"))["panels"]:
                exprs += [
                    (f"{path.name}/{panel['title']}", t["expr"]) for t in panel.get("targets", [])
                ]
        unknown = {}
        for where, expr in exprs:
            bad = {m for m in _metric_names(expr) if m not in known}
            if bad:
                unknown[where] = sorted(bad)
        assert not unknown, f"unverified metric names: {unknown}"

    def test_kafka_alert_uses_the_exporters_label_names(self):
        rule = next(r for _, r in _rules() if r["alert"] == "KafkaConsumerLag")
        assert "consumergroup" in rule["expr"] and "kafka_consumergroup_lag" in rule["expr"]
        assert "$labels.group " not in rule["annotations"]["summary"]

    def test_platform_dashboards_are_current(self, tmp_path):
        import subprocess
        import sys

        subprocess.run(
            [
                sys.executable,
                str(_ROOT / "scripts" / "monitoring" / "build_platform_dashboards.py"),
                str(tmp_path),
            ],
            check=True,
            capture_output=True,
        )
        for name in (
            "api_health.json",
            "kafka_health.json",
            "spark_jobs.json",
            "infrastructure.json",
        ):
            committed = json.loads(
                (_ROOT / "monitoring" / "grafana" / "dashboards" / name).read_text(encoding="utf-8")
            )
            assert (
                json.loads((tmp_path / name).read_text(encoding="utf-8")) == committed
            ), f"stale: {name}"
