"""Docs that state facts about the code must not drift from it."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_DOCS = _ROOT / "docs"


def _makefile_targets() -> set[str]:
    text = (_ROOT / "Makefile").read_text(encoding="utf-8")
    return set(re.findall(r"^([A-Za-z0-9_.%-]+):", text, re.M))


def _dag_ids() -> set[str]:
    ids = set()
    for p in (_ROOT / "airflow" / "dags").glob("*_dag.py"):
        ids |= set(re.findall(r'dag_id="([^"]+)"', p.read_text(encoding="utf-8")))
    return ids


def test_data_dictionary_gold_and_postgres_sections_are_generated_from_the_ddl():
    script = _ROOT / "scripts" / "deployment" / "build_data_dictionary_gold.py"
    result = subprocess.run(
        [sys.executable, str(script), "--check"], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_every_make_target_the_docs_tell_you_to_run_exists():
    targets = _makefile_targets()
    missing = []
    for doc in [*list(_DOCS.rglob("*.md")), _ROOT / "README.md"]:
        for m in re.finditer(
            r"^\s*(?:\$ )?make ([a-z][a-z0-9-]*)", doc.read_text(encoding="utf-8"), re.M
        ):
            if m.group(1) not in targets:
                missing.append(f"{doc.relative_to(_ROOT)}: make {m.group(1)}")
    assert not missing, missing


def test_architecture_doc_lists_the_real_dag_ids():
    text = (_DOCS / "architecture" / "ARCHITECTURE.md").read_text(encoding="utf-8")
    section = text[text.index("## 10") : text.index("## 11")] if "## 11" in text else text
    stale = {
        "silver_processing",
        "gold_processing",
        "quality_checks",
        "quality_report",
        "stream_monitor",
        "compaction",
    }
    assert not [
        d for d in stale if f"`{d}`" in section
    ], "architecture doc names DAGs that do not exist"
    for dag in _dag_ids():
        assert f"`{dag}`" in section, f"DAG {dag} missing from the architecture DAG inventory"


def test_architecture_records_the_gate_and_contract_decisions():
    text = (_DOCS / "architecture" / "ARCHITECTURE.md").read_text(encoding="utf-8")
    assert "TDR-005" in text
    assert "silver_dq_gate" in text and "gold_dq_gate" in text
    assert "DATA_CONTRACT_MODE" in text


def test_deployment_doc_lists_the_production_only_settings():
    text = (_DOCS / "deployment" / "DEPLOYMENT.md").read_text(encoding="utf-8")
    for name in ("PII_HASH_KEY", "DATA_CONTRACT_MODE", "AWS_ROLE_ARN", "create_api_key.py"):
        assert name in text, name


@pytest.mark.parametrize("doc", ["README.md", "docs/setup/SETUP.md"])
def test_documented_dev_api_key_matches_the_seed(doc):
    text = (_ROOT / doc).read_text(encoding="utf-8")
    keys = set(re.findall(r"dev-api-key-[a-z0-9-]+", text))
    assert keys <= {"dev-api-key-retailnexus-2026"}, keys
