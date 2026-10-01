"""The .claude/agents definitions stay valid, least-privilege, and grounded in real paths."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_AGENTS = sorted((_ROOT / ".claude" / "agents").glob("*.md"))
_EXPECTED = {
    "agents",
    "api",
    "architecture",
    "code-style",
    "database",
    "design-system",
    "prd",
    "security",
}
_REVIEWERS = {"security", "code-style", "architecture"}
_TOOLS = {"Read", "Glob", "Grep", "Write", "Edit", "Bash", "WebFetch", "WebSearch"}


def _parse(path: Path) -> tuple[dict, str]:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    assert text.startswith("---\n"), f"{path.name}: missing frontmatter"
    front, body = text[4:].split("\n---\n", 1)
    return yaml.safe_load(front), body


def test_the_expected_roster_exists():
    assert {p.stem for p in _AGENTS} == _EXPECTED


@pytest.mark.parametrize("path", _AGENTS, ids=lambda p: p.stem)
def test_frontmatter_is_valid(path):
    meta, body = _parse(path)
    assert meta["name"] == path.stem
    assert meta["description"].startswith("Use ") or meta["description"].startswith(
        "Meta-agent"
    ), "description must say when to use it"
    assert meta["model"] in {"opus", "sonnet", "haiku"}
    tools = [t.strip() for t in meta["tools"].split(",")]
    assert (
        set(tools) <= _TOOLS
    ), f"unknown/garbled tools: {tools}"  # e.g. a trailing YAML comment folded into the value
    assert len(body.splitlines()) <= 80


@pytest.mark.parametrize("path", [p for p in _AGENTS if p.stem in _REVIEWERS], ids=lambda p: p.stem)
def test_reviewers_cannot_write_files(path):
    tools = {t.strip() for t in _parse(path)[0]["tools"].split(",")}
    assert not tools & {"Write", "Edit"}


@pytest.mark.parametrize("path", _AGENTS, ids=lambda p: p.stem)
def test_every_repo_path_mentioned_exists(path):
    body = _parse(path)[1]
    missing = []
    for ref in set(
        re.findall(
            r"`((?:src|api|spark|airflow|scripts|tests|docs|terraform|warehouse|monitoring|configs)/[\w./*-]+)`",
            body,
        )
    ):
        if "*" in ref:
            if not list(_ROOT.glob(ref)):
                missing.append(ref)
        elif ref.endswith("/"):
            if not (_ROOT / ref).is_dir():
                missing.append(ref)
        elif not (_ROOT / ref.rstrip(".")).exists():
            missing.append(ref)
    assert not missing, f"{path.name} references missing paths: {sorted(missing)}"


@pytest.mark.parametrize("path", _AGENTS, ids=lambda p: p.stem)
def test_referenced_make_targets_exist(path):
    targets = set(
        re.findall(r"^([A-Za-z0-9_.%-]+):", (_ROOT / "Makefile").read_text(encoding="utf-8"), re.M)
    )
    for target in re.findall(r"`make ([a-z][a-z0-9-]*)", _parse(path)[1]):
        assert target in targets, f"{path.name}: make {target}"


def test_router_lists_every_agent():
    body = _parse(_ROOT / ".claude" / "agents" / "agents.md")[1]
    for name in _EXPECTED:
        assert f"`{name}`" in body, name
