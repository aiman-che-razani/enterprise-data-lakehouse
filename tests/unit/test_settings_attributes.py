"""Every ``settings.<attr>`` the code reads must exist on Settings.

Unit tests pass MagicMock settings, which accept any attribute, so a typo or a never-added field
(`pipeline_version`, `silver_path`, `kafka_sasl_*`) only fails in a real run - for `pipeline_version`
that was every Silver and Gold build.
"""

from __future__ import annotations

import re
from pathlib import Path

from src.common.config.settings import Settings

_ROOT = Path(__file__).resolve().parents[2]
# names that are not settings fields but appear as `settings.<name>` on other objects
_ALLOW = {"Settings"}


def _known() -> set[str]:
    return (
        {n for n in dir(Settings) if not n.startswith("_")}
        | set(Settings.model_fields)
        | set(Settings.model_computed_fields)
    )


def _usages() -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    for base in ("src", "spark", "api", "airflow", "scripts"):
        for path in (_ROOT / base).rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if "loading/" in path.as_posix() and "warehouse_config" not in path.name:
                continue  # loaders use WarehouseSettings, a different class
            for name in re.findall(r"\b(?:self\.)?settings\.([a-z_][a-z0-9_]*)", text):
                found.setdefault(name, set()).add(path.relative_to(_ROOT).as_posix())
    return found


def test_every_settings_attribute_used_in_code_exists():
    known = _known()
    missing = {
        name: sorted(files)
        for name, files in _usages().items()
        if name not in known and name not in _ALLOW
    }
    # WarehouseSettings fields are legitimately read as `.settings.<x>` in the loading package only
    assert not missing, f"attributes read from Settings that do not exist: {missing}"


def test_the_fields_that_used_to_be_missing_now_exist():
    for name in (
        "pipeline_version",
        "kafka_security_protocol",
        "kafka_sasl_mechanism",
        "kafka_sasl_username",
        "kafka_sasl_password",
        "bronze_s3_path",
        "silver_s3_path",
        "gold_s3_path",
        "postgres_dsn",
        "pii_hash_key",
        "contract_mode",
    ):
        assert name in _known(), name


def test_dq_engines_build_paths_from_real_settings_attributes():
    for layer in ("bronze", "silver", "gold"):
        text = (_ROOT / "src" / "quality" / "engines" / f"{layer}_dq.py").read_text(
            encoding="utf-8"
        )
        assert f"settings.{layer}_s3_path" in text and f"settings.{layer}_path" not in text


def test_metadata_step_reads_a_real_version_field():
    s = Settings(_env_file=None)
    assert s.pipeline_version == "3.0.0"
