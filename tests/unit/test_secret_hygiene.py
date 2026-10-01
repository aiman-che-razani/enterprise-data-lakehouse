"""Guards against development credentials leaking into anything that ships to staging/prod."""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

from src.processing.bronze.spark_session import s3a_conf

_ROOT = Path(__file__).resolve().parents[2]


def _conf_lines(name: str) -> list[str]:
    text = (_ROOT / "configs" / "spark" / name).read_text(encoding="utf-8")
    return [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")]


def _keys(lines: list[str]) -> set[str]:
    return {ln.split()[0] for ln in lines}


class TestSparkDefaults:
    def test_shipped_conf_contains_no_credentials_or_dev_endpoint(self):
        text = (_ROOT / "configs" / "spark" / "spark-defaults.conf").read_text(encoding="utf-8")
        for forbidden in (
            "access.key",
            "secret.key",
            "minioadmin",
            "minio:9000",
            "SimpleAWSCredentialsProvider",
        ):
            assert forbidden not in text, f"spark-defaults.conf must not contain {forbidden!r}"

    def test_shipped_conf_uses_the_default_credential_chain(self):
        assert "com.amazonaws.auth.DefaultAWSCredentialsProviderChain" in "\n".join(
            _conf_lines("spark-defaults.conf")
        )

    def test_dev_conf_is_base_plus_only_minio_overrides(self):
        base, dev = _conf_lines("spark-defaults.conf"), _conf_lines("spark-defaults.dev.conf")
        # every base setting is present in dev unless dev deliberately overrides that key
        overridden = {"spark.hadoop.fs.s3a.aws.credentials.provider"}
        assert [ln for ln in base if ln not in dev and ln.split()[0] not in overridden] == []
        extra = _keys(dev) - _keys(base)
        assert extra == {
            "spark.hadoop.fs.s3a.endpoint",
            "spark.hadoop.fs.s3a.access.key",
            "spark.hadoop.fs.s3a.secret.key",
            "spark.hadoop.fs.s3a.path.style.access",
        }

    def test_image_ships_the_clean_file_and_only_compose_mounts_the_dev_file(self):
        dockerfile = (_ROOT / "docker" / "spark" / "Dockerfile").read_text(encoding="utf-8")
        assert (
            "configs/spark/spark-defaults.conf" in dockerfile
            and "spark-defaults.dev.conf" not in dockerfile
        )
        compose = (_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        assert "spark-defaults.dev.conf:/opt/spark/conf/spark-defaults.conf:ro" in compose
        assert "./configs/spark/spark-defaults.conf:" not in compose
        for wf in (_ROOT / ".github" / "workflows").glob("*.yml"):
            assert "spark-defaults.dev.conf" not in wf.read_text(encoding="utf-8")


class TestS3aConf:
    @staticmethod
    def _settings(endpoint):
        return SimpleNamespace(
            s3_endpoint_url=endpoint,
            aws_access_key_id="ak",
            aws_secret_access_key=SimpleNamespace(get_secret_value=lambda: "sk"),
        )

    def test_minio_uses_static_keys_and_path_style(self):
        conf = s3a_conf(self._settings("http://minio:9000"))
        assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
        assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
        assert (conf["spark.hadoop.fs.s3a.access.key"], conf["spark.hadoop.fs.s3a.secret.key"]) == (
            "ak",
            "sk",
        )

    def test_aws_configures_no_keys_and_no_custom_endpoint(self):
        conf = s3a_conf(self._settings(None))
        assert not [
            k
            for k in conf
            if k.endswith(("access.key", "secret.key", "endpoint", "path.style.access"))
        ]
        assert conf["spark.hadoop.fs.s3a.aws.credentials.provider"].endswith(
            "DefaultAWSCredentialsProviderChain"
        )


class TestRepoWideSecrets:
    _PATTERNS = [
        re.compile(r"AKIA[0-9A-Z]{16}"),
        re.compile(r"-----BEGIN (RSA |EC )?PRIVATE KEY-----"),
    ]

    def test_no_cloud_keys_or_private_keys_committed(self):
        skip = {".git", ".claude", "__pycache__", ".venv", "node_modules"}
        hits = []
        for path in _ROOT.rglob("*"):
            if (
                path.is_file()
                and not (set(path.parts) & skip)
                and path.suffix
                in {
                    ".py",
                    ".yml",
                    ".yaml",
                    ".tf",
                    ".conf",
                    ".sql",
                    ".sh",
                    ".json",
                    ".md",
                    ".env",
                    ".tfvars",
                }
            ):
                if path.name == Path(__file__).name:
                    continue
                text = path.read_text(encoding="utf-8", errors="ignore")
                hits += [str(path.relative_to(_ROOT)) for p in self._PATTERNS if p.search(text)]
        assert hits == []


class TestDockerfiles:
    _FILES = sorted((_ROOT / "docker").glob("*/Dockerfile"))

    def test_version_specifiers_are_quoted(self):
        """`pip install pkg>=1.0` in a RUN line is a shell redirect: the pin is silently dropped."""
        bad = []
        for f in self._FILES:
            for n, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
                if re.match(r"^\s+[A-Za-z0-9_.\-]+>=[0-9]", line):
                    bad.append(f"{f.parent.name}/Dockerfile:{n}")
        assert bad == []

    def test_spark_downloads_are_checksum_verified(self):
        text = (_ROOT / "docker" / "spark" / "Dockerfile").read_text(encoding="utf-8")
        assert "sha512sum -c" in text and "sha1sum -c" in text

    def test_api_image_has_no_unused_crypto_packages(self):
        text = (_ROOT / "docker" / "api" / "Dockerfile").read_text(encoding="utf-8")
        assert "python-jose" not in text and "passlib" not in text

    def test_images_run_as_non_root(self):
        for f in self._FILES:
            users = re.findall(r"^USER\s+(\S+)", f.read_text(encoding="utf-8"), re.M)
            assert users and users[-1] not in {"root", "0"}, f"{f.parent.name} ends as root"


class TestWorkflows:
    _FILES = sorted((_ROOT / ".github" / "workflows").glob("*.yml"))

    def test_every_action_is_pinned_to_a_commit_sha(self):
        loose = []
        for f in self._FILES:
            for n, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
                m = re.match(r"^\s*-?\s*uses:\s*([\w./-]+)@(\S+)", line)
                if (
                    m
                    and not m.group(1).startswith("./")
                    and not re.fullmatch(r"[0-9a-f]{40}", m.group(2))
                ):
                    loose.append(f"{f.name}:{n} {m.group(1)}@{m.group(2)}")
        assert loose == []

    def test_pinned_actions_keep_their_version_in_a_comment(self):
        for f in self._FILES:
            for line in f.read_text(encoding="utf-8").splitlines():
                if re.search(r"uses:\s*[\w./-]+@[0-9a-f]{40}", line):
                    assert re.search(
                        r"#\s*v\d", line
                    ), f"{f.name}: pinned action lost its version comment: {line.strip()}"

    def test_no_long_lived_aws_keys_in_deploy_workflows(self):
        for name in ("cd-staging.yml", "cd-prod.yml"):
            text = (_ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
            assert (
                "secrets.AWS_ACCESS_KEY_ID" not in text
                and "secrets.AWS_SECRET_ACCESS_KEY" not in text
            )
            assert (
                "role-to-assume: ${{ secrets.AWS_ROLE_ARN }}" in text and "id-token: write" in text
            )

    def test_every_workflow_declares_least_privilege_permissions(self):
        for f in self._FILES:
            assert re.search(
                r"^permissions:\s*\n\s+contents:\s*read", f.read_text(encoding="utf-8"), re.M
            ), f.name
