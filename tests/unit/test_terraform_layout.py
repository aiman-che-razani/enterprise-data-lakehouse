"""
Static checks on the Terraform tree (no terraform binary or AWS credentials needed).

Catches the mistakes that only show up at `terraform plan`: modules called with missing or
unknown arguments, tfvars referencing undeclared variables, environments drifting apart.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_TF = Path(__file__).resolve().parents[2] / "terraform"
_MODULES = sorted(p.name for p in (_TF / "modules").iterdir() if p.is_dir())
_ENVS = ["dev", "staging", "prod"]
_SHARED = ("main.tf", "variables.tf", "outputs.tf")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _block(text: str, start: int) -> str:
    """Return the brace-balanced body starting at the '{' found at/after *start*."""
    i = text.index("{", start)
    depth = 0
    for j in range(i, len(text)):
        depth += {"{": 1, "}": -1}.get(text[j], 0)
        if depth == 0:
            return text[i + 1 : j]
    raise AssertionError("unbalanced braces")


def _variables(text: str) -> dict[str, bool]:
    """variable name -> has a default"""
    out = {}
    for m in re.finditer(r'^variable\s+"(\w+)"', text, re.M):
        out[m.group(1)] = re.search(r"^\s*default\s*=", _block(text, m.end()), re.M) is not None
    return out


def _top_level_args(body: str) -> set[str]:
    """Argument names assigned at nesting depth 0 of a block body."""
    depth, args = 0, set()
    for line in body.split("\n"):
        stripped = line.strip()
        if depth == 0:
            m = re.match(r"^(\w+)\s*=", stripped)
            if m:
                args.add(m.group(1))
        depth += (
            line.count("{")
            + line.count("[")
            + line.count("(")
            - line.count("}")
            - line.count("]")
            - line.count(")")
        )
    return args


def _tfvars(path: Path) -> set[str]:
    return set(re.findall(r"^(\w+)\s*=", _read(path), re.M))


class TestModules:
    def test_expected_modules_exist(self):
        assert set(_MODULES) == {"s3", "networking", "iam", "kafka", "emr", "redshift"}

    @pytest.mark.parametrize("module", _MODULES)
    def test_module_has_standard_files(self, module):
        for f in ("main.tf", "variables.tf", "outputs.tf"):
            assert (_TF / "modules" / module / f).is_file(), f"{module}/{f}"

    @pytest.mark.parametrize("module", _MODULES)
    def test_module_pins_terraform_and_provider(self, module):
        text = _read(_TF / "modules" / module / "main.tf")
        assert 'required_version = ">= 1.8.0"' in text and 'source  = "hashicorp/aws"' in text

    @pytest.mark.parametrize("module", _MODULES)
    def test_module_has_outputs(self, module):
        assert re.search(r'^output\s+"', _read(_TF / "modules" / module / "outputs.tf"), re.M)


class TestEnvironments:
    @pytest.mark.parametrize("env", _ENVS)
    def test_environment_files(self, env):
        for f in (*_SHARED, "backend.tf", f"{env}.tfvars"):
            assert (_TF / "environments" / env / f).is_file(), f"{env}/{f}"

    @pytest.mark.parametrize("fname", _SHARED)
    def test_environments_share_identical_code(self, fname):
        contents = {env: _read(_TF / "environments" / env / fname) for env in _ENVS}
        assert (
            contents["dev"] == contents["staging"] == contents["prod"]
        ), f"{fname} drifted between environments"

    @pytest.mark.parametrize("env", _ENVS)
    def test_backend_state_key_is_per_environment(self, env):
        text = _read(_TF / "environments" / env / "backend.tf")
        assert f'key            = "lakehouse/{env}/terraform.tfstate"' in text
        assert "encrypt        = true" in text and "dynamodb_table" in text

    @pytest.mark.parametrize("env", _ENVS)
    def test_tfvars_set_every_required_variable_and_nothing_undeclared(self, env):
        declared = _variables(_read(_TF / "environments" / env / "variables.tf"))
        given = _tfvars(_TF / "environments" / env / f"{env}.tfvars")
        required = {v for v, has_default in declared.items() if not has_default}
        assert required <= given, f"{env}.tfvars missing: {sorted(required - given)}"
        assert given <= set(
            declared
        ), f"{env}.tfvars sets undeclared vars: {sorted(given - set(declared))}"

    @pytest.mark.parametrize("env", _ENVS)
    def test_tfvars_environment_value_matches_directory(self, env):
        assert re.search(
            rf'^environment\s*=\s*"{env}"',
            _read(_TF / "environments" / env / f"{env}.tfvars"),
            re.M,
        )

    def test_prod_is_hardened_relative_to_dev(self):
        dev = _read(_TF / "environments" / "dev" / "dev.tfvars")
        prod = _read(_TF / "environments" / "prod" / "prod.tfvars")
        assert "enable_kms            = true" in prod and "enable_kms            = false" in dev
        assert "force_destroy_buckets = false" in prod
        assert "single_nat_gateway = false" in prod and "az_count           = 3" in prod

    def test_cidrs_do_not_overlap_between_environments(self):
        cidrs = [
            re.search(
                r'vpc_cidr\s*=\s*"([\d./]+)"', _read(_TF / "environments" / e / f"{e}.tfvars")
            ).group(1)
            for e in _ENVS
        ]
        assert len(cidrs) == len(set(cidrs))


class TestModuleWiring:
    main = _read(_TF / "environments" / "dev" / "main.tf")

    def _calls(self) -> dict[str, tuple[str, set[str]]]:
        calls = {}
        for m in re.finditer(r'^module\s+"(\w+)"', self.main, re.M):
            body = _block(self.main, m.end())
            source = re.search(r'source\s*=\s*"\.\./\.\./modules/(\w+)"', body).group(1)
            calls[m.group(1)] = (source, _top_level_args(body) - {"source"})
        return calls

    def test_every_module_is_used_by_the_environment(self):
        assert {src for src, _ in self._calls().values()} == set(_MODULES)

    def test_calls_pass_all_required_and_only_declared_variables(self):
        for name, (source, args) in self._calls().items():
            declared = _variables(_read(_TF / "modules" / source / "variables.tf"))
            required = {v for v, has_default in declared.items() if not has_default}
            assert (
                required <= args
            ), f"module.{name} missing required args {sorted(required - args)}"
            assert args <= set(
                declared
            ), f"module.{name} passes unknown args {sorted(args - set(declared))}"

    def test_module_output_references_exist(self):
        outputs = {
            m: set(
                re.findall(r'^output\s+"(\w+)"', _read(_TF / "modules" / m / "outputs.tf"), re.M)
            )
            for m in _MODULES
        }
        calls = self._calls()
        for ref_module, ref_output in re.findall(r"module\.(\w+)\.(\w+)", self.main):
            source = calls[ref_module][0]
            assert (
                ref_output in outputs[source]
            ), f"module.{ref_module}.{ref_output} is not an output of modules/{source}"

    def test_environment_outputs_reference_real_module_outputs(self):
        text = _read(_TF / "environments" / "dev" / "outputs.tf")
        outputs = {
            m: set(
                re.findall(r'^output\s+"(\w+)"', _read(_TF / "modules" / m / "outputs.tf"), re.M)
            )
            for m in _MODULES
        }
        calls = self._calls()
        for ref_module, ref_output in re.findall(r"module\.(\w+)\.(\w+)", text):
            assert ref_output in outputs[calls[ref_module][0]]

    def test_redshift_password_is_managed_not_supplied(self):
        text = _read(_TF / "modules" / "redshift" / "main.tf")
        assert "manage_master_password = true" in text
        assert not re.search(r"^\s*master_password\s*=", text, re.M)


class TestIamLeastPrivilege:
    text = _read(_TF / "modules" / "iam" / "main.tf")

    @staticmethod
    def _statement(text: str, sid: str) -> str:
        """Text of the brace-balanced ``statement`` block that declares *sid*."""
        start = text.rindex("statement {", 0, text.index(f'sid     = "{sid}"'))
        return "statement " + "{" + _block(text, start) + "}"

    def test_glue_access_is_scoped_to_environment_databases(self):
        block = self._statement(self.text, "GlueCatalog")
        assert 'resources = ["*"]' not in block
        assert (
            "database/${local.glue_db_prefix}*" in block
            and "table/${local.glue_db_prefix}*/*" in block
        )

    def test_redshift_role_gets_only_the_read_only_copy_policy(self):
        attachments = re.findall(
            r"aws_iam_role\.redshift\.name\s*policy_arn\s*=\s*(\S+)", self.text
        )
        assert attachments == ["aws_iam_policy.redshift_copy.arn"]
        assert "aws_iam_policy.data_access.arn" in self.text  # still used by the EMR role

    def test_redshift_copy_policy_is_read_only(self):
        start = self.text.index('data "aws_iam_policy_document" "redshift_copy"')
        body = self.text[start : self.text.index('resource "aws_iam_policy" "redshift_copy"')]
        for write in (
            "s3:PutObject",
            "s3:DeleteObject",
            "secretsmanager",
            "glue:",
            "kms:GenerateDataKey",
        ):
            assert write not in body

    def test_every_service_assume_role_pins_the_source_account(self):
        for name in ("emr_assume", "redshift_assume"):
            start = self.text.index(f'data "aws_iam_policy_document" "{name}"')
            body = "{" + _block(self.text, start) + "}"
            assert "aws:SourceAccount" in body, name


class TestSyntaxBasics:
    @pytest.mark.parametrize(
        "path", sorted(_TF.rglob("*.tf")), ids=lambda p: str(p.relative_to(_TF))
    )
    def test_braces_and_quotes_balanced(self, path):
        text = re.sub(r"<<-?(\w+)\n.*?\n\s*\1\n", "", _read(path), flags=re.S)  # drop heredocs
        text = re.sub(r"#.*", "", text)
        for open_c, close_c in ("{}", "[]", "()"):
            assert text.count(open_c) == text.count(
                close_c
            ), f"unbalanced {open_c}{close_c} in {path.name}"
        assert text.count('"') % 2 == 0, f"odd number of quotes in {path.name}"

    def test_no_hardcoded_credentials(self):
        for path in _TF.rglob("*"):
            if path.is_file():
                assert not re.search(
                    r"AKIA[0-9A-Z]{16}|aws_secret_access_key|password\s*=\s*\"", _read(path)
                ), path
