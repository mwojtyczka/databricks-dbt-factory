"""Validate the committed ``tests/test_data`` job specs against the Databricks Asset
Bundle (DAB) JSON schema.

The unit and CLI tests prove the generator produces the output we *expect* (golden-file
diffs), but nothing there proves that expected output is actually a spec Databricks would
accept — a rename or typo could keep the golden files self-consistent yet emit an invalid
bundle. This test closes that gap: it validates every fixture against the real bundle
schema, so an invalid field or wrong type in a fixture (or in future generated output that
we snapshot as a fixture) fails CI.

The schema is produced on the fly with ``databricks bundle schema``, which runs fully
offline (no workspace, no auth). We deliberately do not commit the schema to avoid a
duplicated, drift-prone copy — CI installs the Databricks CLI and generates it fresh.
"""

import json
import os
import re
import shutil
import subprocess
import warnings
from pathlib import Path

import jsonschema
import pytest
import yaml

TEST_DATA = Path(__file__).resolve().parent.parent / "test_data"


def _require_databricks_cli() -> str:
    """Resolve the Databricks CLI, deciding fail-vs-skip based on the environment.

    In CI the CLI must be present (the integration workflow installs it via
    databricks/setup-cli), so a missing CLI is a hard failure that surfaces a broken
    pipeline rather than a silent skip. Locally, where contributors may not have the CLI,
    the tests skip with a clear reason instead of forcing everyone to install it.

    This runs inside a fixture (not at import time) so that merely importing the module —
    e.g. pylint's pytest plugin enumerating fixtures, or pytest collection in the build
    workflow, which does not install the CLI — never trips the requirement.
    """
    cli = shutil.which("databricks")
    if cli is not None:
        return cli
    message = (
        "the 'databricks' CLI is required to generate the bundle schema but was not found "
        "on PATH. The CI workflow must install it (databricks/setup-cli)."
    )
    # pytest.fail/skip raise, so this never falls through; the explicit raise keeps the
    # control flow unambiguous for static analysis.
    if os.environ.get("CI"):
        raise pytest.fail.Exception(message)
    raise pytest.skip.Exception(message)


def _job_spec_fixtures() -> list[Path]:
    """All test_data YAML fixtures that contain a `resources` block to validate.

    The bare template (`job_definition_template.yaml`) is included too — it is a valid
    partial bundle and should also conform to the schema.
    """
    fixtures = []
    for path in sorted(TEST_DATA.glob("job_definition*.yaml")):
        with open(path, "r", encoding="utf-8") as file:
            content = yaml.safe_load(file)
        if isinstance(content, dict) and "resources" in content:
            fixtures.append(path)
    return fixtures


def _neutralize_unsupported_regex(node):
    """Recursively rewrite ``pattern`` regexes that Python's ``re`` cannot compile.

    The Databricks CLI emits ECMA-262 patterns using Unicode property escapes such as
    ``\\p{L}`` (letters) and ``\\p{N}`` (numbers) — e.g. the ``${var.name}`` interpolation
    pattern — which Python's ``re`` module rejects with ``bad escape``, crashing jsonschema
    when a matching string field is validated. We translate those escapes to their Python
    equivalents so the pattern still means the same thing.

    We translate rather than delete or blanket-replace with ``.*`` because ``pattern``
    discriminates ``oneOf`` branches (e.g. ``git_provider`` is *either* a known-provider
    enum *or* a ``${var...}`` reference). Collapsing the pattern to ``.*`` would make a
    literal like ``gitHub`` match both branches and cause a spurious ``oneOf`` failure.
    """
    if isinstance(node, dict):
        return {
            key: (_translate_pattern(value) if key == "pattern" else _neutralize_unsupported_regex(value))
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [_neutralize_unsupported_regex(item) for item in node]
    return node


def _translate_pattern(value):
    """Translate ECMA Unicode property escapes to Python-compatible character classes.

    ``\\p{L}`` -> ``[^\\W\\d_]`` (any letter) and ``\\p{N}`` -> ``\\d`` (any digit). Falls
    back to a permissive ``.*`` only if the result still cannot compile, so a genuinely
    exotic future pattern degrades gracefully instead of crashing the suite.
    """
    if not isinstance(value, str):
        return value
    translated = value.replace(r"\p{L}", r"[^\W\d_]").replace(r"\p{N}", r"\d")
    try:
        with warnings.catch_warnings():
            # Translating `[\p{L}\p{N}]` yields a nested set `[[^\W\d_]\d]`, which Python
            # accepts but warns about; the pattern still means "letters or digits".
            warnings.simplefilter("ignore", FutureWarning)
            re.compile(translated)
    except re.error:
        return ".*"
    return translated


@pytest.fixture(scope="module")
def bundle_validator() -> jsonschema.protocols.Validator:
    """Generate the DAB JSON schema via the Databricks CLI and build a validator."""
    cli = _require_databricks_cli()
    result = subprocess.run(  # noqa: S603
        [cli, "bundle", "schema"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"`databricks bundle schema` failed:\n{result.stderr}"
    schema = _neutralize_unsupported_regex(json.loads(result.stdout))
    # The bundle schema declares no `$schema`. Draft 2020-12, which `validator_for` would
    # pick by default, reports spurious `oneOf` failures on this schema; Draft 7 still
    # resolves the nested `$ref`s and catches real errors (unknown fields, wrong types).
    return jsonschema.Draft7Validator(schema)


def test_test_data_fixtures_exist():
    """Guard against silently validating nothing if the fixtures move or are renamed."""
    assert _job_spec_fixtures(), "expected at least one job_definition*.yaml fixture in tests/test_data"


@pytest.mark.parametrize("fixture", _job_spec_fixtures(), ids=lambda p: p.name)
def test_fixture_is_valid_dab(fixture, bundle_validator):
    """Every committed job spec fixture conforms to the Databricks Asset Bundle schema."""
    with open(fixture, "r", encoding="utf-8") as file:
        spec = yaml.safe_load(file)

    errors = sorted(bundle_validator.iter_errors(spec), key=lambda e: list(e.path))
    assert not errors, "invalid DAB in {}:\n{}".format(
        fixture.name,
        "\n".join(f"  at {list(e.path)}: {e.message}" for e in errors),
    )
