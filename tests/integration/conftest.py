"""Shared fixtures for the integration tests.

The Databricks Asset Bundle (DAB) schema validator is defined here so multiple test
modules (fixture validation and the end-to-end example) can reuse it without duplicating
the CLI invocation or the schema-normalisation logic.
"""

import json
import os
import re
import shutil
import subprocess
import warnings

import jsonschema
import pytest


def require_databricks_cli() -> str:
    """Resolve the Databricks CLI, deciding fail-vs-skip based on the environment.

    In CI the CLI must be present (the integration workflow installs it via
    databricks/setup-cli), so a missing CLI is a hard failure that surfaces a broken
    pipeline rather than a silent skip. Locally, where contributors may not have the CLI,
    the tests skip with a clear reason instead of forcing everyone to install it.

    This runs inside a fixture (not at import time) so that merely importing a test module —
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


@pytest.fixture(scope="session")
def bundle_validator() -> jsonschema.protocols.Validator:
    """Generate the DAB JSON schema via the Databricks CLI and build a validator."""
    cli = require_databricks_cli()
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
