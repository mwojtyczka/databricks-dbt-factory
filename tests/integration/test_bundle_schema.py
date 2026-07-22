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
import shutil
import subprocess
from pathlib import Path

import jsonschema
import pytest
import yaml

TEST_DATA = Path(__file__).resolve().parent.parent / "test_data"

_DATABRICKS_CLI = shutil.which("databricks")

# In CI the Databricks CLI must be present (the workflow installs it), so a missing CLI is
# a hard failure that surfaces a broken pipeline. Locally, where contributors may not have
# the CLI, we skip with a clear reason instead of forcing everyone to install it.
if _DATABRICKS_CLI is None:
    if os.environ.get("CI"):
        raise RuntimeError(
            "the 'databricks' CLI is required to generate the bundle schema but was not "
            "found on PATH. The CI workflow must install it (databricks/setup-cli)."
        )
    pytestmark = pytest.mark.skip(reason="databricks CLI not installed; skipping bundle schema validation locally")


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


@pytest.fixture(scope="module")
def bundle_validator() -> jsonschema.protocols.Validator:
    """Generate the DAB JSON schema via the Databricks CLI and build a validator."""
    assert _DATABRICKS_CLI is not None  # guaranteed: CI raises above, local skips
    result = subprocess.run(  # noqa: S603
        [_DATABRICKS_CLI, "bundle", "schema"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"`databricks bundle schema` failed:\n{result.stderr}"
    schema = json.loads(result.stdout)
    validator_cls = jsonschema.validators.validator_for(schema)
    validator_cls.check_schema(schema)
    return validator_cls(schema)


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
