"""End-to-end test of the documented example, without a live Databricks workspace.

The project README walks through an end-to-end flow against the companion
`mwojtyczka/dbt-demo` project: compile the dbt project to a manifest, run the factory CLI
to generate a Databricks Workflow spec, then deploy and run it. The only steps that need a
live workspace are `dbt compile` (which needs a warehouse connection) and the final
deploy/run. Everything in between is offline.

This test exercises that offline middle: it runs the installed `databricks_dbt_factory`
CLI with the *same arguments the README documents* (step 5 of the "End-to-end example"),
using the committed dbt manifest — which is itself the frozen output of `dbt compile` on
the demo project — and then validates the freshly generated spec against the real DAB
schema. It deliberately does not clone dbt-demo: the manifest fixture captures that
external dependency's output, so the test stays hermetic and offline.
"""

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

TEST_DATA = Path(__file__).resolve().parent.parent / "test_data"

CLI = shutil.which("databricks_dbt_factory")


def _run_factory(target_path: Path, *extra_args: str) -> None:
    """Run the factory CLI with the README end-to-end example arguments."""
    assert CLI is not None, "databricks_dbt_factory console script must be installed (run via `make integration`)"
    result = subprocess.run(  # noqa: S603
        [
            CLI,
            "--dbt-manifest-path",
            str(TEST_DATA / "manifest.json"),
            "--input-job-spec-path",
            str(TEST_DATA / "job_definition_template.yaml"),
            "--target-job-spec-path",
            str(target_path),
            "--target",
            "${bundle.target}",
            "--project-directory",
            "../",
            "--profiles-directory",
            ".",
            "--environment-key",
            "Default",
            "--new-job-name",
            "dbt_sql_job_explicit_tasks",
            *extra_args,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"factory CLI failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"


@pytest.mark.parametrize(
    "extra_args, description",
    [
        pytest.param((), "dbt task type", id="dbt-task"),
        pytest.param(("--task-type", "notebook"), "notebook task type", id="notebook-task"),
    ],
)
def test_readme_example_generates_valid_bundle(extra_args, description, bundle_validator, tmp_path):
    """The documented end-to-end command produces a spec that is a valid Databricks bundle."""
    target = tmp_path / "dbt_sql_job_explicit_tasks.yml"

    _run_factory(target, *extra_args)

    assert target.exists(), f"factory did not write the target spec for {description}"
    with open(target, "r", encoding="utf-8") as file:
        spec = yaml.safe_load(file)

    # The generated job is renamed via --new-job-name; confirm the CLI honoured it.
    assert "dbt_sql_job_explicit_tasks" in spec["resources"]["jobs"], "expected the renamed job in the generated spec"

    errors = sorted(bundle_validator.iter_errors(spec), key=lambda e: list(e.path))
    assert not errors, "generated spec ({}) is not a valid DAB:\n{}".format(
        description,
        "\n".join(f"  at {list(e.path)}: {e.message}" for e in errors),
    )
