"""Integration tests that exercise the *installed* ``databricks_dbt_factory`` console
script as a real subprocess.

Unlike ``tests/test_app.py`` (which calls ``main()`` in-process), these tests run the
entry point exactly as an end user would after ``pip install``. That way they also catch
packaging regressions the in-process tests cannot see, e.g. a broken ``[project.scripts]``
entry point or a data file (the runner notebook) missing from the wheel.

No Databricks workspace is required: the CLI only reads the dbt manifest and a template
job spec from ``tests/test_data`` and writes a generated spec, which we compare against the
committed golden files.
"""

import shutil
import subprocess
import sys
from pathlib import Path

import yaml

TEST_DATA = Path(__file__).resolve().parent.parent / "test_data"

_CLI_NAME = "databricks_dbt_factory"


def _resolve_cli() -> str:
    """Locate the installed console script and FAIL if it is missing.

    We deliberately do not skip when the CLI is absent: the whole point of these tests is
    to verify the package installs a working entry point, so a missing script must surface
    as a red test in CI, not a silent skip. We look next to the running interpreter first
    (the venv bin dir hatch installs into) and fall back to PATH.
    """
    candidate = Path(sys.executable).parent / _CLI_NAME
    if candidate.exists():
        return str(candidate)
    on_path = shutil.which(_CLI_NAME)
    assert on_path is not None, (
        f"the '{_CLI_NAME}' console script is not installed for {sys.executable}. "
        "Run these tests via `make integration` (or install the package first) so the "
        "entry point exists — skipping would hide a packaging regression."
    )
    return on_path


CLI = _resolve_cli()


def _run_cli(*args: str) -> subprocess.CompletedProcess:
    """Invoke the installed CLI as a subprocess and fail loudly on a non-zero exit."""
    result = subprocess.run(  # noqa: S603
        [CLI, *args],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (
        result.returncode == 0
    ), f"CLI failed (exit {result.returncode})\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    return result


def _load(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def test_cli_default_args_matches_golden_spec(tmp_path):
    """The installed CLI generates the expected default job spec from test_data."""
    target = tmp_path / "job_definition.yaml"

    _run_cli(
        "--dbt-manifest-path",
        str(TEST_DATA / "manifest.json"),
        "--input-job-spec-path",
        str(TEST_DATA / "job_definition_template.yaml"),
        "--target-job-spec-path",
        str(target),
    )

    assert _load(target) == _load(TEST_DATA / "job_definition_default.yaml")


def test_cli_notebook_mode_packages_and_copies_runner(tmp_path):
    """In notebook mode the CLI copies the packaged runner notebook next to the spec.

    This asserts the runner notebook data file is actually shipped in the installed
    package (resolved via importlib.resources at runtime), which the in-process tests
    cannot guarantee.
    """
    target = tmp_path / "job_definition.yaml"

    _run_cli(
        "--dbt-manifest-path",
        str(TEST_DATA / "manifest.json"),
        "--input-job-spec-path",
        str(TEST_DATA / "job_definition_template.yaml"),
        "--target-job-spec-path",
        str(target),
        "--task-type",
        "notebook",
    )

    copied_runner = tmp_path / "run_dbt_command.py"
    assert copied_runner.exists(), "runner notebook should be copied next to the generated spec"
    assert "dbtRunner" in copied_runner.read_text(encoding="utf-8"), "copied file should be the packaged runner"

    tasks = _load(target)["resources"]["jobs"]["dbt_sql_job"]["tasks"]
    for task in tasks:
        assert task["notebook_task"]["notebook_path"] == "./run_dbt_command.py"


def test_cli_help_runs():
    """The entry point is wired correctly and `--help` exits successfully."""
    result = subprocess.run(  # noqa: S603
        [CLI, "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "--dbt-manifest-path" in result.stdout


def test_cli_missing_required_args_fails():
    """Invoked with no arguments the CLI exits non-zero (argparse error)."""
    result = subprocess.run(  # noqa: S603
        [CLI],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "usage" in (result.stderr + result.stdout).lower()
