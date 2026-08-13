import argparse
import hashlib
import json
import os
import re
import shlex
import stat
from dataclasses import FrozenInstanceError
from tempfile import NamedTemporaryFile
from pathlib import Path
import pytest
import yaml

import databricks_dbt_factory.main as main_module
from databricks_dbt_factory import file_io
from databricks_dbt_factory.__version__ import __version__
from databricks_dbt_factory.main import main, parse_args
from databricks_dbt_factory.task_factory import validate_extra_dbt_options

BASE_PATH = str(Path(__file__).resolve().parent)


def prepare_runner_notebook(target: Path, project_directory: str | None):
    return main_module._prepare_runner_notebook(  # pylint: disable=protected-access
        target,
        project_directory,
    )


@pytest.mark.parametrize("target", ["qa environment", "-sprod"])
def test_build_dbt_options_preserves_a_nonempty_target_name(target):
    args = argparse.Namespace(target=target, extra_dbt_command_options="--fail-fast")

    dbt_options = main_module.build_dbt_options(args)

    assert shlex.split(dbt_options) == ["--target", target, "--fail-fast"]


def test_build_dbt_options_rejects_an_empty_dedicated_target():
    args = argparse.Namespace(target="", extra_dbt_command_options="--fail-fast")

    with pytest.raises(ValueError, match="target requires a nonempty value"):
        main_module.build_dbt_options(args)


def _filesystem_is_case_sensitive(directory: Path) -> bool:
    probe = directory / "case_probe"
    probe.write_bytes(b"probe")
    try:
        return not probe.with_name(probe.name.upper()).exists()
    finally:
        probe.unlink()


@pytest.mark.parametrize(
    ("project_directory", "destination_parent", "relative_prefix", "at_project_root"),
    [
        pytest.param(None, "resources", "./", False, id="next-to-spec"),
        pytest.param("../", ".", "../", True, id="project-root"),
    ],
)
def test_prepare_runner_notebook_uses_full_content_digest(
    tmp_path, project_directory, destination_parent, relative_prefix, at_project_root
):
    spec_dir = tmp_path / "resources"
    spec_dir.mkdir()
    target = spec_dir / "job.yaml"

    artifact = prepare_runner_notebook(target.resolve(), project_directory)

    digest = hashlib.sha256(artifact.content).hexdigest()
    assert re.fullmatch(r"[0-9a-f]{64}", digest)
    assert artifact.destination == (tmp_path / destination_parent / f"run_dbt_command_{digest}.py").resolve()
    assert artifact.notebook_path == f"{relative_prefix}run_dbt_command_{digest}.py"
    assert artifact.at_project_root is at_project_root
    assert not artifact.destination.exists()
    with pytest.raises(FrozenInstanceError):
        setattr(artifact, "notebook_path", "changed.py")


def test_main_dbt_task_type(monkeypatch):
    """Test the main function generating native dbt_task specs."""
    dbt_manifest_path = BASE_PATH + "/test_data/manifest.json"
    input_job_spec_path = BASE_PATH + "/test_data/job_definition_template.yaml"
    expected_job_definition_path = BASE_PATH + "/test_data/job_definition_default.yaml"

    with NamedTemporaryFile(suffix=".yaml", delete=False) as temp_file:
        target_job_spec_path = temp_file.name

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            dbt_manifest_path,
            "--input-job-spec-path",
            input_job_spec_path,
            "--target-job-spec-path",
            target_job_spec_path,
            "--task-type",
            "dbt",
        ],
    )

    try:
        main()

        with open(expected_job_definition_path, "r", encoding="utf-8") as file:
            expected_job_definition = yaml.safe_load(file)

        with open(target_job_spec_path, "r", encoding="utf-8") as file:
            job_definition = yaml.safe_load(file)

        assert job_definition == expected_job_definition
    finally:
        if os.path.exists(target_job_spec_path):
            os.remove(target_job_spec_path)


@pytest.mark.parametrize("task_type", ["dbt", "notebook"])
def test_main_dry_run_prints_tasks_and_writes_nothing(monkeypatch, capsys, tmp_path, task_type):
    """--dry-run prints the generated tasks and writes nothing — not the spec, nor (in notebook
    mode) the runner notebook that a real run copies next to it."""
    target_job_spec_path = tmp_path / "out.yaml"

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target_job_spec_path),
            "--task-type",
            task_type,
            "--dry-run",
        ],
    )

    main()

    out = capsys.readouterr().out
    assert "task_key" in out
    # the printed tasks must match the requested task type, not just contain a task_key
    expected_task_field = "notebook_task" if task_type == "notebook" else "dbt_task"
    unexpected_task_field = "dbt_task" if task_type == "notebook" else "notebook_task"
    assert expected_task_field in out
    assert unexpected_task_field not in out
    if task_type == "notebook":
        artifact = prepare_runner_notebook(target_job_spec_path.resolve(), None)
        assert artifact.notebook_path in out
    # dry-run writes nothing: not the spec, nor (in notebook mode) the runner notebook
    assert not list(tmp_path.iterdir())


def test_main_notebook_mode_auto_copies_runner_notebook_next_to_spec(monkeypatch, tmp_path):
    """Without --project-directory, the content-addressed runner is published next to the spec."""
    target_job_spec_path = tmp_path / "job_definition.yaml"
    artifact = prepare_runner_notebook(target_job_spec_path.resolve(), None)

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target_job_spec_path),
            "--task-type",
            "notebook",
        ],
    )

    main()

    copied_notebook = artifact.destination
    assert copied_notebook.exists(), "runner notebook should have been copied next to the job spec"
    assert "dbtRunner" in copied_notebook.read_text(encoding="utf-8"), "copied file should be the packaged runner"

    with open(target_job_spec_path, "r", encoding="utf-8") as file:
        job_definition = yaml.safe_load(file)

    tasks = job_definition["resources"]["jobs"]["dbt_sql_job"]["tasks"]
    for task in tasks:
        assert task["notebook_task"]["notebook_path"] == artifact.notebook_path
        assert task["notebook_task"]["source"] == "WORKSPACE"


@pytest.mark.skipif(os.name == "nt", reason="Windows symlink creation may require elevated privileges")
def test_main_anchors_runner_and_spec_to_one_resolved_destination(monkeypatch, tmp_path):
    first_directory = tmp_path / "first"
    second_directory = tmp_path / "second"
    first_directory.mkdir()
    second_directory.mkdir()
    linked_directory = tmp_path / "current"
    linked_directory.symlink_to(first_directory, target_is_directory=True)
    target = linked_directory / "job_definition.yaml"
    runner_artifact = prepare_runner_notebook(target.resolve(), None)
    real_render_job_spec = main_module.render_job_spec

    def render_then_retarget(*args, **kwargs):
        rendered = real_render_job_spec(*args, **kwargs)
        linked_directory.unlink()
        linked_directory.symlink_to(second_directory, target_is_directory=True)
        return rendered

    monkeypatch.setattr(main_module, "render_job_spec", render_then_retarget)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
        ],
    )

    main()

    anchored_spec = first_directory / target.name
    assert anchored_spec.exists()
    assert runner_artifact.destination.read_bytes() == runner_artifact.content
    assert not (second_directory / target.name).exists()
    assert not list(second_directory.glob("run_dbt_command_*.py"))
    tasks = yaml.safe_load(anchored_spec.read_text(encoding="utf-8"))["resources"]["jobs"]["dbt_sql_job"]["tasks"]
    assert all(task["notebook_task"]["notebook_path"] == runner_artifact.notebook_path for task in tasks)


def test_main_notebook_mode_auto_copies_runner_notebook_to_project_root(monkeypatch, tmp_path):
    """A relative --project-directory places the content-addressed runner at the project root."""
    spec_dir = tmp_path / "resources"
    spec_dir.mkdir()
    target_job_spec_path = spec_dir / "job_definition.yaml"
    artifact = prepare_runner_notebook(target_job_spec_path.resolve(), "../")

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target_job_spec_path),
            "--task-type",
            "notebook",
            "--project-directory",
            "../",
        ],
    )

    main()

    copied_notebook = artifact.destination
    assert copied_notebook.exists(), "runner should have been copied to the project root (one level up from the spec)"
    assert not (spec_dir / copied_notebook.name).exists(), "runner should NOT be copied next to the spec in this case"

    with open(target_job_spec_path, "r", encoding="utf-8") as file:
        job_definition = yaml.safe_load(file)

    tasks = job_definition["resources"]["jobs"]["dbt_sql_job"]["tasks"]
    for task in tasks:
        assert task["notebook_task"]["notebook_path"] == artifact.notebook_path
        assert task["notebook_task"]["source"] == "WORKSPACE"
        # With the runner at project root, CWD at runtime = project root. We explicitly
        # pin project_directory to "." so the spec is self-documenting (the user's original
        # "../" would resolve one level too high and has been rewritten).
        assert task["notebook_task"]["base_parameters"]["project_directory"] == "."


def test_auto_copy_rejects_git_source_before_writing(monkeypatch, tmp_path):
    target = tmp_path / "job.yaml"
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
            "--source",
            "GIT",
        ],
    )

    with pytest.raises(SystemExit, match=r"--source GIT.*--notebook-path"):
        main()

    assert not list(tmp_path.iterdir())


def test_caller_managed_notebook_path_may_use_git(monkeypatch, tmp_path):
    target = tmp_path / "job.yaml"
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
            "--notebook-path",
            "./managed_runner.py",
            "--source",
            "GIT",
        ],
    )

    main()

    tasks = yaml.safe_load(target.read_text(encoding="utf-8"))["resources"]["jobs"]["dbt_sql_job"]["tasks"]
    for task in tasks:
        assert task["notebook_task"]["notebook_path"] == "./managed_runner.py"
        assert task["notebook_task"]["source"] == "GIT"
    assert not list(tmp_path.glob("run_dbt_command_*.py"))


def test_auto_copy_preserves_legacy_runner_and_reuses_hashed_runner(monkeypatch, tmp_path):
    target_job_spec_path = tmp_path / "job_definition.yaml"
    artifact = prepare_runner_notebook(target_job_spec_path.resolve(), None)
    legacy = tmp_path / "run_dbt_command.py"
    legacy.write_text("# user managed\n", encoding="utf-8")

    argv = [
        "main.py",
        "--dbt-manifest-path",
        BASE_PATH + "/test_data/manifest.json",
        "--input-job-spec-path",
        BASE_PATH + "/test_data/job_definition_template.yaml",
        "--target-job-spec-path",
        str(target_job_spec_path),
        "--task-type",
        "notebook",
    ]
    monkeypatch.setattr("sys.argv", argv)
    main()

    assert legacy.read_text(encoding="utf-8") == "# user managed\n"
    assert artifact.destination.read_bytes() == artifact.content

    def unexpected_runner_write(*_args, **_kwargs):
        raise AssertionError("an identical content-addressed runner must be reused")

    monkeypatch.setattr(main_module, "atomic_write_bytes", unexpected_runner_write)
    main()

    assert list(tmp_path.glob("run_dbt_command_*.py")) == [artifact.destination]


def test_main_notebook_mode(monkeypatch):
    """Test the main function for notebook task type generation."""
    dbt_manifest_path = BASE_PATH + "/test_data/manifest.json"
    input_job_spec_path = BASE_PATH + "/test_data/job_definition_template.yaml"
    expected_job_definition_path = BASE_PATH + "/test_data/job_definition_notebook_default.yaml"

    with NamedTemporaryFile(suffix=".yaml", delete=False) as temp_file:
        target_job_spec_path = temp_file.name

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            dbt_manifest_path,
            "--input-job-spec-path",
            input_job_spec_path,
            "--target-job-spec-path",
            target_job_spec_path,
            "--task-type",
            "notebook",
            "--notebook-path",
            "./notebooks/dbt_runner.py",
        ],
    )

    try:
        main()

        with open(expected_job_definition_path, "r", encoding="utf-8") as file:
            expected_job_definition = yaml.safe_load(file)

        with open(target_job_spec_path, "r", encoding="utf-8") as file:
            job_definition = yaml.safe_load(file)

        assert job_definition == expected_job_definition
    finally:
        if os.path.exists(target_job_spec_path):
            os.remove(target_job_spec_path)


def test_main_all_args(monkeypatch):
    """Test the main function for job spec generation."""
    dbt_manifest_path = BASE_PATH + "/test_data/manifest.json"
    input_job_spec_path = BASE_PATH + "/test_data/job_definition_template.yaml"
    expected_job_definition_path = BASE_PATH + "/test_data/job_definition_deps_selected.yaml"

    with NamedTemporaryFile(suffix=".yaml", delete=False) as temp_file:
        target_job_spec_path = temp_file.name

    new_job_name = "test_job"
    warehouse_id = "1234567890abcdef"
    schema = "dqx_test"
    catalog = "main"
    profiles_dir = "profiles_dir"
    project_dir = "/project_dir"
    extra_dbt_command_options = '"--upgrade"'

    # Mock command-line arguments
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--new-job-name",
            new_job_name,
            "--dbt-manifest-path",
            dbt_manifest_path,
            "--input-job-spec-path",
            input_job_spec_path,
            "--target-job-spec-path",
            target_job_spec_path,
            "--target",
            "dev",
            "--environment-key",
            "Default",
            "--source",
            "GIT",
            "--enable-dbt-deps",
            "--dbt-tasks-deps",
            "diamonds_prices,second_dbt_model",
            "--warehouse_id",
            warehouse_id,
            "--schema",
            schema,
            "--catalog",
            catalog,
            "--profiles-directory",
            profiles_dir,
            "--project-directory",
            project_dir,
            "--extra-dbt-command-options",
            extra_dbt_command_options,
            "--task-type",
            "dbt",
        ],
    )

    try:
        main()

        with open(expected_job_definition_path, "r", encoding="utf-8") as file:
            expected_job_definition = yaml.safe_load(file)

        with open(target_job_spec_path, "r", encoding="utf-8") as file:
            job_definition = yaml.safe_load(file)

        expected_job_definition = update_spec(
            expected_job_definition,
            new_job_name,
            warehouse_id,
            schema,
            catalog,
            profiles_dir,
            project_dir,
            extra_dbt_command_options,
        )

        assert job_definition == expected_job_definition
    finally:
        if os.path.exists(target_job_spec_path):
            os.remove(target_job_spec_path)


REQUIRED_ARGS = [
    "--dbt-manifest-path",
    "manifest.json",
    "--input-job-spec-path",
    "in.yaml",
    "--target-job-spec-path",
    "out.yaml",
]


def test_version_flag_prints_version_and_exits(monkeypatch, capsys):
    # --version short-circuits argparse (exits 0) before the required args are enforced.
    monkeypatch.setattr("sys.argv", ["main.py", "--version"])
    with pytest.raises(SystemExit) as exc:
        parse_args()

    assert exc.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_extra_dbt_options_help_documents_unambiguous_values(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["main.py", "--help"])

    with pytest.raises(SystemExit) as error:
        parse_args()

    assert error.value.code == 0
    help_text = " ".join(capsys.readouterr().out.split())
    assert "--option=value" in help_text
    assert "reserved short-option prefix" in help_text


def test_explicit_environment_key_with_job_cluster_key_is_rejected(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        ["main.py", *REQUIRED_ARGS, "--job-cluster-key", "foo", "--environment-key", "Default"],
    )
    with pytest.raises(SystemExit):
        parse_args()


def test_job_cluster_key_alone_parses(monkeypatch):
    monkeypatch.setattr("sys.argv", ["main.py", *REQUIRED_ARGS, "--job-cluster-key", "foo"])
    args = parse_args()
    assert args.job_cluster_key == "foo"
    assert args.environment_key is None


def test_environment_key_alone_parses(monkeypatch):
    monkeypatch.setattr("sys.argv", ["main.py", *REQUIRED_ARGS, "--environment-key", "Default"])
    args = parse_args()
    assert args.environment_key == "Default"
    assert args.job_cluster_key is None


def test_boolean_flags_default(monkeypatch):
    monkeypatch.setattr("sys.argv", ["main.py", *REQUIRED_ARGS])
    args = parse_args()
    assert args.run_tests is True  # tests enabled by default
    assert args.bundle_tests is False
    assert args.enable_dbt_deps is False
    assert args.dry_run is False
    assert args.task_type == "notebook"


def test_boolean_flags_toggled(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            *REQUIRED_ARGS,
            "--no-run-tests",
            "--bundle-tests",
            "--enable-dbt-deps",
            "--dry-run",
        ],
    )
    args = parse_args()
    assert args.run_tests is False
    assert args.bundle_tests is True
    assert args.enable_dbt_deps is True
    assert args.dry_run is True


@pytest.mark.parametrize(
    "extra_options",
    [
        pytest.param("--select beta", id="select"),
        pytest.param("--exclude alpha", id="exclude"),
        pytest.param("--resource-type model", id="resource-type"),
        pytest.param("--", id="option-delimiter"),
        pytest.param("{{job.parameters.dbt_options}}", id="dynamic-value-reference"),
    ],
)
def test_main_rejects_selection_changing_extra_options_before_publishing_artifacts(
    monkeypatch, tmp_path, extra_options
):
    target = tmp_path / "job.yaml"
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            f"--extra-dbt-command-options={extra_options}",
        ],
    )

    with pytest.raises(SystemExit, match="selection"):
        main()

    assert not list(tmp_path.iterdir())


def test_main_rejects_an_empty_dedicated_target_before_publishing_artifacts(monkeypatch, tmp_path):
    target = tmp_path / "job.yaml"
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--target",
            "",
        ],
    )

    with pytest.raises(SystemExit, match="target requires a nonempty value"):
        main()

    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    ("extra_options", "parse_context_option", "dedicated_option"),
    [
        pytest.param("--vars '{enable_alpha: false}'", "--vars", None, id="vars"),
        pytest.param("--vars='{enable_alpha: false}'", "--vars", None, id="vars-equals"),
        pytest.param("--profile prod", "--profile", None, id="profile"),
        pytest.param("--profile=prod", "--profile", None, id="profile-equals"),
        pytest.param("--profiles-dir profiles", "--profiles-dir", "--profiles-directory", id="profiles-dir"),
        pytest.param("--profiles-dir=profiles", "--profiles-dir", "--profiles-directory", id="profiles-dir-equals"),
        pytest.param("--project-dir project", "--project-dir", "--project-directory", id="project-dir"),
        pytest.param("--project-dir=project", "--project-dir", "--project-directory", id="project-dir-equals"),
        pytest.param("--target prod", "--target", "--target", id="target"),
        pytest.param("--target=prod", "--target", "--target", id="target-equals"),
        pytest.param("-t prod", "-t", "--target", id="target-short"),
        pytest.param("-tprod", "-t", "--target", id="target-short-attached"),
    ],
)
def test_main_rejects_parse_context_overrides_in_extra_options_before_publishing_artifacts(
    monkeypatch,
    tmp_path,
    extra_options,
    parse_context_option,
    dedicated_option,
):
    target = tmp_path / "job.yaml"
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            f"--extra-dbt-command-options={extra_options}",
        ],
    )

    with pytest.raises(SystemExit) as error:
        main()

    message = str(error.value)
    assert parse_context_option in message
    assert "runtime parse context" in message
    assert "supplied manifest" in message
    if dedicated_option is not None:
        assert f"dedicated {dedicated_option}" in message
    assert not list(tmp_path.iterdir())


def test_notebook_task_type_with_warehouse_id_is_rejected(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            *REQUIRED_ARGS,
            "--task-type",
            "notebook",
            "--notebook-path",
            "/n",
            "--warehouse_id",
            "wh123",
        ],
    )
    with pytest.raises(SystemExit):
        parse_args()


def remove_target_from_spec(expected_job_definition):
    """Remove 'target' key from dbt_task in the expected job definition."""
    spec = dict(expected_job_definition)

    for task in expected_job_definition["resources"]["jobs"]["dbt_sql_job"]["tasks"]:
        if "dbt_task" in task:
            task["dbt_task"].pop("source", None)

    return spec


def update_spec(
    expected_spec: dict,
    new_job_name: str,
    warehouse_id: str,
    schema: str,
    catalog: str,
    profiles_dir: str,
    project_dir: str,
    extra_dbt_command_options: str,
) -> dict:
    """Update the job specification with new parameters."""
    spec = dict(expected_spec)

    # Update job name
    spec["resources"]["jobs"][new_job_name] = spec["resources"]["jobs"].pop("dbt_sql_job")
    spec["resources"]["jobs"][new_job_name]["name"] = new_job_name

    # Add warehouse_id under dbt_task
    for task in spec["resources"]["jobs"][new_job_name]["tasks"]:
        if "dbt_task" in task:
            task["dbt_task"]["schema"] = schema
            task["dbt_task"]["catalog"] = catalog
            task["dbt_task"]["warehouse_id"] = warehouse_id
            task["dbt_task"]["project_directory"] = project_dir
            task["dbt_task"]["profiles_directory"] = profiles_dir

        # Update commands with extra dbt command options
        updated_commands = []
        for command in task["dbt_task"]["commands"]:
            if "--target dev" in command:
                command = command.replace("--target dev", f"--target dev {extra_dbt_command_options}")
            updated_commands.append(command)
        task["dbt_task"]["commands"] = updated_commands

    return spec


def test_failed_generation_writes_no_runner_notebook(monkeypatch, tmp_path):
    """A manifest failure publishes neither the content-addressed runner nor the job spec."""
    target_job_spec_path = tmp_path / "job_definition.yaml"

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            str(tmp_path / "does_not_exist.json"),
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target_job_spec_path),
            "--task-type",
            "notebook",
        ],
    )

    with pytest.raises(SystemExit):
        main()

    assert not list(tmp_path.glob("run_dbt_command_*.py")), "a failed run must not leave a runner notebook behind"
    assert not target_job_spec_path.exists(), "a failed run must not write a job spec"


def test_failed_generation_preserves_an_existing_runner_notebook(monkeypatch, tmp_path):
    """A manifest failure never touches a caller-managed legacy runner."""
    target_job_spec_path = tmp_path / "job_definition.yaml"
    existing_runner = tmp_path / "run_dbt_command.py"
    existing_runner.write_text("# edited by the user\n", encoding="utf-8")

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            str(tmp_path / "does_not_exist.json"),
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target_job_spec_path),
            "--task-type",
            "notebook",
        ],
    )

    with pytest.raises(SystemExit):
        main()

    assert existing_runner.read_text(encoding="utf-8") == "# edited by the user\n"
    assert not list(tmp_path.glob("run_dbt_command_*.py"))


@pytest.mark.parametrize(
    ('spec_body', 'note'),
    [
        pytest.param(None, 'the input spec does not exist', id='missing-input-spec'),
        pytest.param('not_a_job: true\n', 'the input spec holds no jobs', id='malformed-input-spec'),
    ],
)
def test_input_spec_failure_preserves_an_existing_runner(monkeypatch, tmp_path, spec_body, note):
    """Input-spec preparation completes before any runner is published."""
    assert note
    spec_dir = tmp_path / "spec"
    spec_dir.mkdir()
    input_path = spec_dir / "in.yaml"
    if spec_body is not None:
        input_path.write_text(spec_body, encoding="utf-8")
    target_path = spec_dir / "out.yaml"
    existing_runner = tmp_path / "run_dbt_command.py"
    existing_runner.write_text("# edited by the user\n", encoding="utf-8")

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            str(input_path),
            "--target-job-spec-path",
            str(target_path),
            "--task-type",
            "notebook",
            "--project-directory",
            "../",
        ],
    )

    with pytest.raises(SystemExit):
        main()

    assert existing_runner.read_text(encoding="utf-8") == "# edited by the user\n"
    assert not list(tmp_path.glob("run_dbt_command_*.py"))
    assert not target_path.exists(), "a failed run must not write a target spec"


@pytest.mark.parametrize(
    "runner_kind",
    [
        "tampered",
        pytest.param(
            "symlink",
            marks=pytest.mark.skipif(
                os.name == "nt", reason="Windows symlink creation may require elevated privileges"
            ),
        ),
        "directory",
    ],
)
def test_invalid_hashed_runner_is_rejected_without_updating_spec(monkeypatch, tmp_path, runner_kind):
    target = tmp_path / "job.yaml"
    target.write_bytes(b"original spec\n")
    target.chmod(0o604)
    original_mode = stat.S_IMODE(target.stat().st_mode)
    artifact = prepare_runner_notebook(target.resolve(), None)
    if runner_kind == "tampered":
        artifact.destination.write_bytes(b"tampered\n")
    elif runner_kind == "directory":
        artifact.destination.mkdir()
    else:
        linked = tmp_path / "linked_runner.py"
        linked.write_bytes(artifact.content)
        os.symlink(linked, artifact.destination)

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
        ],
    )

    with pytest.raises(SystemExit, match="Runner target"):
        main()

    assert target.read_bytes() == b"original spec\n"
    assert stat.S_IMODE(target.stat().st_mode) == original_mode
    if runner_kind == "tampered":
        assert artifact.destination.read_bytes() == b"tampered\n"


def test_runner_and_spec_destination_collision_is_rejected_before_writing(monkeypatch, tmp_path):
    placeholder = tmp_path / "job.yaml"
    artifact = prepare_runner_notebook(placeholder.resolve(), None)
    target = artifact.destination
    target.write_bytes(artifact.content)
    target.chmod(0o604)
    original_mode = stat.S_IMODE(target.stat().st_mode)

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
        ],
    )

    with pytest.raises(SystemExit, match="runner and job spec destinations must be different"):
        main()

    assert target.read_bytes() == artifact.content
    assert stat.S_IMODE(target.stat().st_mode) == original_mode
    assert list(tmp_path.iterdir()) == [target]
    assert not list(tmp_path.glob(".*.tmp"))


def test_case_alias_collision_is_rejected_after_runner_publication(monkeypatch, tmp_path):
    if _filesystem_is_case_sensitive(tmp_path):
        pytest.skip("requires a case-insensitive filesystem to reproduce the destination alias")

    placeholder = tmp_path / "job.yaml"
    artifact = prepare_runner_notebook(placeholder.resolve(), None)
    target = artifact.destination.with_name(artifact.destination.name.upper())
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
        ],
    )

    with pytest.raises(SystemExit, match="runner and job spec destinations must be different"):
        main()

    assert artifact.destination.read_bytes() == artifact.content
    assert target.read_bytes() == artifact.content


def test_hard_link_collision_is_rejected_after_runner_publication(monkeypatch, tmp_path):
    placeholder = tmp_path / "job.yaml"
    artifact = prepare_runner_notebook(placeholder.resolve(), None)
    artifact.destination.write_bytes(artifact.content)
    target = tmp_path / "job.yaml"
    try:
        os.link(artifact.destination, target)
    except OSError as error:
        pytest.skip(f"hard links are unavailable on this test filesystem: {error}")

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
        ],
    )

    with pytest.raises(SystemExit, match="runner and job spec destinations must be different"):
        main()

    assert artifact.destination.read_bytes() == artifact.content
    assert target.read_bytes() == artifact.content


def test_runner_publication_failure_leaves_existing_spec_and_no_temp(monkeypatch, tmp_path):
    target = tmp_path / "job.yaml"
    target.write_bytes(b"original spec\n")
    target.chmod(0o604)
    original_mode = stat.S_IMODE(target.stat().st_mode)
    artifact = prepare_runner_notebook(target.resolve(), None)
    real_replace = file_io.os.replace

    def fail_runner_replace(source, destination):
        if Path(destination) == artifact.destination:
            raise OSError("runner replace failed")
        real_replace(source, destination)

    monkeypatch.setattr(file_io.os, "replace", fail_runner_replace)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
        ],
    )

    with pytest.raises(OSError, match="runner replace failed"):
        main()

    assert target.read_bytes() == b"original spec\n"
    assert stat.S_IMODE(target.stat().st_mode) == original_mode
    assert not artifact.destination.exists()
    assert not list(tmp_path.glob(".*.tmp"))


def test_spec_publication_failure_keeps_old_spec_and_valid_runner(monkeypatch, tmp_path):
    target = tmp_path / "job.yaml"
    target.write_bytes(b"original spec\n")
    target.chmod(0o604)
    original_mode = stat.S_IMODE(target.stat().st_mode)
    artifact = prepare_runner_notebook(target.resolve(), None)
    real_replace = file_io.os.replace

    def fail_spec_replace(source, destination):
        if Path(destination) == target:
            raise OSError("spec replace failed")
        real_replace(source, destination)

    monkeypatch.setattr(file_io.os, "replace", fail_spec_replace)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
        ],
    )

    with pytest.raises(OSError, match="spec replace failed"):
        main()

    assert target.read_bytes() == b"original spec\n"
    assert stat.S_IMODE(target.stat().st_mode) == original_mode
    assert artifact.destination.read_bytes() == artifact.content
    assert not list(tmp_path.glob(".*.tmp"))


@pytest.mark.parametrize(
    "target_kind",
    [
        "directory",
        pytest.param(
            "symlink",
            marks=pytest.mark.skipif(
                os.name == "nt", reason="Windows symlink creation may require elevated privileges"
            ),
        ),
    ],
)
def test_invalid_spec_target_is_rejected_before_runner_publication(monkeypatch, tmp_path, target_kind):
    target = tmp_path / "job.yaml"
    if target_kind == "directory":
        target.mkdir()
    else:
        linked = tmp_path / "linked_spec.yaml"
        linked.write_text("keep\n", encoding="utf-8")
        os.symlink(linked, target)

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
        ],
    )

    with pytest.raises(SystemExit, match="regular non-symlink file"):
        main()

    assert not list(tmp_path.rglob("run_dbt_command_*.py"))


def test_utf8_encoding_failure_occurs_before_runner_publication(monkeypatch, tmp_path):
    target = tmp_path / "job.yaml"
    monkeypatch.setattr(main_module, "render_job_spec", lambda *_args, **_kwargs: "\ud800")
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
        ],
    )

    with pytest.raises(SystemExit, match="can't encode character"):
        main()

    assert not list(tmp_path.glob("run_dbt_command_*.py"))
    assert not target.exists()


def test_task_limit_failure_publishes_no_cli_artifacts(monkeypatch, tmp_path):
    manifest_path = tmp_path / "manifest.json"
    nodes = {
        f"model.pkg.model_{index:04d}": {
            "resource_type": "model",
            "name": f"model_{index:04d}",
            "package_name": "pkg",
            "fqn": ["pkg", f"model_{index:04d}"],
            "original_file_path": f"models/model_{index:04d}.sql",
            "depends_on": {"nodes": []},
        }
        for index in range(1_001)
    }
    manifest_path.write_text(json.dumps({"nodes": nodes}), encoding="utf-8")
    target = tmp_path / "job.yaml"
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            str(manifest_path),
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            "--task-type",
            "notebook",
        ],
    )

    with pytest.raises(SystemExit, match="at most 1,000 tasks"):
        main()

    assert not list(tmp_path.glob("run_dbt_command_*.py"))
    assert not target.exists()


@pytest.mark.parametrize(
    "argv_form",
    ["--extra-dbt-command-options=--", "--extra-dbt-command-options=-- "],
    ids=["bare", "trailing-space"],
)
def test_parse_args_normalizes_a_bare_double_dash_value(monkeypatch, tmp_path, argv_form):
    # On Python 3.10 (in the supported range and in CI's matrix) argparse yields `[]` for
    # `--extra-dbt-command-options=--`, not the string `'--'`; on 3.12 it yields `'--'`. Either way the
    # value must reach `_reserved_selection_option`, whose `token == '--'` check refuses it — a `[]`
    # slipping through would be treated as "no options" and silently drop a refusal the factory owes.
    #
    # Note the trigger is the `=--` form: a bare `--extra-dbt-command-options --` exits 2 on both
    # versions, because argparse reads `--` as the end-of-options marker rather than as the value.
    target = tmp_path / "out.yaml"
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
            argv_form,
        ],
    )

    args = parse_args()

    assert args.extra_dbt_command_options == argv_form.split("=", 1)[1]


def test_validate_extra_dbt_options_needs_a_string_not_an_empty_list():
    # Pins why `parse_args` normalizes `[]` to `'--'`. On Python 3.10 (supported, and in CI's matrix)
    # `--extra-dbt-command-options=--` parses to `[]`; unnormalized that reaches the validator, whose
    # regex raises `TypeError` on a non-string. `main` catches only `ValueError`/`FileNotFoundError`, so
    # the CLI would abort with a traceback rather than refuse `--` with a message.
    #
    # Asserted on every supported version: the validator's contract is "give me a string", independent of
    # which version produces the empty list.
    with pytest.raises(TypeError):
        validate_extra_dbt_options([])

    # The normalized value is refused properly, with a diagnosable message.
    with pytest.raises(ValueError, match="cannot include selection option"):
        validate_extra_dbt_options("--")


def test_parse_args_normalizes_an_empty_list_extra_option(monkeypatch, tmp_path):
    # Python 3.10 (supported, and in CI's matrix) parses `--extra-dbt-command-options=--` to `[]`, while
    # 3.12 yields `'--'`. This test forces the 3.10 shape on any interpreter by making argparse produce a
    # list, so the normalization is covered wherever the suite runs — otherwise the guard is only
    # exercised on 3.10 and a removal passes CI on 3.11/3.12.
    target = tmp_path / "out.yaml"
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
        ],
    )
    real_parse_args = argparse.ArgumentParser.parse_args

    def parse_args_yielding_a_list(self, *args, **kwargs):
        namespace = real_parse_args(self, *args, **kwargs)
        # what 3.10's argparse hands back for `--extra-dbt-command-options=--`
        namespace.extra_dbt_command_options = []
        return namespace

    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", parse_args_yielding_a_list)

    args = parse_args()

    # Must be the string `--`, so `_validate_dbt_options`' regex gets a string and refuses it, rather
    # than raising an uncaught TypeError.
    assert args.extra_dbt_command_options == "--"
    with pytest.raises(ValueError, match="cannot include selection option"):
        validate_extra_dbt_options(args.extra_dbt_command_options)


@pytest.mark.parametrize(
    "argparse_value",
    [
        pytest.param([], id="empty-list"),
        pytest.param(["--"], id="single-element-list"),
        pytest.param(None, id="none"),
    ],
)
def test_parse_args_normalizes_any_non_string_extra_option(monkeypatch, tmp_path, argparse_value):
    # The `--` end-of-options marker is the only way a non-string reaches this namespace, since the
    # argument is `type=str`. 3.10 hands back `[]` for `--extra-dbt-command-options=--`, but the guard
    # must not be pinned to that exact value: any non-string it fails to normalize reaches the validator's
    # regex as a non-string and raises an uncaught `TypeError`, aborting the CLI with a traceback instead
    # of the refusal `--` is owed. Forcing several non-string shapes pins the guard to "not a string"
    # rather than "== []", so a future argparse that returns a different non-string stays safe.
    target = tmp_path / "out.yaml"
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            BASE_PATH + "/test_data/manifest.json",
            "--input-job-spec-path",
            BASE_PATH + "/test_data/job_definition_template.yaml",
            "--target-job-spec-path",
            str(target),
        ],
    )
    real_parse_args = argparse.ArgumentParser.parse_args

    def parse_args_yielding_a_non_string(self, *args, **kwargs):
        namespace = real_parse_args(self, *args, **kwargs)
        namespace.extra_dbt_command_options = argparse_value
        return namespace

    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", parse_args_yielding_a_non_string)

    args = parse_args()

    assert args.extra_dbt_command_options == "--"
    with pytest.raises(ValueError, match="cannot include selection option"):
        validate_extra_dbt_options(args.extra_dbt_command_options)
