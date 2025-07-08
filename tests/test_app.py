import os
from tempfile import NamedTemporaryFile
from pathlib import Path
import yaml

from databricks_dbt_factory.main import main

BASE_PATH = str(Path(__file__).resolve().parent)


def test_main_given_default_args(monkeypatch):
    """Test the main function for job spec generation."""
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
            "true",
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
            "--run-tests",
            "true",
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
