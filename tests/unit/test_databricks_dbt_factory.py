import os
from tempfile import NamedTemporaryFile
from pathlib import Path
import yaml


BASE_PATH = str(Path(__file__).resolve().parent.parent)


def cleanup_file(file_path: str):
    """Utility to remove files if they exist."""
    if os.path.exists(file_path):
        os.remove(file_path)


def test_create_job_definition_and_save(file_handler, databricks_dbt_factory):
    """Test job definition generation and saving to file."""
    dbt_manifest_path = BASE_PATH + "/test_data/manifest.json"
    input_job_definition_path = BASE_PATH + "/test_data/job_definition.yaml"
    expected_job_definition_path = BASE_PATH + "/test_data/job_definition.yaml"

    with NamedTemporaryFile(suffix=".yaml", delete=False) as temp_file:
        actual_job_definition_path = temp_file.name

    try:
        databricks_dbt_factory.create_job_tasks_and_update(
            dbt_manifest_path, input_job_definition_path, actual_job_definition_path
        )

        with open(expected_job_definition_path, "r", encoding="utf-8") as file:
            expected_job_definition = yaml.safe_load(file)

        with open(actual_job_definition_path, "r", encoding="utf-8") as file:
            job_definition = yaml.safe_load(file)

        assert job_definition == expected_job_definition
    finally:
        cleanup_file(actual_job_definition_path)


def test_generate(file_handler, databricks_dbt_factory):
    """Test job definition generation and saving to file."""
    dbt_manifest_path = BASE_PATH + "/test_data/manifest.json"
    job_definition_path = BASE_PATH + "/test_data/job_definition_template.yaml"
    destgination_job_definition_path = "job_definition.yaml"

    databricks_dbt_factory.create_job_tasks_and_update(dbt_manifest_path, job_definition_path,
                                                       destgination_job_definition_path)
