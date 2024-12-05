import yaml
import os
from tempfile import NamedTemporaryFile


def cleanup_file(file_path: str):
    """Utility to remove files if they exist."""
    if os.path.exists(file_path):
        os.remove(file_path)


# Tests
def test_generate_job_definition(file_handler, databricks_dbt_factory):
    """Test job definition generation without saving to file."""
    dbt_manifest_path = "../test_data/manifest.json"
    expected_job_definition_path = "../test_data/job_definition.yaml"

    dbt_manifest = file_handler.read_dbt_manifest(dbt_manifest_path)
    job_definition = databricks_dbt_factory.generate_job_definition(dbt_manifest, "dbt_job")

    with open(expected_job_definition_path, "r", encoding="utf-8") as file:
        expected_job_definition = yaml.safe_load(file)

    assert job_definition == expected_job_definition


def test_generate_job_definition_and_save(file_handler, databricks_dbt_factory):
    """Test job definition generation and saving to file."""
    dbt_manifest_path = "../test_data/manifest.json"
    expected_job_definition_path = "../test_data/job_definition.yaml"

    with NamedTemporaryFile(suffix=".yaml", delete=False) as temp_file:
        actual_job_definition_path = temp_file.name

    try:
        databricks_dbt_factory.generate_job_definition_and_save(
            dbt_manifest_path, actual_job_definition_path, "dbt_job"
        )

        with open(expected_job_definition_path, "r", encoding="utf-8") as file:
            expected_job_definition = yaml.safe_load(file)

        with open(actual_job_definition_path, "r", encoding="utf-8") as file:
            job_definition = yaml.safe_load(file)

        assert job_definition == expected_job_definition
    finally:
        cleanup_file(actual_job_definition_path)
