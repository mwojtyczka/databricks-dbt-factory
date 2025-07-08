import os
from tempfile import NamedTemporaryFile
from pathlib import Path
import pytest
import yaml


BASE_PATH = str(Path(__file__).resolve().parent)


def test_create_job_spec_and_update(dbt_factory):
    run_job_spec_test(
        dbt_factory,
        BASE_PATH + "/test_data/job_definition_no_deps.yaml",
    )


def test_create_job_spec_and_update_with_dbt_deps(dbt_factory_with_deps):
    run_job_spec_test(
        dbt_factory_with_deps,
        BASE_PATH + "/test_data/job_definition.yaml",
    )


def test_create_job_spec_and_update_with_selected_dbt_deps(dbt_factory_with_deps_selected):
    run_job_spec_test(
        dbt_factory_with_deps_selected,
        BASE_PATH + "/test_data/job_definition_deps_selected.yaml",
    )


def run_job_spec_test(dbt_factory, expected_job_definition_path):
    """Helper function to test databricks job definition generation."""
    dbt_manifest_path = BASE_PATH + "/test_data/manifest.json"
    input_job_definition_path = BASE_PATH + "/test_data/job_definition_template.yaml"

    with NamedTemporaryFile(suffix=".yaml", delete=False) as temp_file:
        actual_job_definition_path = temp_file.name

    try:
        dbt_factory.create_tasks_and_update_job_spec(
            dbt_manifest_path, input_job_definition_path, actual_job_definition_path
        )

        with open(expected_job_definition_path, "r", encoding="utf-8") as file:
            expected_job_definition = yaml.safe_load(file)

        with open(actual_job_definition_path, "r", encoding="utf-8") as file:
            job_definition = yaml.safe_load(file)

        assert job_definition == expected_job_definition
    finally:
        if os.path.exists(actual_job_definition_path):
            os.remove(actual_job_definition_path)


@pytest.mark.skip("Manual testing")
def test_generate(databricks_dbt_factory):
    """Test job definition generation and saving to file."""
    dbt_manifest_path = BASE_PATH + "/test_data/manifest.json"
    job_definition_path = BASE_PATH + "/test_data/job_definition_template.yaml"
    destination_job_definition_path = "job_definition.yaml"

    databricks_dbt_factory.create_tasks_and_update_job_spec(
        dbt_manifest_path, job_definition_path, destination_job_definition_path, "new_job_name"
    )
