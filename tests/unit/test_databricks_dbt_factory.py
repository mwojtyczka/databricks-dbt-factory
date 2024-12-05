import yaml
import os
from databricks_dbt_factory.DatabricksDbtFactory import DatabricksDbtFactory
from databricks_dbt_factory.FileHandler import FileHandler


def test_generate_job_definition():
    file_handler = FileHandler()
    manifest = file_handler.read_manifest("../test_data/manifest.json")
    job_definition = DatabricksDbtFactory(file_handler).generate_job_definition(manifest, "dbt_job")

    with open("../test_data/job_definition.yaml", "r") as file:
        expected_job_definition = yaml.safe_load(file)

    assert job_definition == expected_job_definition


def test_generate_job_definition_and_save():
    factory = DatabricksDbtFactory(FileHandler())
    actual_job_definition_path = "job_definition.yaml"
    factory.generate_job_definition_and_save(
        "../test_data/manifest.json", actual_job_definition_path, "dbt_job"
    )

    with open("../test_data/job_definition.yaml", "r") as file:
        expected_job_definition = yaml.safe_load(file)

    with open(actual_job_definition_path, "r") as file:
        job_definition = yaml.safe_load(file)

    os.remove(actual_job_definition_path)

    assert job_definition == expected_job_definition
