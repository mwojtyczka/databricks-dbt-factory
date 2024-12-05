# pylint: disable=duplicate-code
from unittest.mock import MagicMock
import pytest
from databricks_dbt_factory.DatabricksDbtFactory import DatabricksDbtFactory
from databricks_dbt_factory.FileHandler import FileHandler


@pytest.fixture
def mock_file_handler():
    mock_handler = MagicMock(spec=FileHandler)
    mock_handler.read.return_value = {"metadata": "value"}
    return mock_handler


def test_generate_job_definition(mock_file_handler):
    factory = DatabricksDbtFactory(file_handler=mock_file_handler)
    job_definition = factory.generate_job_definition("dummy_path")

    expected_job_definition = {
        "name": "example_job",
        "new_cluster": {"spark_version": "7.3.x-scala2.12", "node_type_id": "i3.xlarge", "num_workers": 2},
        "libraries": [
            {"pypi": {"package": "requests"}},
            {"maven": {"coordinates": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"}},
        ],
        "spark_conf": {"spark.speculation": "true"},
        "notebook_task": {"notebook_path": "/Users/example@example.com/ExampleNotebook"},
    }

    assert job_definition == expected_job_definition
    mock_file_handler.read.assert_called_once_with("dummy_path")
