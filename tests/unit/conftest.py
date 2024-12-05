import pytest
from databricks_dbt_factory.FileHandler import FileHandler
from databricks_dbt_factory.DatabricksDbtFactory import DatabricksDbtFactory


@pytest.fixture
def file_handler():
    return FileHandler()


@pytest.fixture
def databricks_dbt_factory(file_handler):
    return DatabricksDbtFactory(file_handler)
