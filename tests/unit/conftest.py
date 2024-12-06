import pytest
from databricks_dbt_factory.FileHandler import FileHandler
from databricks_dbt_factory.DatabricksDbtFactory import DatabricksDbtFactory
from databricks_dbt_factory.TaskFactory import ModelTaskFactory, SnapshotTaskFactory, SeedTaskFactory, TestTaskFactory


@pytest.fixture
def file_handler():
    return FileHandler()


@pytest.fixture
def databricks_dbt_factory(file_handler):
    task_factories = {
        'model': ModelTaskFactory(),
        'snapshot': SnapshotTaskFactory(),
        'seed': SeedTaskFactory(),
        'test': TestTaskFactory(),
    }
    return DatabricksDbtFactory(file_handler, task_factories)
