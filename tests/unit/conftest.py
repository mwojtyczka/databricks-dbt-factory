import pytest
from databricks_dbt_factory.FileHandler import FileHandler
from databricks_dbt_factory.DatabricksDbtFactory import DatabricksDbtFactory
from databricks_dbt_factory.Task import TaskOptions
from databricks_dbt_factory.TaskFactory import ModelTaskFactory, SnapshotTaskFactory, SeedTaskFactory, TestTaskFactory


@pytest.fixture
def file_handler():
    return FileHandler()


@pytest.fixture
def databricks_dbt_factory(file_handler):
    task_options = TaskOptions(warehouse_id="475b94ddc7cd5211")
    dbt_options = "--target dev --profiles-dir ."
    task_factories = {
        'model': ModelTaskFactory(task_options, dbt_options),
        'snapshot': SnapshotTaskFactory(task_options, dbt_options),
        'seed': SeedTaskFactory(task_options, dbt_options),
        'test': TestTaskFactory(task_options, dbt_options),
    }
    return DatabricksDbtFactory(file_handler, task_factories)
