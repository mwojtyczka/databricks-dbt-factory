import pytest
from databricks_dbt_factory.FileHandler import FileHandler
from databricks_dbt_factory.DatabricksDbtFactory import DatabricksDbtFactory
from databricks_dbt_factory.DbtTask import DbtTaskOptions
from databricks_dbt_factory.TaskFactory import (
    ModelTaskFactory,
    SnapshotTaskFactory,
    SeedTaskFactory,
    TestTaskFactory,
    DbtDependencyResolver,
)


@pytest.fixture
def file_handler():
    return FileHandler()


@pytest.fixture
def databricks_dbt_factory(file_handler):
    resolver = DbtDependencyResolver()
    task_options = DbtTaskOptions(
        source="GIT",
        environment_key="Default",
    )
    dbt_options = "--target dev"

    task_factories = {
        'model': ModelTaskFactory(resolver, task_options, dbt_options),
        'snapshot': SnapshotTaskFactory(resolver, task_options, dbt_options),
        'seed': SeedTaskFactory(resolver, task_options, dbt_options),
        'test': TestTaskFactory(resolver, task_options, dbt_options),
    }

    return DatabricksDbtFactory(file_handler, task_factories)
