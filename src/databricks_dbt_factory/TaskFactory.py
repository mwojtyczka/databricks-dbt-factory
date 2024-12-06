from abc import ABC, abstractmethod
from enum import Enum

from databricks_dbt_factory.Task import Task, TaskOptions


class DbtNodeTypes(Enum):
    """Enum class to represent dbt node types."""

    MODEL = "model"
    TEST = "test"
    SEED = "seed"
    SNAPSHOT = "snapshot"


class DbtDependencyResolver:
    @staticmethod
    def resolve(node_info: dict, dbt_dependency_types: list[str]) -> list[str]:
        """Common logic for resolving dependencies based on node prefixes."""
        dependencies = node_info.get('depends_on', {}).get('nodes', [])
        resolved_dependencies = []
        for dep in dependencies:
            if any(dep.startswith(dbt_type + ".") for dbt_type in dbt_dependency_types):
                resolved_dependencies.append(dep.replace('.', '_'))
        return resolved_dependencies


class TaskFactory(ABC):
    """Abstract base class for creating tasks."""

    def __init__(self, resolver: DbtDependencyResolver, task_options: TaskOptions, dbt_options: str = ""):
        self.resolver = resolver
        self.task_options = task_options
        self.dbt_options = dbt_options

    @abstractmethod
    def create_task(self, node_name: str, task_key: str, node_info: dict) -> Task:
        """Abstract method to create a task."""


class ModelTaskFactory(TaskFactory):
    """Factory for creating model tasks."""

    @property
    def valid_dbt_dependency_types(self) -> list[str]:
        return [DbtNodeTypes.MODEL.value, DbtNodeTypes.SEED.value, DbtNodeTypes.SNAPSHOT.value, DbtNodeTypes.TEST.value]

    def create_task(self, node_name: str, task_key: str, node_info: dict) -> Task:
        depends_on = self.resolver.resolve(node_info, self.valid_dbt_dependency_types)
        dbt_commands = [f"dbt deps {self.dbt_options}", f"dbt run --select {node_name} {self.dbt_options}"]
        return Task(task_key, dbt_commands, self.task_options, depends_on)


class SnapshotTaskFactory(TaskFactory):
    """Factory for creating snapshot tasks."""

    @property
    def valid_dbt_dependency_types(self) -> list[str]:
        return [DbtNodeTypes.MODEL.value]

    def create_task(self, node_name: str, task_key: str, node_info: dict) -> Task:
        depends_on = self.resolver.resolve(node_info, self.valid_dbt_dependency_types)
        dbt_commands = [f"dbt deps {self.dbt_options}", f"dbt snapshot --select {node_name} {self.dbt_options}"]
        return Task(task_key, dbt_commands, self.task_options, depends_on)


class SeedTaskFactory(TaskFactory):
    """Factory for creating seed tasks."""

    @property
    def valid_dbt_dependency_types(self) -> list[str]:
        return []  # Seeds don't have dependencies

    def create_task(self, node_name: str, task_key: str, node_info: dict) -> Task:
        depends_on = self.resolver.resolve(node_info, self.valid_dbt_dependency_types)
        dbt_commands = [f"dbt deps {self.dbt_options}", f"dbt seed --select {node_name} {self.dbt_options}"]
        return Task(task_key, dbt_commands, self.task_options, depends_on)


class TestTaskFactory(TaskFactory):
    """Factory for creating test tasks."""

    @property
    def valid_dbt_dependency_types(self) -> list[str]:
        return [DbtNodeTypes.MODEL.value]

    def create_task(self, node_name: str, task_key: str, node_info: dict) -> Task:
        depends_on = self.resolver.resolve(node_info, self.valid_dbt_dependency_types)
        dbt_commands = [f"dbt deps {self.dbt_options}", f"dbt test --select {node_name} {self.dbt_options}"]
        return Task(task_key, dbt_commands, self.task_options, depends_on)