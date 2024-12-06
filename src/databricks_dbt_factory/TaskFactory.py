from abc import ABC, abstractmethod
from enum import Enum

from databricks_dbt_factory.Task import Task


class DbtNodeTypes(Enum):
    """Enum class to represent dbt node types."""
    MODEL = "model"
    TEST = "test"
    SEED = "seed"
    SNAPSHOT = "snapshot"


class TaskFactory(ABC):
    """Abstract base class for creating tasks."""

    @property
    @abstractmethod
    def valid_dbt_dependency_types(self) -> list[str]:
        """Abstract property to define valid dependency types."""
        pass

    @abstractmethod
    def create_task(self, node_name: str, task_key: str, depends_on: list[str]) -> Task:
        """Abstract method to create a task."""
        pass

    def _resolve_dependencies(self, node_info: dict) -> list[str]:
        """Common logic for resolving dependencies based on node prefixes."""
        dependencies = node_info.get('depends_on', {}).get('nodes', [])
        return [
            dep.replace('.', '_')
            for dep in dependencies
            if any(dep.startswith(dbt_type + ".") for dbt_type in self.valid_dbt_dependency_types)
        ]


class ModelTaskFactory(TaskFactory):
    """Factory for creating model tasks."""
    @property
    def valid_dbt_dependency_types(self) -> list[str]:
        return [DbtNodeTypes.MODEL.value, DbtNodeTypes.SEED.value, DbtNodeTypes.SNAPSHOT.value, DbtNodeTypes.TEST.value]

    def create_task(self, node_name: str, task_key: str, node_info: dict) -> Task:
        depends_on = self._resolve_dependencies(node_info)
        dbt_commands = [
            "dbt deps --target dev --profiles-dir .",
            f"dbt run --select {node_name} --target dev --profiles-dir ."
        ]
        return Task(task_key, dbt_commands, depends_on)


class SnapshotTaskFactory(TaskFactory):
    """Factory for creating snapshot tasks."""
    @property
    def valid_dbt_dependency_types(self) -> list[str]:
        return [DbtNodeTypes.MODEL.value]

    def create_task(self, node_name: str, task_key: str, node_info: dict) -> Task:
        depends_on = self._resolve_dependencies(node_info)
        dbt_commands = [
            "dbt deps --target dev --profiles-dir .",
            f"dbt snapshot --select {node_name} --target dev --profiles-dir ."
        ]
        return Task(task_key, dbt_commands, depends_on)


class SeedTaskFactory(TaskFactory):
    """Factory for creating seed tasks."""
    @property
    def valid_dbt_dependency_types(self) -> list[str]:
        return []  # Seeds don't have dependencies

    def create_task(self, node_name: str, task_key: str, node_info: dict) -> Task:
        depends_on = self._resolve_dependencies(node_info)
        dbt_commands = [
            "dbt deps --target dev --profiles-dir .",
            f"dbt seed --select {node_name} --target dev --profiles-dir ."
        ]
        return Task(task_key, dbt_commands, depends_on)


class TestTaskFactory(TaskFactory):
    """Factory for creating test tasks."""
    @property
    def valid_dbt_dependency_types(self) -> list[str]:
        return [DbtNodeTypes.MODEL.value]

    def create_task(self, node_name: str, task_key: str, node_info: dict) -> Task:
        depends_on = self._resolve_dependencies(node_info)
        dbt_commands = [
            "dbt deps --target dev --profiles-dir .",
            f"dbt test --select {node_name} --target dev --profiles-dir ."
        ]
        return Task(task_key, dbt_commands, depends_on)