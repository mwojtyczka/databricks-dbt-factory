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
    def resolve(dbt_node_info: dict, dbt_dependency_types: list[str]) -> list[str]:
        """
        Resolves dependencies for a given DBT node.

        Args:
            dbt_node_info (dict): Information about the DBT node.
            dbt_dependency_types (list[str]): List of valid DBT dependency types.

        Returns:
            list[str]: List of resolved dependencies.
        """
        dependencies = dbt_node_info.get('depends_on', {}).get('nodes', [])
        resolved_dependencies = []
        for dep in dependencies:
            if any(dep.startswith(dbt_type + ".") for dbt_type in dbt_dependency_types):
                resolved_dependencies.append(dep.replace('.', '_'))
        return resolved_dependencies


class TaskFactory(ABC):
    """Abstract base class for creating tasks."""

    def __init__(self, resolver: DbtDependencyResolver, task_options: TaskOptions, dbt_options: str = ""):
        """
        Initializes the TaskFactory.

        Args:
            resolver (DbtDependencyResolver): An instance of DbtDependencyResolver to resolve dependencies.
            task_options (TaskOptions): Options for the task.
            dbt_options (str, optional): Additional DBT options. Defaults to "".
        """
        self.resolver = resolver
        self.task_options = task_options
        self.dbt_options = dbt_options

    @abstractmethod
    def create_task(self, dbt_node_name: str, dbt_node_info: dict, task_key: str) -> Task:
        """
        Abstract method to create a task.

        Args:
            dbt_node_name (str): Name of the DBT node.
            dbt_node_info (dict): Information about the DBT node.
            task_key (str): Key for the task.

        Returns:
            Task: An instance of Task.
        """
        pass


class ModelTaskFactory(TaskFactory):
    """Factory for creating model tasks."""

    _valid_dbt_deps_types: list[str] = [
        DbtNodeTypes.MODEL.value,
        DbtNodeTypes.SEED.value,
        DbtNodeTypes.SNAPSHOT.value,
        DbtNodeTypes.TEST.value,
    ]

    def create_task(self, dbt_node_name: str, dbt_node_info: dict, task_key: str) -> Task:
        """
        Creates a model task.

        Args:
            dbt_node_name (str): Name of the DBT node.
            dbt_node_info (dict): Information about the DBT node.
            task_key (str): Key for the task.

        Returns:
            Task: An instance of Task.
        """
        depends_on = self.resolver.resolve(dbt_node_info, self._valid_dbt_deps_types)
        dbt_commands = [f"dbt deps {self.dbt_options}", f"dbt run --select {dbt_node_name} {self.dbt_options}"]
        return Task(task_key, dbt_commands, self.task_options, depends_on)


class SnapshotTaskFactory(TaskFactory):
    """Factory for creating snapshot tasks."""

    _valid_dbt_deps_types: list[str] = [DbtNodeTypes.MODEL.value]

    def create_task(self, dbt_node_name: str, dbt_node_info: dict, task_key: str) -> Task:
        """
        Creates a snapshot task.

        Args:
            dbt_node_name (str): Name of the DBT node.
            dbt_node_info (dict): Information about the DBT node.
            task_key (str): Key for the task.

        Returns:
            Task: An instance of Task.
        """
        depends_on = self.resolver.resolve(dbt_node_info, self._valid_dbt_deps_types)
        dbt_commands = [f"dbt deps {self.dbt_options}", f"dbt snapshot --select {dbt_node_name} {self.dbt_options}"]
        return Task(task_key, dbt_commands, self.task_options, depends_on)


class SeedTaskFactory(TaskFactory):
    """Factory for creating seed tasks."""

    _valid_dbt_deps_types: list[str] = []  # Seeds don't have dependencies

    def create_task(self, dbt_node_name: str, dbt_node_info: dict, task_key: str) -> Task:
        """
        Creates a seed task.

        Args:
            dbt_node_name (str): Name of the DBT node.
            dbt_node_info (dict): Information about the DBT node.
            task_key (str): Key for the task.

        Returns:
            Task: An instance of Task.
        """
        depends_on = self.resolver.resolve(dbt_node_info, self._valid_dbt_deps_types)
        dbt_commands = [f"dbt deps {self.dbt_options}", f"dbt seed --select {dbt_node_name} {self.dbt_options}"]
        return Task(task_key, dbt_commands, self.task_options, depends_on)


class TestTaskFactory(TaskFactory):
    """Factory for creating test tasks."""

    _valid_dbt_deps_types: list[str] = [DbtNodeTypes.MODEL.value]

    def create_task(self, dbt_node_name: str, dbt_node_info: dict, task_key: str) -> Task:
        """
        Creates a test task.

        Args:
            dbt_node_name (str): Name of the DBT node.
            dbt_node_info (dict): Information about the DBT node.
            task_key (str): Key for the task.

        Returns:
            Task: An instance of Task.
        """
        depends_on = self.resolver.resolve(dbt_node_info, self._valid_dbt_deps_types)
        dbt_commands = [f"dbt deps {self.dbt_options}", f"dbt test --select {dbt_node_name} {self.dbt_options}"]
        return Task(task_key, dbt_commands, self.task_options, depends_on)
