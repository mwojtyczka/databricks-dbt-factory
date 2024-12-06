from databricks_dbt_factory import TaskFactory
from databricks_dbt_factory.FileHandler import FileHandler
from databricks_dbt_factory.Task import Task


class DatabricksDbtFactory:
    """A factory for generating Databricks job definitions from DBT manifests."""

    def __init__(self, file_handler: FileHandler, task_factories: dict[str, TaskFactory]):
        self.file_handler = file_handler
        self.task_factories = task_factories

    def create_job_tasks_and_update(
        self, manifest_path: str, job_definition_path: str, destination_job_definition_path: str | None = None
    ):
        """Generates tasks for Databricks Job from a DBT manifest and update it in the existing job definition file."""
        manifest = self.file_handler.read_dbt_manifest(manifest_path)
        tasks = self.create_job_tasks(manifest)
        self.file_handler.replace_tasks_in_yaml(job_definition_path, tasks, destination_job_definition_path)

    def create_job_tasks(self, manifest: dict) -> list[dict]:
        """Generates tasks for Databricks Job from a DBT manifest."""
        tasks = self._create_tasks(manifest)
        return [task.to_dict() for task in tasks]

    def _create_tasks(self, manifest: dict) -> list[Task]:
        """Generates a list of tasks based on the DBT manifest."""
        dbt_nodes = manifest.get('nodes', {})
        tasks = []

        for node_full_name, node_info in dbt_nodes.items():
            resource_type = node_info['resource_type']
            if resource_type not in self.task_factories:
                continue

            node_name = node_info['name']
            task_key = self._clean_name(node_full_name)
            factory = self.task_factories[resource_type]

            task = factory.create_task(node_name, task_key, node_info)
            tasks.append(task)

        return tasks

    @staticmethod
    def _clean_name(name: str) -> str:
        """Cleans a DBT node name to make it suitable as a Databricks task key."""
        return name.replace('.', '_')
