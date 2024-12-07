from databricks_dbt_factory import TaskFactory
from databricks_dbt_factory.FileHandler import FileHandler
from databricks_dbt_factory.DbtTask import DbtTask


class DatabricksDbtFactory:
    """A factory for generating Databricks job definitions from DBT manifests."""

    def __init__(self, file_handler: FileHandler, task_factories: dict[str, TaskFactory]):
        """
        Initializes the DatabricksDbtFactory.

        Args:
            file_handler (FileHandler): An instance of FileHandler to handle file operations.
            task_factories (dict[str, TaskFactory]): A dictionary mapping resource types to their respective TaskFactory.
        """
        self.file_handler = file_handler
        self.task_factories = task_factories

    def create_tasks_and_update_job_spec(
        self, dbt_manifest_path: str, input_job_spec_path: str, target_job_spec_path: str | None = None
    ):
        """
        Generates tasks for Databricks Job from a DBT manifest and updates the existing job definition file
        either in place, or to a new file if target_job_spec_path is provided.

        Args:
            dbt_manifest_path (str): Path to the DBT manifest file.
            input_job_spec_path (str): Path to the input job specification YAML file.
            target_job_spec_path (str, optional): Path to save the updated job specification file. If not provided, the input file will be updated in place. Defaults to None.
        """
        manifest = self.file_handler.read_dbt_manifest(dbt_manifest_path)
        tasks = self.create_tasks(manifest)
        self.file_handler.replace_tasks_in_job_spec(input_job_spec_path, tasks, target_job_spec_path)

    def create_tasks(self, dbt_manifest: dict) -> list[dict]:
        """
        Generates tasks for Databricks Job from a DBT manifest.

        Args:
            dbt_manifest (dict): The DBT manifest content.

        Returns:
            list[dict]: A list of task dictionaries suitable for the job definition.
        """
        tasks = self._create_tasks(dbt_manifest)
        return [task.to_dict() for task in tasks]

    def _create_tasks(self, dbt_manifest: dict) -> list[DbtTask]:
        """
        Generates a list of Databricks job tasks based on the DBT manifest.

        Args:
            dbt_manifest (dict): The DBT manifest content.

        Returns:
            list[DbtTask]: A list of Task instances.
        """
        dbt_nodes = dbt_manifest.get('nodes', {})
        tasks = []

        for node_full_name, node_info in dbt_nodes.items():
            resource_type = node_info['resource_type']
            if resource_type not in self.task_factories:
                continue

            node_name = node_info['name']
            task_key = node_full_name.replace('.', '_')  # make sure it can be used as a task key
            factory = self.task_factories[resource_type]

            task = factory.create_task(node_name, node_info, task_key)
            tasks.append(task)

        return tasks
