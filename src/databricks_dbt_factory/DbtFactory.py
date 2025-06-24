from databricks_dbt_factory import TaskFactory
from databricks_dbt_factory.SpecsHandler import SpecsHandler
from databricks_dbt_factory.DbtTask import DbtTask
from databricks_dbt_factory.Utils import generate_task_key


class DbtFactory:
    """A factory for generating Databricks job definitions from DBT manifests."""

    def __init__(self, file_handler: SpecsHandler, task_factories: dict[str, TaskFactory]):
        """
        Initializes the dbt factory.

        Args:
            file_handler (SpecsHandler): An instance of FileHandler to handle file operations.
            task_factories (dict[str, TaskFactory]): A dictionary mapping resource types to their respective TaskFactory.
        """
        self.file_handler = file_handler
        self.task_factories = task_factories

    def create_tasks_and_update_job_spec(
        self,
        dbt_manifest_path: str,
        input_job_spec_path: str,
        target_job_spec_path: str,
        new_job_name: str | None = None,
        dry_run: bool = False,
    ):
        """
        Generates tasks for Databricks Job from a DBT manifest and updates the existing job definition file
        either in place, or to a new file if target_job_spec_path is provided.

        Args:
            dbt_manifest_path (str): Path to the DBT manifest file.
            input_job_spec_path (str): Path to the input job specification YAML file.
            target_job_spec_path (str): Path to save the updated job specification file.
            new_job_name (str, optional): The name of the job to update. Defaults to None.
            dry_run (bool, optional): If True, the tasks will be printed to the console instead of writing to a file. Defaults to False.
        """
        manifest = self.file_handler.read_dbt_manifest(dbt_manifest_path)
        tasks = self.create_tasks(manifest)
        if dry_run:
            print(tasks)
        else:
            self.file_handler.replace_tasks_in_job_spec(input_job_spec_path, tasks, target_job_spec_path, new_job_name)

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
            task_key = generate_task_key(node_full_name)
            factory = self.task_factories[resource_type]

            task = factory.create_task(node_name, node_info, task_key)
            tasks.append(task)

        return tasks
