from dataclasses import replace

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

        tests_enabled = 'test' in self.task_factories

        # First pass: build a map of model full name -> list of test task keys (only if tests enabled)
        model_to_test_keys: dict[str, list[str]] = {}
        if tests_enabled:
            for node_full_name, node_info in dbt_nodes.items():
                if node_info['resource_type'] != 'test':
                    continue
                parent_model_name = self._get_parent_model_name(node_info, dbt_nodes)
                test_task_key = generate_task_key(node_full_name, parent_model_name)
                for dep in node_info.get('depends_on', {}).get('nodes', []):
                    if dep.startswith('model.'):
                        model_to_test_keys.setdefault(dep, []).append(test_task_key)

        # Second pass: create all tasks
        tasks = []
        for node_full_name, node_info in dbt_nodes.items():
            resource_type = node_info['resource_type']
            if resource_type not in self.task_factories:
                continue

            node_name = node_info['name']
            parent_model_name = self._get_parent_model_name(node_info, dbt_nodes) if resource_type == 'test' else None
            task_key = generate_task_key(node_full_name, parent_model_name)
            factory = self.task_factories[resource_type]

            task = factory.create_task(node_name, node_info, task_key)

            # For models: if upstream model has tests, depend on tests instead of the model.
            # If upstream model has no tests (or tests disabled), keep the model dependency.
            if resource_type == 'model' and tests_enabled:
                new_deps = []
                for dep_key in (task.depends_on or []):
                    # Find the model full name that produced this dep_key
                    model_full_name = self._find_model_by_task_key(dep_key, dbt_nodes)
                    test_keys = model_to_test_keys.get(model_full_name, []) if model_full_name else []
                    if test_keys:
                        # Replace model dep with its test deps
                        for tkey in test_keys:
                            if tkey not in new_deps:
                                new_deps.append(tkey)
                    else:
                        # No tests for this upstream model, keep direct dependency
                        new_deps.append(dep_key)
                task = replace(task, depends_on=new_deps)

            tasks.append(task)

        return tasks

    @staticmethod
    def _find_model_by_task_key(task_key: str, dbt_nodes: dict) -> str | None:
        """Finds the model full name that corresponds to a given task key."""
        for node_full_name, node_info in dbt_nodes.items():
            if node_info['resource_type'] == 'model' and node_info['name'] == task_key:
                return node_full_name
        return None

    @staticmethod
    def _get_parent_model_name(test_node_info: dict, all_nodes: dict) -> str | None:
        """Returns the name of the first model this test depends on."""
        for dep in test_node_info.get('depends_on', {}).get('nodes', []):
            if dep.startswith('model.') and dep in all_nodes:
                return all_nodes[dep]['name']
        return None
