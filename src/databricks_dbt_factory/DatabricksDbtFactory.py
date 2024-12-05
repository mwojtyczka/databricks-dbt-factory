from typing import Dict, List
from databricks_dbt_factory.FileHandler import FileHandler


class Task:
    """Represents a task in the Databricks job definition."""
    def __init__(self, task_key: str, dbt_command: str, depends_on: List[str]):
        self.task_key = task_key
        self.dbt_command = dbt_command
        self.depends_on = depends_on

    def to_dict(self) -> dict:
        """Converts the Task to a dictionary suitable for the job definition."""
        return {
            'task_key': self.task_key,
            'dbt_task': {
                'commands': [self.dbt_command],
                'project_directory': "",
                'catalog': 'main',
                'schema': 'default',
                'warehouse_id': '475b94ddc7cd5211',
            },
            'environment_key': 'Default',
            'depends_on': [{'task_key': dep} for dep in self.depends_on]
        }


class DatabricksDbtFactory:
    """A factory for generating Databricks job definitions from DBT manifests."""

    def __init__(self, file_handler: FileHandler):
        self.file_handler = file_handler

    def generate_job_definition_and_save(self, manifest_path: str, job_definition_path: str, job_name: str):
        """Generates a job definition from a DBT manifest and saves it to a file."""
        manifest = self.file_handler.read_manifest(manifest_path)
        job_definition = self.generate_job_definition(manifest, job_name)
        self.file_handler.write_job_definition(job_definition, job_definition_path)

    def generate_job_definition(self, manifest: dict, job_name: str) -> dict:
        """Generates a job definition dictionary from a DBT manifest."""
        tasks = self._generate_tasks(manifest)

        return {
            'name': job_name,
            'tasks': [task.to_dict() for task in tasks],
            'git_source': self._get_git_source(),
            'queue': {'enabled': True},
            'environments': self._get_environments()
        }

    def _generate_tasks(self, manifest: dict) -> List[Task]:
        """Generates a list of tasks based on the DBT manifest."""
        models = manifest.get('nodes', {})
        tests = manifest.get('nodes', {})

        tasks = []

        for model_name, model_info in models.items():
            if model_info['resource_type'] != 'model':
                continue

            task_key = self._clean_name(model_name)
            depends_on = self._get_model_dependencies(model_info)
            dbt_command = f"dbt run --select {model_name} --target dev --profiles-dir ."

            model_task = Task(task_key, dbt_command, depends_on)
            tasks.append(model_task)

            # Add corresponding test tasks
            test_tasks = self._generate_test_tasks(model_name, tests, task_key)
            tasks.extend(test_tasks)

        return tasks

    def _generate_test_tasks(self, model_name: str, tests: Dict, model_task_key: str) -> List[Task]:
        """Generates test tasks for a specific model."""
        test_tasks = []

        for test_name, test_info in tests.items():
            if test_info['resource_type'] == 'test' and model_name in test_info.get('depends_on', {}).get('nodes', []):
                test_key = self._clean_name(test_name)
                dbt_command = f"dbt test --select {test_name} --target dev --profiles-dir ."
                test_task = Task(test_key, dbt_command, [model_task_key])
                test_tasks.append(test_task)

        return test_tasks

    @staticmethod
    def _clean_name(name: str) -> str:
        """Cleans a DBT node name to make it suitable as a task key."""
        return name.replace('.', '_')

    @staticmethod
    def _get_model_dependencies(model_info: dict) -> List[str]:
        """Gets the dependencies of a model."""
        dependencies = model_info.get('depends_on', {}).get('nodes', [])
        return [dep.replace('.', '_') for dep in dependencies if dep.startswith('model.')]

    @staticmethod
    def _get_git_source() -> dict:
        """Returns the git source configuration."""
        return {
            'git_url': 'https://github.com/mwojtyczka/dbt-demo.git',
            'git_provider': 'gitHub',
            'git_branch': 'main',
        }

    @staticmethod
    def _get_environments() -> List[dict]:
        """Returns the environment configuration."""
        return [
            {
                'environment_key': 'Default',
                'spec': {
                    'client': "1",
                    'dependencies': ['dbt-databricks']
                }
            }
        ]
