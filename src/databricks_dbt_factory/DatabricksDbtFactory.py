from databricks_dbt_factory import TaskFactory
from databricks_dbt_factory.FileHandler import FileHandler
from databricks_dbt_factory.Task import Task


class DatabricksDbtFactory:
    """A factory for generating Databricks job definitions from DBT manifests."""

    def __init__(self, file_handler: FileHandler, task_factories: dict[str, TaskFactory]):
        self.file_handler = file_handler
        self.task_factories = task_factories

    def generate_job_definition_and_save(self, manifest_path: str, job_definition_path: str, job_name: str):
        """Generates a job definition from a DBT manifest and saves it to a file."""
        manifest = self.file_handler.read_dbt_manifest(manifest_path)
        job_definition = self.generate_job_definition(manifest, job_name)
        self.file_handler.write_job_definition(job_definition, job_definition_path)

    def generate_job_definition(self, manifest: dict, job_name: str) -> dict:
        """Generates a job definition dictionary from a DBT manifest."""
        tasks = self._build_dag(manifest)
        return {
            'name': job_name,
            'tasks': [task.to_dict() for task in tasks],
            'git_source': self._get_git_source(),
            'queue': {'enabled': True},
            'environments': self._get_environments(),
        }

    def _build_dag(self, manifest: dict) -> list[Task]:
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

    @staticmethod
    def _get_git_source() -> dict:
        """Returns the git source configuration."""
        return {
            'git_url': 'https://github.com/mwojtyczka/dbt-demo.git',
            'git_provider': 'gitHub',
            'git_branch': 'main',
        }

    @staticmethod
    def _get_environments() -> list[dict]:
        """Returns the environment configuration."""
        return [
            {
                'environment_key': 'Default',
                'spec': {
                    'client': "1",
                    'dependencies': ['dbt-databricks'],
                },
            },
        ]
