from dataclasses import dataclass
from databricks_dbt_factory.FileHandler import FileHandler


@dataclass(frozen=True)
class Task:
    """Represents a task in the Databricks job definition."""
    task_key: str
    dbt_commands: list[str]
    depends_on: list[str] | None = None

    def to_dict(self) -> dict:
        """Converts the Task to a dictionary suitable for the job definition."""
        # TODO parametrize catalog, schema, warehouse_id
        return {
            'task_key': self.task_key,
            'dbt_task': {
                'commands': self.dbt_commands,
                'project_directory': "",
                'catalog': 'main',
                'schema': 'default',
                'warehouse_id': '475b94ddc7cd5211',
            },
            'environment_key': 'Default',
            'depends_on': [{'task_key': dep} for dep in (self.depends_on or [])]
        }


class DatabricksDbtFactory:
    """A factory for generating Databricks job definitions from DBT manifests."""

    def __init__(self, file_handler: FileHandler):
        self.file_handler = file_handler

    def generate_job_definition_and_save(self, manifest_path: str, job_definition_path: str, job_name: str):
        """Generates a job definition from a DBT manifest and saves it to a file."""
        manifest = self.file_handler.read_dbt_manifest(manifest_path)
        job_definition = self.generate_job_definition(manifest, job_name)
        self.file_handler.write_job_definition(job_definition, job_definition_path)

    def generate_job_definition(self, manifest: dict, job_name: str) -> dict:
        """Generates a job definition dictionary from a DBT manifest."""
        tasks = self._generate_job_tasks(manifest)

        # TODO parametrize git_source, queue, environments
        return {
            'name': job_name,
            'tasks': [task.to_dict() for task in tasks],
            'git_source': self._get_git_source(),
            'queue': {'enabled': True},
            'environments': self._get_environments()
        }

    def _generate_job_tasks(self, manifest: dict) -> list[Task]:
        """Generates a list of tasks based on the DBT manifest."""
        dbt_nodes = manifest.get('nodes', {})
        job_tasks = []

        for node_full_name, node_info in dbt_nodes.items():

            node_name = node_info['name']
            task_name = self._clean_name(node_full_name)

            if node_info['resource_type'] == 'seed':
                depends_on = []
                seed_task = self._generate_seed_task(node_name, task_name, depends_on)
                job_tasks.append(seed_task)

            if node_info['resource_type'] == 'snapshot':
                depends_on = self._get_snapshot_dependencies(node_info)
                snapshot_task = self._generate_snapshot_task(node_name, task_name, depends_on)
                job_tasks.append(snapshot_task)

            if node_info['resource_type'] == 'test':
                depends_on = self._get_test_dependencies(node_info)
                test_task = self._generate_test_task(node_name, task_name, depends_on)
                job_tasks.append(test_task)

            if node_info['resource_type'] == 'model':
                depends_on = self._get_model_dependencies(node_info)
                model_task = self._generate_model_task(node_name, task_name, depends_on)
                job_tasks.append(model_task)

        return job_tasks

    def _generate_model_task(self, model_name, task_name, depends_on):
        """Generates model task."""
        dbt_commands = [
            "dbt deps --target dev --profiles-dir .",
            f"dbt run --select {model_name} --target dev --profiles-dir ."
        ]
        return Task(task_name, dbt_commands, depends_on)

    def _generate_snapshot_task(self, snapshot_name, task_name: str, depends_on: list[str]) -> Task:
        """Generates snapshot task."""
        dbt_command = [
            "dbt deps --target dev --profiles-dir .",
            f"dbt snapshot --select {snapshot_name} --target dev --profiles-dir ."
        ]
        return Task(task_name, dbt_command, depends_on)

    def _generate_seed_task(self, seed_name, task_name: str, depends_on: list[str]) -> Task:
        """Generates seed task."""
        dbt_command = [
            "dbt deps --target dev --profiles-dir .",
            f"dbt seed --select {seed_name} --target dev --profiles-dir ."
        ]
        # seeds don't have any dependencies
        return Task(task_name, dbt_command, depends_on)

    def _generate_test_task(self, test_name, task_name: str, depends_on: list[str]) -> Task:
        """Generates seed task."""
        dbt_command = [
            "dbt deps --target dev --profiles-dir .",
            f"dbt test --select {test_name} --target dev --profiles-dir ."
        ]
        # seeds don't have any dependencies
        return Task(task_name, dbt_command, depends_on)

    @staticmethod
    def _get_model_dependencies(info: dict) -> list[str]:
        """Gets the dependencies of a model."""
        dependencies = info.get('depends_on', {}).get('nodes', [])
        # TODO run dependent models only after data tests pass
        # models can have dependencies to other models, tests and snapshots
        return [dep.replace('.', '_') for dep in dependencies
                if dep.startswith('model.') or dep.startswith('seed.')
                or dep.startswith('snapshot.') or dep.startswith('test.')]

    @staticmethod
    def _get_snapshot_dependencies(info: dict) -> list[str]:
        """Gets the dependencies of a model."""
        dependencies = info.get('depends_on', {}).get('nodes', [])
        # snapshots can have models and sources as dependencies
        return [dep.replace('.', '_') for dep in dependencies if dep.startswith('model.')]

    @staticmethod
    def _get_test_dependencies(info: dict) -> list[str]:
        """Gets the dependencies of a model."""
        dependencies = info.get('depends_on', {}).get('nodes', [])
        # test can have only models as dependencies
        return [dep.replace('.', '_') for dep in dependencies if dep.startswith('model.')]

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
                    'dependencies': ['dbt-databricks']
                }
            }
        ]
