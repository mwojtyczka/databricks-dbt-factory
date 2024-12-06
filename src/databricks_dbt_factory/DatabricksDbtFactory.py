from databricks_dbt_factory.FileHandler import FileHandler


class Task:
    """Represents a task in the Databricks job definition."""
    def __init__(self, task_key: str, dbt_commands: list[str], depends_on: list[str] = []):
        self.task_key = task_key
        self.dbt_commands = dbt_commands
        self.depends_on = depends_on
        # TODO option to disable data tests

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
            'depends_on': [{'task_key': dep} for dep in self.depends_on]
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
        tasks = self._generate_tasks(manifest)

        # TODO parametrize git_source, queue, environments
        return {
            'name': job_name,
            'tasks': [task.to_dict() for task in tasks],
            'git_source': self._get_git_source(),
            'queue': {'enabled': True},
            'environments': self._get_environments()
        }

    def _generate_tasks(self, manifest: dict) -> list[Task]:
        """Generates a list of tasks based on the DBT manifest."""
        nodes = manifest.get('nodes', {})
        tasks = []

        for name, info in nodes.items():

            if info['resource_type'] == 'seed':
                # seeds don't have any dependencies
                test_task = self._generate_seed_task(name)
                tasks.append(test_task)

            if info['resource_type'] == 'snapshot':
                # snapshots can have models and sources as dependencies
                depends_on = self._get_snapshot_dependencies(info)
                test_task = self._generate_snapshot_task(name, depends_on)
                tasks.append(test_task)

            if info['resource_type'] == 'model':
                # models can have dependencies to other models, tests and snapshots
                depends_on = self._get_model_dependencies(info)
                task_key = self._clean_name(name)
                model_task = self._generate_model_task(info['name'], task_key, depends_on)
                tasks.append(model_task)

                # add corresponding test tasks
                test_tasks = self._generate_test_tasks(name, nodes, task_key)
                tasks.extend(test_tasks)

        return tasks

    def _generate_model_task(self, name, task_key, depends_on):
        dbt_commands = [
            "dbt deps --target dev --profiles-dir .",
            f"dbt run --select {name} --target dev --profiles-dir ."
        ]
        return Task(task_key, dbt_commands, depends_on)

    def _generate_test_tasks(self, model_name: str, nodes: dict, model_task_key: str) -> list[Task]:
        """Generates test tasks for a specific model."""
        tasks = []
        for name, info in nodes.items():
            if info['resource_type'] == 'test' and model_name in info.get('depends_on', {}).get('nodes', []):
                dbt_command = [
                    "dbt deps --target dev --profiles-dir .",
                    f"dbt test --select {info['name']} --target dev --profiles-dir ."
                ]
                test_key = self._clean_name(name)
                test_task = Task(test_key, dbt_command, [model_task_key])
                tasks.append(test_task)
        return tasks

    def _generate_snapshot_tasks(self, model_name: str, nodes: dict, model_task_key: str) -> list[Task]:
        """Generates test tasks for a specific model."""
        tasks = []
        for name, info in nodes.items():
            if info['resource_type'] == 'snapshot' and model_name in info.get('depends_on', {}).get('nodes', []):
                dbt_command = [
                    "dbt deps --target dev --profiles-dir .",
                    f"dbt snapshot --select {info['name']} --target dev --profiles-dir ."
                ]
                snapshot_key = self._clean_name(name)
                snapshot_task = Task(snapshot_key, dbt_command, [model_task_key])
                tasks.append(snapshot_task)
        return tasks

    def _generate_snapshot_task(self, name: str, depends_on: list[str]) -> Task:
        """Generates snapshot tasks for a specific model."""
        dbt_command = [
            "dbt deps --target dev --profiles-dir .",
            f"dbt snapshot --select {name} --target dev --profiles-dir ."
        ]
        seed_key = self._clean_name(name)
        return Task(seed_key, dbt_command, depends_on)


    def _generate_seed_task(self, name: str) -> Task:
        """Generates seed tasks for a specific model."""
        dbt_command = [
            "dbt deps --target dev --profiles-dir .",
            f"dbt seed --select {name} --target dev --profiles-dir ."
        ]
        seed_key = self._clean_name(name)
        return Task(seed_key, dbt_command)

    @staticmethod
    def _clean_name(name: str) -> str:
        """Cleans a DBT node name to make it suitable as a task key."""
        return name.replace('.', '_')

    @staticmethod
    def _get_model_dependencies(info: dict) -> list[str]:
        """Gets the dependencies of a model."""
        dependencies = info.get('depends_on', {}).get('nodes', [])
        # TODO run dependent models only after data tests pass
        return [dep.replace('.', '_') for dep in dependencies
                if dep.startswith('model.') or dep.startswith('seed.') or dep.startswith('snapshot.')]

    @staticmethod
    def _get_snapshot_dependencies(info: dict) -> list[str]:
        """Gets the dependencies of a model."""
        dependencies = info.get('depends_on', {}).get('nodes', [])
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
