from dataclasses import replace

from databricks_dbt_factory import TaskFactory
from databricks_dbt_factory.SpecsHandler import SpecsHandler
from databricks_dbt_factory.DbtTask import DbtTask
from databricks_dbt_factory.Utils import generate_task_key


class DbtFactory:

    def __init__(self, file_handler: SpecsHandler, task_factories: dict[str, TaskFactory]):
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
        manifest = self.file_handler.read_dbt_manifest(dbt_manifest_path)
        tasks = self.create_tasks(manifest)
        if dry_run:
            print(tasks)
        else:
            self.file_handler.replace_tasks_in_job_spec(input_job_spec_path, tasks, target_job_spec_path, new_job_name)

    def create_tasks(self, dbt_manifest: dict) -> list[dict]:
        tasks = self._create_tasks(dbt_manifest)
        return [task.to_dict() for task in tasks]

    def _create_tasks(self, dbt_manifest: dict) -> list[DbtTask]:
        dbt_nodes = dbt_manifest.get('nodes', {})
        tests_enabled = 'test' in self.task_factories

        models_with_tests: set[str] = set()
        if tests_enabled:
            for node_info in dbt_nodes.values():
                if node_info['resource_type'] != 'test':
                    continue
                for dep in node_info.get('depends_on', {}).get('nodes', []):
                    if dep.startswith('model.') and dep in dbt_nodes:
                        models_with_tests.add(dbt_nodes[dep]['name'])

        tasks = []
        for node_full_name, node_info in dbt_nodes.items():
            resource_type = node_info['resource_type']
            if resource_type == 'test' or resource_type not in self.task_factories:
                continue

            node_name = node_info['name']
            task_key = generate_task_key(node_full_name)
            factory = self.task_factories[resource_type]
            task = factory.create_task(node_name, node_info, task_key)

            if resource_type == 'model' and tests_enabled:
                new_deps = []
                for dep_key in (task.depends_on or []):
                    if dep_key in models_with_tests:
                        new_deps.append(f"{dep_key}_tests")
                    else:
                        new_deps.append(dep_key)
                task = replace(task, depends_on=new_deps)

            tasks.append(task)

        if tests_enabled:
            test_factory = self.task_factories['test']
            for model_name in sorted(models_with_tests):
                task_key = f"{model_name}_tests"
                dbt_deps = test_factory.get_dbt_deps_command(model_name)
                commands = [dbt_deps] if dbt_deps else []
                commands.append(
                    f"dbt test --select {model_name}"
                    + (f" {test_factory.dbt_options}" if test_factory.dbt_options else "")
                )
                task = DbtTask(task_key, commands, test_factory.task_options, [model_name])
                tasks.append(task)

        return tasks
