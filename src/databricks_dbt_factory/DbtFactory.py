from dataclasses import replace

from databricks_dbt_factory import TaskFactory
from databricks_dbt_factory.SpecsHandler import SpecsHandler
from databricks_dbt_factory.DbtTask import DbtTask
from databricks_dbt_factory.Utils import build_task_key_map, generate_task_key


class DbtFactory:
    """A factory for generating Databricks job definitions from dbt manifests."""

    def __init__(
        self,
        file_handler: SpecsHandler,
        task_factories: dict[str, TaskFactory],
        bundle_tests: bool = True,
        gate_on_tests: bool = True,
    ):
        """
        Initializes the dbt factory.

        Args:
            file_handler (SpecsHandler): Handles reading the dbt manifest and writing the job spec.
            task_factories (dict[str, TaskFactory]): Maps dbt resource types (`model`, `seed`,
                `snapshot`, `test`) to their respective `TaskFactory` instances. Omitting `test`
                disables test-task generation entirely.
            bundle_tests (bool): When True, emit one `<resource>_tests` task per tested resource
                running `dbt test --select <resource>`. When False, emit one task per dbt test node.
            gate_on_tests (bool): When True and bundling, rewire downstream models/seeds/snapshots
                to depend on the upstream `_tests` task so failing tests halt the DAG. Ignored in
                flat mode (per-test tasks never gate downstream).
        """
        self.file_handler = file_handler
        self.task_factories = task_factories
        self.bundle_tests = bundle_tests
        self.gate_on_tests = gate_on_tests

    def create_tasks_and_update_job_spec(
        self,
        dbt_manifest_path: str,
        input_job_spec_path: str,
        target_job_spec_path: str,
        new_job_name: str | None = None,
        dry_run: bool = False,
    ):
        """
        Generates tasks from a dbt manifest and writes them into a Databricks job spec.

        Args:
            dbt_manifest_path (str): Path to the dbt `manifest.json`.
            input_job_spec_path (str): Path to the input (template) job spec YAML.
            target_job_spec_path (str): Path to write the updated job spec YAML.
            new_job_name (str | None): Optional replacement job name. If provided, overrides the
                name in the input spec.
            dry_run (bool): When True, print the generated tasks instead of writing to disk.
        """
        manifest = self.file_handler.read_dbt_manifest(dbt_manifest_path)
        tasks = self.create_tasks(manifest)
        if dry_run:
            print(tasks)
        else:
            self.file_handler.replace_tasks_in_job_spec(input_job_spec_path, tasks, target_job_spec_path, new_job_name)

    def create_tasks(self, dbt_manifest: dict) -> list[dict]:
        """
        Generates the Databricks task dictionaries from a dbt manifest.

        Args:
            dbt_manifest (dict): Parsed dbt manifest content.

        Returns:
            list[dict]: Task dictionaries ready to be injected into the `tasks` list of a
            Databricks job spec.
        """
        tasks = self._create_tasks(dbt_manifest)
        return [task.to_dict() for task in tasks]

    def _create_tasks(self, dbt_manifest: dict) -> list[DbtTask]:
        """
        Builds `DbtTask` instances from the manifest, applying the bundling and gating policies.

        Args:
            dbt_manifest (dict): Parsed dbt manifest content.

        Returns:
            list[DbtTask]: `DbtTask` instances (not yet rendered to dicts).
        """
        dbt_nodes = dbt_manifest.get('nodes', {})
        dbt_sources = dbt_manifest.get('sources', {})
        task_key_map = build_task_key_map([*dbt_nodes, *dbt_sources])
        for factory in self.task_factories.values():
            factory.resolver.set_task_key_map(task_key_map)

        bundle = 'test' in self.task_factories and self.bundle_tests

        nodes_with_tests: set[str] = set()
        if bundle:
            for node_info in dbt_nodes.values():
                if node_info['resource_type'] != 'test':
                    continue
                for dep in node_info.get('depends_on', {}).get('nodes', []):
                    if dep.startswith(('model.', 'seed.', 'snapshot.', 'source.')) and (
                        dep in dbt_nodes or dep in dbt_sources
                    ):
                        nodes_with_tests.add(dep)

        task_keys_with_tests = {task_key_map[fn] for fn in nodes_with_tests}

        tasks = []
        for node_full_name, node_info in dbt_nodes.items():
            resource_type = node_info['resource_type']
            if resource_type not in self.task_factories:
                continue
            if bundle and resource_type == 'test':
                continue

            node_name = node_info['name']
            task_key = task_key_map[node_full_name]
            factory = self.task_factories[resource_type]
            task = factory.create_task(node_name, node_info, task_key)

            if bundle and self.gate_on_tests and resource_type in ('model', 'seed', 'snapshot'):
                new_deps = [
                    f"{dep_key}_tests" if dep_key in task_keys_with_tests else dep_key
                    for dep_key in (task.depends_on or [])
                ]
                task = replace(task, depends_on=new_deps)

            tasks.append(task)

        if bundle:
            test_factory = self.task_factories['test']
            for full_name in sorted(nodes_with_tests):
                is_source = full_name.startswith('source.')
                info = dbt_sources[full_name] if is_source else dbt_nodes[full_name]
                resource_task_key = task_key_map[full_name]
                bare_name = info['name']
                pkg_prefix = f"{info['package_name']}." if resource_task_key != generate_task_key(full_name) else ""
                select = f"source:{pkg_prefix}{info['source_name']}.{bare_name}" if is_source else f"{pkg_prefix}{bare_name}"
                tasks.append(
                    test_factory.create_bundled_task(
                        task_key=f"{resource_task_key}_tests",
                        select=select,
                        deps_command_name=bare_name,
                        depends_on=[] if is_source else [resource_task_key],
                    )
                )

        return tasks
