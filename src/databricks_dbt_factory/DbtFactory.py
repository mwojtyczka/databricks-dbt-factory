from dataclasses import replace

from databricks_dbt_factory import TaskFactory
from databricks_dbt_factory.SpecsHandler import SpecsHandler
from databricks_dbt_factory.DbtTask import DbtTask
from databricks_dbt_factory.Utils import generate_task_key


class DbtFactory:
    """A factory for generating Databricks job definitions from dbt manifests."""

    def __init__(
        self,
        file_handler: SpecsHandler,
        task_factories: dict[str, TaskFactory],
        bundle_tests: bool = False,
    ):
        """
        Initializes the dbt factory.

        Args:
            file_handler (SpecsHandler): Handles reading the dbt manifest and writing the job spec.
            task_factories (dict[str, TaskFactory]): Maps dbt resource types (`model`, `seed`,
                `snapshot`, `test`) to their respective `TaskFactory` instances. Omitting `test`
                disables test-task generation entirely.
            bundle_tests (bool): When True, emit one `<resource>_tests` task per tested resource
                and rewire downstream models/seeds/snapshots to depend on the upstream's `_tests`
                task so failing tests halt the DAG. When False, emit one task per dbt test node.
        """
        self.file_handler = file_handler
        self.task_factories = task_factories
        self.bundle_tests = bundle_tests

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

    _GATEABLE_TYPES = frozenset({'model', 'seed', 'snapshot'})
    _DBT_TEST_TARGET_PREFIXES = ('model.', 'seed.', 'snapshot.', 'source.')

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

        bundle = 'test' in self.task_factories and self.bundle_tests
        single_model_tested: set[str] = set()
        standalone_tests: list[tuple[str, dict]] = []
        if bundle:
            single_model_tested, standalone_tests = self._classify_tests(dbt_nodes, dbt_sources)
        task_keys_with_tests = {generate_task_key(fn) for fn in single_model_tested}

        tasks = self._build_resource_tasks(dbt_nodes, bundle, task_keys_with_tests)

        if bundle:
            tasks.extend(self._build_bundled_test_tasks(dbt_nodes, dbt_sources, single_model_tested))
            tasks.extend(self._build_standalone_test_tasks(standalone_tests))

        return tasks

    def _classify_tests(self, dbt_nodes: dict, dbt_sources: dict) -> tuple[set[str], list[tuple[str, dict]]]:
        """
        Classifies test nodes for bundled mode so that no test is silently dropped.

        - Tests with exactly 1 testable dep: will be covered by their resource's bundled
          `<resource>_tests` task under `--indirect-selection cautious`.
        - Tests with >1 testable deps (cross-model, e.g. `relationships`): emitted as their own
          tasks with multi-resource deps — `cautious` filters them out of bundles.
        - Tests with 0 testable deps (singular/custom tests that don't `ref()` or `source()`
          any resource): also emitted as their own tasks, since no bundle would pick them up.

        Returns:
            (single_model_tested, standalone_tests):
                - `single_model_tested`: full names of resources with at least one single-model
                  test — these become `<resource>_tests` bundled tasks.
                - `standalone_tests`: list of `(test_full_name, test_node_info)` for tests
                  that must run as individual tasks (cross-model or zero-dep).
        """
        single_model_tested: set[str] = set()
        standalone_tests: list[tuple[str, dict]] = []
        for node_full_name, node_info in dbt_nodes.items():
            if node_info['resource_type'] != 'test':
                continue
            testable_deps: list[str] = []
            for dep in node_info.get('depends_on', {}).get('nodes', []):
                if dep.startswith(self._DBT_TEST_TARGET_PREFIXES) and (dep in dbt_nodes or dep in dbt_sources):
                    testable_deps.append(dep)
            if len(testable_deps) == 1:
                single_model_tested.add(testable_deps[0])
            else:
                standalone_tests.append((node_full_name, node_info))
        return single_model_tested, standalone_tests

    def _build_resource_tasks(
        self,
        dbt_nodes: dict,
        bundle: bool,
        task_keys_with_tests: set[str],
    ) -> list[DbtTask]:
        """Builds tasks for every non-test resource (plus per-test tasks when not bundling)."""
        tasks: list[DbtTask] = []
        for node_full_name, node_info in dbt_nodes.items():
            resource_type = node_info['resource_type']
            if resource_type not in self.task_factories:
                continue
            if bundle and resource_type == 'test':
                continue

            task_key = generate_task_key(node_full_name)
            factory = self.task_factories[resource_type]
            task = factory.create_task(node_info['name'], node_info, task_key)

            if bundle and resource_type in self._GATEABLE_TYPES:
                task = replace(task, depends_on=self._rewire_deps(task.depends_on, task_keys_with_tests))

            tasks.append(task)
        return tasks

    @staticmethod
    def _rewire_deps(deps: list[str] | None, task_keys_with_tests: set[str]) -> list[str]:
        """Rewrites dependencies that point at a tested resource to its `_tests` gating task."""
        rewired: list[str] = []
        for dep_key in deps or []:
            rewired.append(f"{dep_key}_tests" if dep_key in task_keys_with_tests else dep_key)
        return rewired

    def _build_bundled_test_tasks(
        self,
        dbt_nodes: dict,
        dbt_sources: dict,
        nodes_with_tests: set[str],
    ) -> list[DbtTask]:
        """Emits one `<resource>_tests` task per tested resource using `TestTaskFactory.create_bundled_task`."""
        test_factory = self.task_factories['test']
        tasks: list[DbtTask] = []
        for full_name in sorted(nodes_with_tests):
            is_source = full_name.startswith('source.')
            info = dbt_sources[full_name] if is_source else dbt_nodes[full_name]
            resource_task_key = generate_task_key(full_name)
            bare_name = info['name']
            qualified = f"{info['package_name']}.{bare_name}"
            select = f"source:{info['package_name']}.{info['source_name']}.{bare_name}" if is_source else qualified
            tasks.append(
                test_factory.create_bundled_task(
                    task_key=f"{resource_task_key}_tests",
                    select=select,
                    deps_command_name=bare_name,
                    depends_on=[] if is_source else [resource_task_key],
                )
            )
        return tasks

    def _build_standalone_test_tasks(
        self,
        standalone_tests: list[tuple[str, dict]],
    ) -> list[DbtTask]:
        """
        Emits one task per standalone test — cross-model tests (e.g. `relationships`) gated on
        every referenced resource, plus any zero-dep singular tests that bundles can't cover.
        """
        test_factory = self.task_factories['test']
        tasks: list[DbtTask] = []
        for test_full_name, test_info in sorted(standalone_tests, key=lambda item: item[0]):
            test_task_key = generate_task_key(test_full_name)
            tasks.append(test_factory.create_task(test_info['name'], test_info, test_task_key))
        return tasks
