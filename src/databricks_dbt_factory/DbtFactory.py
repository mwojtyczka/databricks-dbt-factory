from collections import Counter
from dataclasses import dataclass, replace
from pathlib import PurePosixPath

from databricks_dbt_factory.TaskFactory import TaskFactory
from databricks_dbt_factory.DbtTask import DbtTask
from databricks_dbt_factory.Utils import build_task_key_maps


@dataclass(frozen=True)
class SelectorFacts:
    """
    The manifest-wide facts `DbtFactory._fqn_select` needs to build an unambiguous `--select`.

    Computed once per manifest and passed down, rather than held as factory state, so selector
    construction never depends on the order in which the task builders run.
    """

    # Flattened fqns that a plain selector would match for more than one node.
    ambiguous_fqns: frozenset[tuple[str, ...]]
    # Base file names claimed by exactly one node, so `file:<name>` identifies that node alone.
    unique_file_names: frozenset[str]


class DbtFactory:
    """A factory for generating Databricks job definitions from dbt manifests."""

    def __init__(
        self,
        task_factories: dict[str, TaskFactory],
        bundle_tests: bool = False,
    ):
        """
        Initializes the dbt factory.

        Args:
            task_factories (dict[str, TaskFactory]): Maps dbt resource types (`model`, `seed`,
                `snapshot`, `test`) to their respective `TaskFactory` instances. Omitting `test`
                disables test-task generation entirely.
            bundle_tests (bool): When True, emit one bundled `<resource>_test` task per tested
                resource and rewire downstream models/seeds/snapshots to depend on the upstream's
                bundled test task so failing tests halt the DAG. When False, emit one task per
                dbt test node.
        """
        self.task_factories = task_factories
        self.bundle_tests = bundle_tests

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

    # Characters that change how dbt parses a selector component, so a component containing one
    # cannot be used to identify a node. Derived from dbt's own grammar and each confirmed against
    # dbt 1.12.0 with `dbt ls`:
    #   ' '    union separator (`graph/cli.py` splits the raw spec on spaces), and the fragments are
    #          matched independently, so a leading fragment can select an unrelated node
    #   ','    intersection separator — the component is read as two, matching zero nodes
    #   '*?[]' `fnmatch` pattern syntax — `*` pulls in other nodes, `[...]` matches nothing
    #   ':'    method prefix (`RAW_SELECTOR_PATTERN`) — dbt raises InvalidSelectorError
    # `@` and `+` are graph operators but only at the very start/end, so they are checked separately.
    _SELECTOR_METACHARACTERS = frozenset(' ,*?[]:')

    @staticmethod
    def _flat_fqn(fqn: list[str]) -> tuple[str, ...]:
        """
        The fqn as dbt compares it: each segment re-split on dots, since dbt treats a dot inside a
        resource name as a namespace separator (`is_selected_node` flattens before matching). So
        `models/marts/orders.items.sql` has fqn `[pkg, marts, 'orders.items']` but is matched as
        `(pkg, marts, orders, items)` — and is therefore selected by `pkg.marts.orders`.
        """
        return tuple(part for segment in fqn for part in segment.split('.'))

    @classmethod
    def _fqn_select(cls, node_info: dict, facts: 'SelectorFacts') -> str:
        """
        Returns the dbt `--select` argument for a node: its fully qualified name (fqn) joined by
        dots, intersected with a `file:` selector when the plain fqn would over-select.

        `facts` carries the manifest-wide information this needs (see `SelectorFacts`), passed in
        rather than held as state so this never depends on call order.

        fqn selection is hierarchical by design: dbt matches a selector as a positional path
        *prefix* — of the fqn, or per `QualifiedNameSelectorMethod` of the fqn with its package
        stripped — which is what makes `--select staging` build everything under `staging/`. That is
        the desired behaviour nearly everywhere, including for a model and its unit tests (whose fqn
        nests under the model's): a resource task is filtered by resource type, and a bundled test
        task is meant to sweep its resource's unit tests in.

        The exception is a model directory named after a sibling model (`models/marts/orders.sql`
        beside `models/marts/orders/items.sql`, or beside `models/marts/orders.items.sql`). There
        `orders`' fqn is a prefix of the sibling's, so the plain selector builds it inside `orders`'
        task — ignoring its own dependency wiring — as well as in its own task, concurrently: two
        dbt runs writing one table. For those nodes only, the selector is intersected with
        `file:<file name>`, which pins it to the one node.

        `file:` rather than `path:`, because `path:` is resolved by globbing the *root project's*
        directory while a package node's `original_file_path` is relative to the *package* root — so
        `path:` selects nothing at all for package nodes, and `dbt run` then exits 0 having built
        nothing. `file:` matches the base name only, so it behaves the same either way. And not a
        bare-name intersection, since dbt also matches the package-stripped fqn, which the
        descendant satisfies too.

        Every component is validated with `_is_usable_component` before use, and a `file:` term is
        only used when it identifies exactly one node.

        Raises:
            ValueError: when no combination of components can isolate the node. Generation fails
                loudly rather than emit a selector that would silently select nothing, or the wrong
                node, at run time.
        """
        fqn = node_info.get('fqn')
        if not fqn:
            return node_info['name']

        fqn_usable = all(cls._is_usable_component(segment) for segment in fqn)
        file_name = PurePosixPath(node_info.get('original_file_path') or '').name
        # A `file:` term only identifies the node if no other node shares the file: every test in one
        # `schema.yml` shares its path, so `file:schema.yml` would select all of them.
        file_usable = cls._is_usable_component(file_name) and file_name in facts.unique_file_names

        if fqn_usable:
            if cls._flat_fqn(fqn) not in facts.ambiguous_fqns:
                return '.'.join(fqn)
            if file_usable:
                return f'{".".join(fqn)},file:{file_name}'
            raise cls._unisolable(node_info, fqn, file_name, 'its fqn is ambiguous with a nested node')

        if not file_usable:
            raise cls._unisolable(node_info, fqn, file_name, 'its fqn cannot be used in a selector')
        # Without the fqn there is no package scoping, and `file:` matches base names in every
        # package — two packages may each have an `orders.sql` — so `package:` restores it.
        package = node_info.get('package_name') or ''
        if cls._is_usable_component(package):
            return f'file:{file_name},package:{package}'
        return f'file:{file_name}'

    @classmethod
    def _is_usable_component(cls, value: str) -> bool:
        """
        Whether `value` can appear in a `--select` component and still mean itself.

        Rejects the empty string, anything containing `_SELECTOR_METACHARACTERS`, and a leading `@`
        or `+N` / trailing `+N`, which dbt's `RAW_SELECTOR_PATTERN` reads as graph operators
        (`@model` = model and its parents' children, `model+` = model and its children).
        """
        if not value:
            return False
        if cls._SELECTOR_METACHARACTERS & set(value):
            return False
        return not (value.startswith(('@', '+')) or value.endswith('+'))

    @staticmethod
    def _unisolable(node_info: dict, fqn: list[str], file_name: str, reason: str) -> ValueError:
        """Builds the error raised when no selector can identify a node, naming the remedy."""
        return ValueError(
            f'Cannot generate a task for {node_info.get("name")!r}: no selector can isolate it, because {reason} '
            f'and its file name {file_name!r} is unusable (empty, shared with another node, or containing characters '
            f'dbt reads as selector syntax). dbt fqn: {fqn!r}. Rename the file, its directory, or the resource.'
        )

    @classmethod
    def _unique_file_names(cls, dbt_manifest: dict) -> frozenset[str]:
        """
        Base file names that exactly one node in the manifest claims, so a `file:` term naming one
        selects a single node.

        dbt points every schema test at the `.yml` that declares it, so a shared `schema.yml` cannot
        identify any of them; `file:` also matches on the name alone, so two packages each having an
        `orders.sql` makes that name ambiguous too.
        """
        counts: Counter[str] = Counter()
        for group in ('nodes', 'sources', 'unit_tests'):
            for info in dbt_manifest.get(group, {}).values():
                name = PurePosixPath(info.get('original_file_path') or '').name
                if name:
                    counts[name] += 1
        return frozenset(name for name, count in counts.items() if count == 1)

    @classmethod
    def _compute_ambiguous_fqns(cls, dbt_manifest: dict) -> frozenset[tuple[str, ...]]:
        """
        The flattened fqns whose plain selector would also match a *buildable* node nested beneath
        them, and which `_fqn_select` therefore pins.

        Only models, seeds and snapshots count as the nested node here — those are the resources a
        selector would wrongly build a second time. A unit test's fqn also nests under its model's
        (`pkg.marts.orders.ut_orders` under `pkg.marts.orders`), but sweeping it in is exactly what
        a bundled test task is for, and a resource task is filtered by resource type so it can never
        run one. Counting unit tests would pin nearly every unit-tested model in a conventional
        project for no benefit.
        """
        nodes = dbt_manifest.get('nodes', {})
        flat_fqns = {unique_id: cls._flat_fqn(info['fqn']) for unique_id, info in nodes.items() if info.get('fqn')}
        buildable = []
        for unique_id, fqn in flat_fqns.items():
            if nodes[unique_id].get('resource_type') in cls._GATEABLE_TYPES:
                buildable.append(fqn)
        counts = Counter(flat_fqns.values())
        all_fqns = set(counts)
        # Two nodes can flatten to the *same* tuple (`orders.items.sql` and `orders/items.sql`), which
        # a plain selector matches for both, so an fqn claimed more than once is ambiguous in itself.
        ambiguous = {fqn for fqn, count in counts.items() if count > 1}
        # Otherwise an fqn needs pinning iff a buildable node's fqn strictly extends it.
        ambiguous.update(fqn[:i] for fqn in buildable for i in range(1, len(fqn)) if fqn[:i] in all_fqns)
        return frozenset(ambiguous)

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
        dbt_unit_tests = dbt_manifest.get('unit_tests', {})
        facts = SelectorFacts(
            ambiguous_fqns=self._compute_ambiguous_fqns(dbt_manifest),
            unique_file_names=self._unique_file_names(dbt_manifest),
        )

        bundle = 'test' in self.task_factories and self.bundle_tests
        single_model_tested: set[str] = set()
        standalone_tests: list[tuple[str, dict]] = []
        if bundle:
            single_model_tested, standalone_tests = self._classify_tests(dbt_nodes, dbt_sources, dbt_unit_tests)
        standalone_test_ids = {full_name for full_name, _ in standalone_tests}

        # Unit tests live under the manifest `unit_tests` key, not `nodes`. In per-test mode each
        # gets its own task (those whose target model is absent are skipped), so include their ids
        # here to receive a task key from `build_task_key_maps`.
        unit_test_ids = (
            self._emitted_unit_test_ids(dbt_unit_tests, dbt_nodes)
            if not bundle and 'test' in self.task_factories
            else []
        )

        task_ids = []
        for full_name, info in dbt_nodes.items():
            if self._node_gets_own_task(full_name, info, bundle, standalone_test_ids):
                task_ids.append(full_name)
        task_ids += unit_test_ids
        task_keys, bundled_test_keys = build_task_key_maps(task_ids, sorted(single_model_tested))

        tests_by_resource: dict[str, list[tuple[str, frozenset[str]]]] = {}
        ancestors: dict[str, set[str]] = {}
        if not bundle and 'test' in self.task_factories:
            tests_by_resource = self._index_tests_by_resource(dbt_nodes, dbt_sources, dbt_unit_tests, task_keys)
            ancestors = self._compute_ancestors(dbt_nodes, dbt_sources)

        tasks = self._build_resource_tasks(
            dbt_nodes,
            bundle,
            task_keys,
            bundled_test_keys,
            tests_by_resource,
            ancestors,
            facts,
        )

        if bundle:
            tasks.extend(
                self._build_bundled_test_tasks(
                    dbt_nodes,
                    dbt_sources,
                    single_model_tested,
                    task_keys,
                    bundled_test_keys,
                    facts,
                )
            )
            tasks.extend(self._build_standalone_test_tasks(standalone_tests, task_keys, facts))
        elif 'test' in self.task_factories:
            tasks.extend(self._build_unit_test_tasks(dbt_unit_tests, task_keys, facts))

        return tasks

    def _node_gets_own_task(self, full_name: str, node_info: dict, bundle: bool, standalone_test_ids: set[str]) -> bool:
        """
        Whether a `dbt_nodes` entry becomes its own task (and so receives a task key). True for any
        resource type with a factory, except single-model test nodes in bundle mode — those fold
        into their resource's bundled test task. The single authority for this decision, so the
        task-key map and the task-building loops stay in agreement.
        """
        resource_type = node_info['resource_type']
        if resource_type not in self.task_factories:
            return False
        if bundle and resource_type == 'test' and full_name not in standalone_test_ids:
            return False
        return True

    def _emitted_unit_test_ids(self, dbt_unit_tests: dict, dbt_nodes: dict) -> list[str]:
        """
        Full names of the unit tests that become their own task in per-test mode: those whose
        target model resolves and is present in the manifest. This is the emission decision for
        unit tests — the returned ids enter `task_ids`, so a unit test's presence in `task_keys`
        is how every consumer knows it was emitted.
        """
        ids: list[str] = []
        for unit_test_full_name, unit_test_info in dbt_unit_tests.items():
            model_full_name = self._unit_test_model(unit_test_info)
            if model_full_name is not None and model_full_name in dbt_nodes:
                ids.append(unit_test_full_name)
        return ids

    def _compute_ancestors(self, dbt_nodes: dict, dbt_sources: dict) -> dict[str, set[str]]:
        """
        Maps each testable resource's full name to the set of resources it transitively depends
        on (not including itself). Used in per-test mode to decide whether a test can safely
        gate a downstream node: a test `T` with refs `R` is only safe to add to node `N`'s
        deps if `R ⊆ ancestors(N)` — i.e. `N` already waits for all of `T`'s endpoints,
        transitively. Otherwise adding `T` would create a cycle (since `T` depends on each
        ref, and some ref might depend on `N`).
        """
        ancestors: dict[str, set[str]] = {}

        def visit(full_name: str) -> set[str]:
            cached = ancestors.get(full_name)
            if cached is not None:
                return cached
            result: set[str] = set()
            info = dbt_nodes.get(full_name) or dbt_sources.get(full_name)
            if info is not None:
                for dep in info.get('depends_on', {}).get('nodes', []):
                    if dep in dbt_nodes or dep in dbt_sources:
                        result.add(dep)
                        result.update(visit(dep))
            ancestors[full_name] = result
            return result

        for full_name in list(dbt_nodes.keys()) + list(dbt_sources.keys()):
            visit(full_name)
        return ancestors

    def _index_tests_by_resource(
        self, dbt_nodes: dict, dbt_sources: dict, dbt_unit_tests: dict, task_keys: dict[str, str]
    ) -> dict[str, list[tuple[str, frozenset[str]]]]:
        """
        Maps each testable resource's full name to a list of (test_task_key, test_refs) pairs
        for tests whose `severity` is `error` (the default). Warn-severity tests still run but
        are NOT indexed here, so they do not appear in any downstream model's `depends_on` —
        their job is to surface findings, not halt the DAG. This matches `dbt build` semantics:
        dbt itself exits 0 on warn-severity failures, so even if we did gate on them the
        Databricks task would succeed and downstream would run; keeping warn tests out of the
        dep graph just avoids the extra DAG clutter.

        Unit tests are indexed too. A unit test has no severity — it always fails the run — so it
        always gates. Only unit tests that were emitted as tasks (present in `task_keys`) are
        indexed, so a downstream node never gates on a unit-test task that was skipped.

        The refs set is carried alongside each test so `_extend_deps_with_upstream_tests` can
        avoid cycles: a test with refs that aren't all ancestors of a candidate node would
        create a cycle if added as that node's dep.
        """
        index: dict[str, list[tuple[str, frozenset[str]]]] = {}
        for node_full_name, node_info in dbt_nodes.items():
            if node_info['resource_type'] != 'test':
                continue
            if self._test_severity(node_info) != 'error':
                continue
            if node_full_name in task_keys:
                self._index_test(index, task_keys[node_full_name], node_info, dbt_nodes, dbt_sources)

        for unit_test_full_name, unit_test_info in dbt_unit_tests.items():
            if unit_test_full_name in task_keys:
                self._index_test(index, task_keys[unit_test_full_name], unit_test_info, dbt_nodes, dbt_sources)
        return index

    def _index_test(
        self,
        index: dict[str, list[tuple[str, frozenset[str]]]],
        test_task_key: str,
        test_info: dict,
        dbt_nodes: dict,
        dbt_sources: dict,
    ) -> None:
        """Indexes a test (data or unit) under each resource it references, carrying its ref set."""
        refs = self._testable_refs(test_info, dbt_nodes, dbt_sources)
        for resource_full in refs:
            index.setdefault(resource_full, []).append((test_task_key, refs))

    def _testable_refs(self, test_info: dict, dbt_nodes: dict, dbt_sources: dict) -> frozenset[str]:
        """Returns the models/seeds/snapshots/sources a test references, as present in the manifest."""
        refs: set[str] = set()
        for dep in test_info.get('depends_on', {}).get('nodes', []):
            if dep.startswith(self._DBT_TEST_TARGET_PREFIXES) and (dep in dbt_nodes or dep in dbt_sources):
                refs.add(dep)
        return frozenset(refs)

    @staticmethod
    def _test_severity(test_node_info: dict) -> str:
        """Reads the test's severity from the manifest, defaulting to `error` when unset."""
        config = test_node_info.get('config') or {}
        severity = config.get('severity')
        if isinstance(severity, str):
            return severity.lower()
        return 'error'

    @staticmethod
    def _unit_test_model(unit_test_info: dict) -> str | None:
        """
        Returns the full name of the model a unit test targets, or None if it can't be resolved.

        Prefers dbt's already-resolved `depends_on.nodes[0]`, which is the authority: dbt resolves
        the `model` field to a node id at parse time and rewrites unit tests on *versioned* models
        to target `model.<pkg>.<name>.v<N>` while leaving `model` as the bare name. Rebuilding
        `model.<pkg>.<model>` from the `model` field would therefore miss versioned models
        entirely (and dbt's `<name> <version>` spelling of the field), silently dropping the unit
        test. Falls back to that reconstruction only when `depends_on` is absent, so manifests
        that predate it keep working.
        """
        for dep in unit_test_info.get('depends_on', {}).get('nodes', []):
            if dep.startswith('model.'):
                return dep
        model = unit_test_info.get('model')
        package = unit_test_info.get('package_name')
        if model and package:
            return f'model.{package}.{model}'
        return None

    @staticmethod
    def _extend_deps_with_upstream_tests(
        node_full_name: str,
        existing_deps: list[str] | None,
        tests_by_resource: dict[str, list[tuple[str, frozenset[str]]]],
        ancestors_by_node: dict[str, set[str]],
    ) -> list[str]:
        """
        Appends task keys of tests that safely gate this node — i.e. tests whose refs are all
        ancestors of the current node. This prevents both direct and transitive cycles: a test
        `T` with refs `R` is added to node `N`'s deps only if `N` transitively depends on every
        resource in `R`. If any ref of `T` is downstream of (or equal to) `N`, adding `T` would
        cycle because `T` already depends on that ref, and the ref depends on `N`.
        """
        extended: list[str] = list(existing_deps or [])
        seen = set(extended)
        node_ancestors = ancestors_by_node.get(node_full_name, set())
        for ancestor in node_ancestors:
            for test_key, test_refs in tests_by_resource.get(ancestor, []):
                if test_key in seen:
                    continue
                if test_refs <= node_ancestors:
                    extended.append(test_key)
                    seen.add(test_key)
        return extended

    def _classify_tests(
        self, dbt_nodes: dict, dbt_sources: dict, dbt_unit_tests: dict
    ) -> tuple[set[str], list[tuple[str, dict]]]:
        """
        Classifies test nodes for bundled mode so that no test is silently dropped.

        - Tests with exactly 1 testable dep: will be covered by their resource's bundled
          `<resource>_test` task under `--indirect-selection cautious`.
        - Tests with >1 testable deps (cross-model, e.g. `relationships`): emitted as their own
          tasks with multi-resource deps — `cautious` filters them out of bundles.
        - Tests with 0 testable deps (singular/custom tests that don't `ref()` or `source()`
          any resource): also emitted as their own tasks, since no bundle would pick them up.

        A model's bundled test task selects the model with `--indirect-selection cautious`, which
        sweeps in the model's unit tests as well. Models that already have a single-model data
        test therefore cover their unit tests for free. A model with *only* unit tests is added to
        `single_model_tested` here so it still gets a bundled task.

        Returns:
            (single_model_tested, standalone_tests):
                - `single_model_tested`: full names of resources with at least one single-model
                  test — these become bundled test tasks.
                - `standalone_tests`: list of `(test_full_name, test_node_info)` for tests
                  that must run as individual tasks (cross-model or zero-dep).
        """
        single_model_tested: set[str] = set()
        standalone_tests: list[tuple[str, dict]] = []
        for node_full_name, node_info in dbt_nodes.items():
            if node_info['resource_type'] != 'test':
                continue
            testable_deps = self._testable_refs(node_info, dbt_nodes, dbt_sources)
            if len(testable_deps) == 1:
                single_model_tested.add(next(iter(testable_deps)))
            else:
                standalone_tests.append((node_full_name, node_info))

        for unit_test_info in dbt_unit_tests.values():
            model_full_name = self._unit_test_model(unit_test_info)
            if model_full_name is not None and model_full_name in dbt_nodes:
                single_model_tested.add(model_full_name)
        return single_model_tested, standalone_tests

    def _build_resource_tasks(
        self,
        dbt_nodes: dict,
        bundle: bool,
        task_keys: dict[str, str],
        bundled_test_keys: dict[str, str],
        tests_by_resource: dict[str, list[tuple[str, frozenset[str]]]],
        ancestors_by_node: dict[str, set[str]],
        facts: SelectorFacts,
    ) -> list[DbtTask]:
        """Builds tasks for every non-test resource (plus per-test tasks when not bundling)."""
        # Maps a tested resource's task key (what `depends_on` holds) to its gating bundled test
        # task key, for rewiring in bundle mode. Sources have a bundled test key but no run task,
        # so they are absent from `task_keys` and skipped.
        bundled_test_key_by_task_key = {task_keys[fn]: key for fn, key in bundled_test_keys.items() if fn in task_keys}
        tasks: list[DbtTask] = []
        for node_full_name, node_info in dbt_nodes.items():
            if node_full_name not in task_keys:
                continue
            if bundle and node_info['resource_type'] == 'test':
                # Standalone tests are keyed but built by `_build_standalone_test_tasks`, not here.
                continue

            resource_type = node_info['resource_type']
            task_key = task_keys[node_full_name]
            factory = self.task_factories[resource_type]
            task = factory.create_task(
                self._fqn_select(node_info, facts),
                node_info['name'],
                node_info,
                task_key,
                task_keys,
            )

            if resource_type in self._GATEABLE_TYPES:
                if bundle:
                    task = replace(task, depends_on=self._rewire_deps(task.depends_on, bundled_test_key_by_task_key))
                elif tests_by_resource:
                    task = replace(
                        task,
                        depends_on=self._extend_deps_with_upstream_tests(
                            node_full_name, task.depends_on, tests_by_resource, ancestors_by_node
                        ),
                    )

            tasks.append(task)
        return tasks

    @staticmethod
    def _rewire_deps(deps: list[str] | None, bundled_test_key_by_task_key: dict[str, str]) -> list[str]:
        """Rewrites a dependency on a tested resource to that resource's gating bundled test task."""
        return [bundled_test_key_by_task_key.get(dep_key, dep_key) for dep_key in (deps or [])]

    def _build_bundled_test_tasks(
        self,
        dbt_nodes: dict,
        dbt_sources: dict,
        nodes_with_tests: set[str],
        task_keys: dict[str, str],
        bundled_test_keys: dict[str, str],
        facts: SelectorFacts,
    ) -> list[DbtTask]:
        """Emits one bundled `<resource>_test` task per tested resource via `TestTaskFactory.create_bundled_task`."""
        test_factory = self.task_factories['test']
        tasks: list[DbtTask] = []
        for full_name in sorted(nodes_with_tests):
            is_source = full_name.startswith('source.')
            info = dbt_sources[full_name] if is_source else dbt_nodes[full_name]
            bare_name = info['name']
            if is_source:
                select = f"source:{info['package_name']}.{info['source_name']}.{bare_name}"
            else:
                select = self._fqn_select(info, facts)
            tasks.append(
                test_factory.create_bundled_task(
                    task_key=bundled_test_keys[full_name],
                    select=select,
                    deps_command_name=bare_name,
                    depends_on=[] if is_source else [task_keys[full_name]],
                )
            )
        return tasks

    def _build_standalone_test_tasks(
        self,
        standalone_tests: list[tuple[str, dict]],
        task_keys: dict[str, str],
        facts: SelectorFacts,
    ) -> list[DbtTask]:
        """
        Emits one task per standalone test — cross-model tests (e.g. `relationships`) gated on
        every referenced resource, plus any zero-dep singular tests that bundles can't cover.
        """
        test_factory = self.task_factories['test']
        tasks: list[DbtTask] = []
        for test_full_name, test_info in sorted(standalone_tests, key=lambda item: item[0]):
            test_task_key = task_keys[test_full_name]
            tasks.append(
                test_factory.create_task(
                    self._fqn_select(test_info, facts),
                    test_info['name'],
                    test_info,
                    test_task_key,
                    task_keys,
                )
            )
        return tasks

    def _build_unit_test_tasks(
        self,
        dbt_unit_tests: dict,
        task_keys: dict[str, str],
        facts: SelectorFacts,
    ) -> list[DbtTask]:
        """
        Emits one task per unit test, selected by its full FQN and gated on the model it tests.
        Only unit tests that received a task key (see `_emitted_unit_test_ids`) are emitted; unit
        tests whose target model is absent from the manifest were never keyed and are skipped, so
        their task can't gate on a model task that is never created. Used in per-test mode; in
        bundled mode a model's bundled test task covers its unit tests via `--indirect-selection cautious`.
        """
        test_factory = self.task_factories['test']
        tasks: list[DbtTask] = []
        for unit_test_full_name, unit_test_info in sorted(dbt_unit_tests.items()):
            if unit_test_full_name not in task_keys:
                continue
            tasks.append(
                test_factory.create_task(
                    self._fqn_select(unit_test_info, facts),
                    unit_test_info['name'],
                    unit_test_info,
                    task_keys[unit_test_full_name],
                    task_keys,
                )
            )
        return tasks
