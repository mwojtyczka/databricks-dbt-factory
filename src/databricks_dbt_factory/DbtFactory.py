from dataclasses import replace
from pathlib import PurePosixPath

from databricks_dbt_factory.TaskFactory import TaskFactory
from databricks_dbt_factory.DbtTask import DbtTask
from databricks_dbt_factory.Utils import build_task_key_maps


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

    # Characters that change how dbt parses a selector *component*, so a component containing one
    # cannot be used to address a node. Derived from dbt's grammar, each confirmed against dbt 1.12.0
    # with `dbt ls`:
    #   ' '    union separator (`graph/cli.py` splits the raw spec on spaces); the fragments then
    #          match independently, so a leading fragment can select an unrelated node
    #   ','    intersection separator — the component is read as two, matching zero nodes
    #   '*?[]' `fnmatch` pattern syntax — `*` pulls in other nodes, `[...]` matches nothing
    #   ':'    method prefix (`RAW_SELECTOR_PATTERN`) — dbt raises InvalidSelectorError
    _SELECTOR_METACHARACTERS = frozenset(' ,*?[]:')

    @classmethod
    def _node_select(
        cls, node_info: dict, source_info: dict | None = None, nodes_per_file: dict[str, int] | None = None
    ) -> str:
        """
        Returns the dbt `--select` argument addressing exactly one node.

        Every node is addressed the same way — the intersection (`,`, dbt's AND) of every independent
        fact the manifest gives us about it:

            <fqn>,package:<package>,file:<file name>[,test_name:<generic test>]

        No term is exact alone, which is why they are combined rather than chosen between:

        * the **fqn** is matched as a positional path *prefix*, so it also selects nodes nested
          beneath it — and dbt compares a node's fqn with its package stripped too, so a package's
          `models/probe/alpha.sql` is matched by the root project's `pkg.alpha` as well;
        * **`package:`** narrows to the owning package, but matches all of it;
        * **`file:`** narrows to one source file, but matches that base name in every package, and a
          `schema.yml` is shared by every test declared in it;
        * **`test_name:`** narrows a generic test to its type (`not_null`, `unique`), separating tests
          that share a `schema.yml` — but only when their types differ, so two `not_null` tests in one
          file still need the bare **name**, which dbt matches against the fqn's leaf. The name stands
          in for the fqn when the fqn itself is unusable.

        A term is omitted when dbt's own grammar cannot express it literally — see
        `_is_usable_component` — so an awkward directory name costs one term rather than the whole
        selector. `source_info` addresses a source's tests, which dbt selects by
        `source:<package>.<source>.<table>` rather than by fqn.

        Raises:
            ValueError: when the surviving terms cannot single the node out — no term at all, only
                `package:`, or a `package:`/`file:` pair whose file holds more than one resource.
                Emitting such a selector would run every resource in the package or every test in one
                `schema.yml`, so generation fails loudly at build time instead of doing the wrong
                thing at run time.

        `nodes_per_file` counts how many manifest resources share each `original_file_path`, which is
        what makes the `file:`-only case decidable: a file holding a single resource is addressed
        exactly by `package:`+`file:`, so a name dbt cannot express costs nothing there. Absent the
        count (an unfiltered direct call) a shared file is assumed, the conservative reading.
        """
        if source_info is not None:
            return cls._source_select(source_info)

        terms: list[str] = []
        discriminating = False
        fqn = node_info.get('fqn') or []
        if fqn and cls._is_usable_selector('.'.join(fqn)) and all(cls._is_usable_component(part) for part in fqn):
            terms.append('.'.join(fqn))
            discriminating = True
        elif cls._is_usable_component(name := node_info.get('name') or '') and cls._is_usable_selector(name):
            # The fqn is unusable, so fall back to the bare resource name, which dbt matches against
            # the fqn's leaf. It is the only term that tells apart two nodes sharing a package, a
            # file and a test type — two `not_null` tests in one `schema.yml`, say. Not used *with* a
            # usable fqn, whose leaf already carries it.
            #
            # `_is_usable_selector` applies here too: unlike an fqn segment, a bare name *is* the
            # whole raw selector, so a name like `orders+1` would be read as a graph operator and
            # select the wrong node (or none). An fqn segment is shielded by the package prefix.
            terms.append(name)
            discriminating = True

        package = node_info.get('package_name') or ''
        if cls._is_usable_component(package):
            terms.append(f'package:{package}')

        file_path = node_info.get('original_file_path') or ''
        file_name = PurePosixPath(file_path).name
        if cls._is_usable_component(file_name):
            terms.append(f'file:{file_name}')
            # A file holding exactly one resource pins it as tightly as its name would.
            if nodes_per_file is not None and nodes_per_file.get(file_path) == 1:
                discriminating = True

        test_name = (node_info.get('test_metadata') or {}).get('name') or ''
        if cls._is_usable_component(test_name):
            terms.append(f'test_name:{test_name}')

        # Something must single the node out: the fqn, the bare name, or a file it has to itself. The
        # remaining terms all address a *group* — `package:` the whole package, a shared `file:` every
        # resource declared in it, `test_name:` a whole test type — so any number of them can still
        # match several nodes. Confirmed with `dbt ls` on dbt 1.12.0, where a bracketed test name in a
        # shared schema.yml left `package:probe,file:schema.yml,test_name:not_null`, resolving to two
        # tests. Refuse at build time rather than emit a task that runs another task's node too.
        if not discriminating:
            raise cls._unaddressable(node_info, terms)
        return ','.join(terms)

    @classmethod
    def _source_select(cls, source_info: dict) -> str:
        """
        Returns the `source:<package>.<source>.<table>` selector for a source's tests.

        A source has no fqn selector of its own, so all three parts must be usable: unlike a node
        there is no other term to fall back on.

        `.` is disallowed on top of the usual metacharacters. It delimits this grammar, which accepts
        at most three parts, so a dot inside one makes four and dbt rejects the whole selector with a
        Runtime Error ("Invalid source selector value") rather than merely selecting nothing —
        confirmed against dbt 1.12.0 with `dbt ls`. A node's fqn is unaffected: there the dot is the
        separator we are already building with, and a dotted segment stays addressable.

        The finished string is checked with `_is_usable_selector` too: like a bare name, the whole
        `source:...` is one raw selector, so a trailing `+N` on the table is read as a graph operator
        and matches nothing while `dbt test` still exits 0. Only the boundary matters —
        `source:pkg.raw.2+ord` resolves exactly — so the check goes on the assembled string rather
        than on each part.
        """
        package = source_info.get('package_name') or ''
        source_name = source_info.get('source_name') or ''
        table = source_info.get('name') or ''
        parts = (package, source_name, table)
        if not all(cls._is_usable_component(part) and '.' not in part for part in parts):
            raise cls._unaddressable(source_info, [])
        select = f'source:{package}.{source_name}.{table}'
        if not cls._is_usable_selector(select):
            raise cls._unaddressable(source_info, [])
        return select

    @classmethod
    def _is_usable_component(cls, value: str) -> bool:
        """Whether `value` can appear inside a selector component and still mean itself."""
        return bool(value) and not cls._SELECTOR_METACHARACTERS & set(value)

    @staticmethod
    def _is_usable_selector(value: str) -> bool:
        """
        Whether `value` is safe as a whole raw selector, i.e. carries no graph operator at its
        boundary.

        dbt's `RAW_SELECTOR_PATTERN` reads a leading `@` or `N+` and a trailing `+N` as graph
        operators, so `pkg.orders+1` means "pkg.orders and its children one level deep" and selects
        the wrong model. The leading form is spelled `<digits>+`, so `2+orders` means "orders and two
        levels of its parents" — confirmed with `dbt ls` on dbt 1.12.0, where it selects a *different*
        model rather than nothing.

        Only the boundary matters: `pkg.+leading` and `pkg.raw.2+ord` are exact, both confirmed with
        `dbt ls`, so an operator character inside a segment is left alone.
        """
        return not (
            value.startswith(('@', '+'))
            or value.rstrip('0123456789').endswith('+')
            or value.lstrip('0123456789').startswith('+')
        )

    @staticmethod
    def _unaddressable(node_info: dict, terms: list[str]) -> ValueError:
        """Builds the error raised when no selector can address a node, naming the remedy."""
        return ValueError(
            f'Cannot generate a task for {node_info.get("name")!r}: no selector can address it uniquely. '
            f'Its dbt name and path carry characters dbt reads as selector syntax — a space, comma, '
            f'colon or one of *?[], a leading @/N+ or trailing +N that dbt reads as a graph operator, '
            f'or (for a source) a dot, which dbt takes as the source selector\'s own separator — '
            f'leaving only {terms or ["nothing"]}, which cannot single it out. '
            'Rename the resource, its file, or its directory.'
        )

    def _create_tasks(self, dbt_manifest: dict) -> list[DbtTask]:
        """
        Builds `DbtTask` instances from the manifest, applying the bundling and gating policies.

        Args:
            dbt_manifest (dict): Parsed dbt manifest content.

        Returns:
            list[DbtTask]: `DbtTask` instances (not yet rendered to dicts).
        """
        dbt_nodes = self._enabled_only(dbt_manifest.get('nodes', {}))
        dbt_sources = self._enabled_only(dbt_manifest.get('sources', {}))
        dbt_unit_tests = self._enabled_only(dbt_manifest.get('unit_tests', {}))
        nodes_per_file = self._count_nodes_per_file(dbt_nodes, dbt_unit_tests)

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
            nodes_per_file,
        )

        if bundle:
            tasks.extend(
                self._build_bundled_test_tasks(
                    dbt_nodes,
                    dbt_sources,
                    single_model_tested,
                    task_keys,
                    bundled_test_keys,
                    nodes_per_file,
                )
            )
            tasks.extend(self._build_standalone_test_tasks(standalone_tests, task_keys, nodes_per_file))
        elif 'test' in self.task_factories:
            tasks.extend(self._build_unit_test_tasks(dbt_unit_tests, task_keys, nodes_per_file))

        return tasks

    @staticmethod
    def _count_nodes_per_file(*entry_groups: dict) -> dict[str, int]:
        """
        Counts the resources declared in each `original_file_path`.

        A `schema.yml` holds every test and unit test declared in it, and two snapshots can share one
        `.sql` — but most files hold exactly one resource, and there `file:` is as precise as the
        resource's own name. `_node_select` needs the distinction to tell an exact `package:`+`file:`
        pair from one that would sweep in a sibling. Unit tests are counted alongside nodes because
        they share the same `.yml` files.
        """
        counts: dict[str, int] = {}
        for entries in entry_groups:
            for info in entries.values():
                path = info.get('original_file_path') or ''
                counts[path] = counts.get(path, 0) + 1
        return counts

    @staticmethod
    def _enabled_only(entries: dict) -> dict:
        """
        Drops entries dbt has disabled.

        dbt normally files an `enabled=false` resource under the manifest's own `disabled` key, which
        we never read — but not always: a versioned model whose declared version has no file leaves
        its test in `nodes` with `config.enabled` false. dbt selects nothing for such a node, and
        `dbt test` on a zero-match selector still exits 0, so a task built from it would go green
        having asserted nothing. Filtering here rather than at each decision site keeps the task-key
        map, the bundling classification and the dependency graph working from one view of the
        manifest. Confirmed against dbt 1.12.0.
        """
        return {
            full_name: info
            for full_name, info in entries.items()
            if (info.get('config') or {}).get('enabled') is not False
        }

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
        nodes_per_file: dict[str, int],
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
                self._node_select(node_info, nodes_per_file=nodes_per_file),
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
        nodes_per_file: dict[str, int],
    ) -> list[DbtTask]:
        """Emits one bundled `<resource>_test` task per tested resource via `TestTaskFactory.create_bundled_task`."""
        test_factory = self.task_factories['test']
        tasks: list[DbtTask] = []
        for full_name in sorted(nodes_with_tests):
            is_source = full_name.startswith('source.')
            info = dbt_sources[full_name] if is_source else dbt_nodes[full_name]
            bare_name = info['name']
            select = self._node_select(info, source_info=info if is_source else None, nodes_per_file=nodes_per_file)
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
        nodes_per_file: dict[str, int],
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
                    self._node_select(test_info, nodes_per_file=nodes_per_file),
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
        nodes_per_file: dict[str, int],
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
                    self._node_select(unit_test_info, nodes_per_file=nodes_per_file),
                    unit_test_info['name'],
                    unit_test_info,
                    task_keys[unit_test_full_name],
                    task_keys,
                )
            )
        return tasks
