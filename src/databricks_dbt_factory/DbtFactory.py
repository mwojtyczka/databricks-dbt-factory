from dataclasses import replace
from pathlib import PurePosixPath, PureWindowsPath

from databricks_dbt_factory.TaskFactory import TaskFactory
from databricks_dbt_factory.DbtTask import DbtTask
from databricks_dbt_factory.Utils import build_task_key_maps


# The resource types whose tasks run `dbt test`, and so are subject to dbt's indirect selection.
_TEST_TYPES = frozenset({'test', 'unit_test'})
# The `unique_id` prefixes of resources a test can be attached to.
_DBT_TEST_TARGET_PREFIXES = ('model.', 'seed.', 'snapshot.', 'source.')


def _flatten_fqn(fqn: list[str]) -> list[str]:
    """
    Flattens an fqn the way dbt does before comparing it, splitting every segment on `.`.

    `is_selected_node` treats dots inside a segment as namespace separators, so a test named
    `check.nested` with fqn `['probe', 'check.nested']` compares as `['probe', 'check', 'nested']` —
    which is why the shorter `probe.check` matches it as a subtree parent.
    """
    return [part for segment in fqn for part in segment.split('.')]


def _base_file_name(original_file_path: str) -> str:
    """
    Returns the base name dbt's `file:` selector matches.

    dbt assembles `original_file_path` with `os.path.join`/`os.path.relpath` and `FileSelectorMethod`
    splits it back with `Path(...).name` — both *platform-dependent*. So a manifest parsed on Windows
    holds `models\\marts\\orders.sql`, and there `\\` is the separator; on POSIX the same string is a
    single file whose name legitimately contains a backslash.

    Splitting on both separators unconditionally gets the POSIX case wrong: for `models/we\\ird.sql` it
    yields `ird.sql`, and the emitted `file:ird.sql` matches nothing while the task exits 0 — the same
    silent no-op it was meant to prevent, confirmed with `dbt ls` on dbt 1.12.0.

    So treat `\\` as a separator only when the path contains no `/`. A manifest generated on Windows uses
    `\\` throughout, while a POSIX path with a backslash in a file name still has `/` separators, which
    distinguishes the two without guessing at the *running* platform — the spec is generated on one
    machine and consumed by a Linux job, so `os.sep` here would be the wrong question.
    """
    if '/' in original_file_path:
        return PurePosixPath(original_file_path).name
    return PureWindowsPath(original_file_path).name


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
    # Shared with `_SelectorIndex`, which files tests by their parents; defined at module level so both
    # read the same definition rather than one reaching into the other.
    _TEST_TYPES = _TEST_TYPES
    _DBT_TEST_TARGET_PREFIXES = _DBT_TEST_TARGET_PREFIXES

    # Characters that change how a selector component is interpreted, so a component containing one
    # cannot be used to address a node. Each confirmed against dbt 1.12.0 with `dbt ls`.
    #
    # Most come from dbt's grammar:
    #   ' '    union separator (`graph/cli.py` splits the raw spec on spaces); the fragments then
    #          match independently, so a leading fragment can select an unrelated node
    #   ','    intersection separator — the component is read as two, matching zero nodes
    #   '*?[]' `fnmatch` pattern syntax — `*` pulls in other nodes, `[...]` matches nothing
    #   ':'    method prefix (`RAW_SELECTOR_PATTERN`) — dbt raises InvalidSelectorError
    #   '/'    dispatches the whole selector to `MethodName.Path` (`SelectionCriteria.default_method`
    #          calls `_probably_path`, which tests for a separator), so it is matched as a path and
    #          resolves to nothing — a test named `check/slash` silently never runs
    #
    # `{}` is *not* from dbt's grammar, which is why an earlier revision derived from that grammar
    # alone could not have caught it: Databricks substitutes `{{...}}` dynamic references in the dbt
    # commands field as plain text before the task runs, so a model under `models/{{job.id}}/` emits a
    # selector that resolves locally and matches nothing once substituted. The whole task then exits 0
    # having built nothing.
    _SELECTOR_METACHARACTERS = frozenset(' ,*?[]:/{}')

    # Suffixes that dispatch the *whole* selector to `MethodName.File` rather than matching an fqn
    # (`SelectionCriteria.default_method`). A model at `models/orders.sql.sql` is named `orders.sql`,
    # so its fqn selector `probe.orders.sql` is read as a file name and matches nothing while
    # `dbt run` still exits 0. Checked against the assembled selector, not each component, because
    # dispatch looks at the value as a whole.
    _FILE_METHOD_SUFFIXES = ('.sql', '.py', '.csv')

    @classmethod
    def _node_select(
        cls,
        node_info: dict,
        source_info: dict | None = None,
        peers: dict | None = None,
        expected_ids: set[str] | None = None,
    ) -> str:
        """
        Returns the dbt `--select` argument addressing exactly one node.

        `peers` is every selectable resource in the manifest, keyed by id, used to prove the finished
        selector matches only `node_info` (see `_assert_exact`). It is optional so the library API and
        unit tests can build a selector for a lone node, but callers with a manifest should pass it:
        without it, exactness is assumed rather than established. `expected_ids` names the `peers` keys
        the selector is *allowed* to match besides this node, for the version-clone group that shares a
        task.

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

        **The fqn or the bare name must survive, and the result must then be checked for exactness.**

        dbt has no `unique_id:` selector method, so a selector is always a *predicate*, and its
        exactness has to be established rather than assumed. Requiring the fqn or the bare name is
        necessary — `package:`, `file:`, `resource_type:` and `test_name:` each address a group — but it
        is *not* sufficient, which an earlier revision of this docstring got wrong. An fqn is a
        positional prefix over dbt's *flattened* fqn, so it names a subtree, not a node:

        * a test named `check.nested` flattens to `[probe, check, nested]`, so the sibling `check`'s
          selector `probe.check` matches it too — even with `package:`, `file:` and `test_name:` all
          present and identical;
        * a unit test on `orders` (`[probe, orders, unit_orders]`) and a data test named
          `orders.unit_orders` (`[probe, orders.unit_orders]`) flatten identically, which `resource_type:`
          does separate;
        * dbt clones a unit test per model version leaving both clones the same fqn, name and file, and
          `version:` does not apply to unit tests at all (it accepts only `latest`/`prerelease`/`old`/
          `none`), so the clones cannot be told apart — `_unit_test_groups` emits one task for the group
          instead;
        * two generic tests may simply *share* an fqn: dbt does not require test names to be unique and
          disambiguates in the `unique_id` hash only.

        So `_assert_exact` intersects the finished selector against the manifest — mirroring dbt's own
        matching, pinned against `dbt ls` in `tests/integration/test_selector_against_dbt.py` — and
        refuses the node when it is not alone. Refusing is the last resort but the only honest one: for
        an equal-fqn or prefix collision dbt offers no term that separates the nodes, so the alternative
        is a task that runs its neighbour's resource before that neighbour's dependencies have run.

        All of this is a per-node check against the rest of the manifest, which is stricter than dbt
        needs in one direction and unavoidable in the other: exactness genuinely is a property of the
        node *plus* its neighbours, and an earlier attempt to make it a property of the node alone is
        what let these collisions through.

        Raises:
            ValueError: when neither the fqn nor the bare name survives, or when the resulting selector
                also matches another resource. Generation fails at build time, naming the resource and
                the remedy, instead of emitting a task that would do the wrong thing at run time.
        """
        if source_info is not None:
            return cls._source_select(source_info)

        # Exactly one of the fqn and the bare name is used, and one of them must be: they are the only
        # terms that address a *single* resource, so a selector without either could run another task's
        # resource. dbt offers no `unique_id:` method — the full set it accepts, confirmed against dbt
        # 1.12.0, is `fqn`, `tag`, `source`, `path`, `file`, `package`, `config`, `test_name`,
        # `test_type`, `resource_type`, `state`, `exposure`, `metric`, `result`, `source_status`,
        # `group`, `version`, `access`, `semantic_model`, `saved_query`, `unit_test`, `selector`.
        terms: list[str] = []
        fqn = node_info.get('fqn') or []
        if fqn and cls._is_usable_selector('.'.join(fqn)) and all(cls._is_usable_component(part) for part in fqn):
            terms.append('.'.join(fqn))
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
        else:
            raise cls._unaddressable(node_info)

        package = node_info.get('package_name') or ''
        if cls._is_usable_component(package):
            terms.append(f'package:{package}')

        file_name = cls._base_file_name(node_info.get('original_file_path') or '')
        if cls._is_usable_component(file_name):
            terms.append(f'file:{file_name}')

        # `resource_type:` separates nodes whose flattened fqns coincide across *types*, which the
        # other terms cannot: a unit test `unit_orders` on `orders` (fqn [probe, orders, unit_orders])
        # and a data test named `orders.unit_orders` (fqn [probe, orders.unit_orders]) flatten to the
        # same string and share their file. Confirmed on dbt 1.12.0: adding `resource_type:unit_test`
        # and `resource_type:test` respectively isolates each to one node.
        #
        # It does *not* undo dbt's indirect selection: `resource_type:test` still admits the attached
        # tests eagerly pulled in with a selected model, because dbt intersects sets that are already
        # expanded. Nodes needing that are refused by `_assert_exact` instead.
        resource_type = node_info.get('resource_type') or ''
        if cls._is_usable_component(resource_type):
            terms.append(f'resource_type:{resource_type}')

        test_name = (node_info.get('test_metadata') or {}).get('name') or ''
        if cls._is_usable_component(test_name):
            terms.append(f'test_name:{test_name}')

        select = ','.join(terms)
        if peers is not None:
            cls._assert_exact(select, node_info, peers, expected_ids)
        return select

    @classmethod
    def _assert_exact(cls, select: str, node_info: dict, peers: dict, expected_ids: set[str] | None = None) -> None:
        """
        Raises unless `select` runs only the node it was built for.

        Mirrors dbt's actual pipeline, which is *expand each component, then intersect* — see
        `NodeSelector.select_nodes_recursively`, where every component goes through
        `get_nodes_from_criteria` and only then is combined with `spec.combined(indirect_sets)`. Modelling
        it the other way round (intersect, then expand) leaves a hole: a component can reach a model that
        the intersection later excludes, yet the model's attached tests are added *inside that component*
        and survive the intersection on their own terms.

        Verified on dbt 1.12.0. With `models/orders.sql` carrying `not_null`, and `models/other.sql`
        carrying `not_null: {name: orders}` in the shared `schema.yml`, the selector
        `probe.orders,package:probe,file:schema.yml,resource_type:test,test_name:not_null` resolves to
        *two* tests: the `probe.orders` component matches `model.probe.orders`, pulls in its
        `not_null_orders_id` eagerly, and that test independently satisfies `file:`, `resource_type:` and
        `test_name:`. The task depends only on `other_model`, so it asserts on `orders` before
        `orders_model` has built it — the exact failure this check exists to prevent.

        So each term's match set is expanded before intersecting, and expansion follows dbt's eager rule:
        **if ANY parent is selected, select the test** (`expand_selection`'s own comment). Under that rule
        a multi-endpoint `relationships` test leaks from a single endpoint too.

        `expected_ids` names the additional ids allowed, for a group that deliberately shares one task.
        The node recognises itself by object identity *or* by `unique_id`: identity alone would call a node
        its own collision if a caller passed a copy, and `unique_id` alone fails on the hand-written
        fixtures that omit the field.
        """
        allowed = cls._own_ids(node_info, peers) | (expected_ids or set())
        # Eager expansion only matters for a task whose command is `dbt test`: `dbt run`, `dbt seed` and
        # `dbt snapshot` build the resources they select and never execute a test, so a model task is
        # judged on its direct matches alone. Verified with `dbt ls` on dbt 1.12.0 — the same selector
        # returns just the model under `--resource-type model` while pulling a test in under `test`.
        expand = (node_info.get('resource_type') or '') in cls._TEST_TYPES
        run = cls._nodes_run_by(select, peers, expand=expand)
        surplus = run - allowed
        if surplus:
            raise cls._ambiguous(node_info, select, sorted(surplus))

    @staticmethod
    def _own_ids(node_info: dict, peers: dict) -> set[str]:
        """
        The `peers` keys that *are* this node, by `unique_id` when present or by object identity.

        `unique_id` is a direct dict lookup, so the common path costs nothing. The identity scan is the
        fallback for hand-written fixtures that omit the field — walking every peer here once per node was
        itself quadratic, and defeated the index added to avoid exactly that.
        """
        own_id = node_info.get('unique_id')
        if own_id is not None and peers.get(own_id) is not None:
            return {own_id}
        return {full_name for full_name, info in peers.items() if info is node_info}

    @classmethod
    def _nodes_run_by(cls, select: str, peers: dict, expand: bool = True) -> set[str]:
        """
        Every node dbt would run for `select`, mirroring dbt's own pipeline.

        dbt evaluates each comma-separated component, expands indirect selection *within* that component,
        and only then intersects — see `NodeSelector.select_nodes_recursively`, where each component goes
        through `get_nodes_from_criteria` before `spec.combined(indirect_sets)`. So the model here is: for
        each term, the nodes it matches plus the tests eager selection attaches to them; intersect those.

        `expand` is False for a `dbt run`/`seed`/`snapshot` task, which builds what it selects and never
        executes a test — verified with `dbt ls` on dbt 1.12.0, where the same selector returns just the
        model under `--resource-type model` while pulling a test in under `test`.

        Deliberately a straightforward pass over `peers` per term. Narrowing each term against an
        index is tempting — this is the hot path for large manifests — but a term like `package:pkg`
        matches nearly everything, so the narrowing has to be joint, and every attempt at that dropped
        real collisions. Correct and slower beats fast and wrong: a missed collision is a task that runs
        another task's resource before its dependencies, which is the whole bug class this guards.
        """
        if not expand:
            return set(cls._matching_ids(select, peers))

        combined: set[str] | None = None
        for term in select.split(','):
            matched = set(cls._matching_ids(term, peers))
            reachable = matched | cls._eagerly_attached(matched, peers)
            combined = reachable if combined is None else combined & reachable
        return combined or set()

    @classmethod
    def _eagerly_attached(cls, selected: set[str], peers: dict) -> set[str]:
        """
        The tests dbt's eager indirect selection adds when `selected` is chosen.

        dbt's rule is "if ANY parent is selected, select the test" — quoted from `expand_selection`'s own
        comment, and confirmed with `dbt ls`: a `relationships` test on `beta` referencing `delta` is
        pulled in by a selector matching only `beta`. An earlier revision required *every* endpoint to be
        selected, which is the `cautious` rule, and so missed multi-endpoint tests entirely.
        """
        if not selected:
            return set()
        if isinstance(peers, _SelectorIndex):
            return peers.tests_depending_on(selected)
        attached: set[str] = set()
        for full_name, info in peers.items():
            if (info.get('resource_type') or '') in cls._TEST_TYPES and cls._has_selected_parent(info, selected):
                attached.add(full_name)
        return attached

    @classmethod
    def _has_selected_parent(cls, test_info: dict, selected: set[str]) -> bool:
        """Whether any testable parent of `test_info` is in `selected` — dbt's eager condition."""
        for dep in test_info.get('depends_on', {}).get('nodes', []):
            if dep in selected and dep.startswith(cls._DBT_TEST_TARGET_PREFIXES):
                return True
        return False

    @staticmethod
    def _base_file_name(original_file_path: str) -> str:
        """The base name dbt's `file:` selector matches — see the module-level `_base_file_name`."""
        return _base_file_name(original_file_path)

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
            raise cls._unaddressable(source_info)
        select = f'source:{package}.{source_name}.{table}'
        if not cls._is_usable_selector(select):
            raise cls._unaddressable(source_info)
        return select

    @classmethod
    def _is_usable_component(cls, value: str) -> bool:
        """Whether `value` can appear inside a selector component and still mean itself."""
        return bool(value) and not cls._SELECTOR_METACHARACTERS & set(value)

    @classmethod
    def _is_usable_selector(cls, value: str) -> bool:
        """
        Whether `value` is safe as a whole raw selector: no graph operator at its boundary, and
        nothing that makes dbt dispatch it to a method other than fqn matching.

        dbt's `RAW_SELECTOR_PATTERN` reads a leading `@` or `N+` and a trailing `+N` as graph
        operators, so `pkg.orders+1` means "pkg.orders and its children one level deep" and selects
        the wrong model. The leading form is spelled `<digits>+`, so `2+orders` means "orders and two
        levels of its parents" — confirmed with `dbt ls` on dbt 1.12.0, where it selects a *different*
        model rather than nothing.

        Only the boundary matters for operators: `pkg.+leading` and `pkg.raw.2+ord` are exact, both
        confirmed with `dbt ls`, so an operator character inside a segment is left alone.

        A trailing `.sql`/`.py`/`.csv` is rejected for a different reason — not the grammar but
        `SelectionCriteria.default_method`, which dispatches such a value to `MethodName.File`. The
        value is then matched as a file name rather than an fqn and resolves to nothing, so the task
        exits 0 having built nothing. Confirmed on dbt 1.12.0 with a model at `models/orders.sql.sql`,
        whose dbt name is `orders.sql`.

        The suffix comparison is case-insensitive because dbt's is: `default_method` tests
        `value.lower().endswith(...)`. A model at `models/orders.SQL.sql` is dispatched to `File` by dbt
        just the same, and a case-sensitive guard here would pass it through to a selector that matches
        nothing — verified with `dbt ls` on dbt 1.12.0.
        """
        return not (
            value.startswith(('@', '+'))
            or value.rstrip('0123456789').endswith('+')
            or value.lstrip('0123456789').startswith('+')
            or value.lower().endswith(cls._FILE_METHOD_SUFFIXES)
        )

    @staticmethod
    def _flat_fqn(fqn: list[str]) -> list[str]:
        """Flattens an fqn as dbt does — see the module-level `_flatten_fqn`."""
        return _flatten_fqn(fqn)

    @classmethod
    def _fqn_term_matches(cls, term: str, node_info: dict) -> bool:
        """
        Whether dbt's fqn selector `term` matches `node_info`.

        Mirrors `QualifiedNameSelectorMethod.node_is_match` on dbt 1.12.0, including the two details
        that make an fqn a *predicate* rather than an identifier:

        * the comparison is a positional walk over the *flattened* fqn, and a selector that runs out of
          parts still matches — so `probe.check` matches `['probe', 'check', 'nested']`, i.e. anything
          nested beneath it, whether the nesting comes from a directory or from a dot in a name;
        * it is retried with the node's own package stripped, so a package's `models/probe/alpha.sql`
          is matched by the root project's `probe.alpha` too.

        Wildcards are not handled: `_is_usable_component` rejects `*?[]` outright, so no term reaching
        here contains one. This mirrors dbt rather than reasoning about it because being *lenient* here
        would mean accepting a selector that matches two nodes, which is the whole bug class; the
        integration suite pins the mirror against `dbt ls`.
        """
        fqn = node_info.get('fqn') or []
        if not fqn:
            return False
        # dbt's `is_versioned` requires the resource type to be a model (`VERSIONED_NODE_TYPES`), not
        # merely that `version` is set — and a unit-test clone *does* carry `version`. Deriving it from
        # the field alone takes dbt's versioned branch for unit tests and so skips its plain
        # `fqn[-1] == term` match, making the mirror stricter than dbt and hiding a real collision on
        # such a node's bare-name selector.
        is_versioned = node_info.get('resource_type') == 'model' and node_info.get('version') is not None
        return cls._is_selected_node(fqn, term, is_versioned) or cls._is_selected_node(fqn[1:], term, is_versioned)

    @classmethod
    def _is_selected_node(cls, fqn: list[str], term: str, is_versioned: bool) -> bool:
        """Mirrors dbt's `is_selected_node` for the operator-free, wildcard-free terms we emit."""
        if not fqn:
            return False
        selector_parts = term.split('.')
        if cls._matches_fqn_leaf(fqn, term, selector_parts, is_versioned):
            return True
        flat_fqn = cls._flat_fqn(fqn)
        if len(flat_fqn) < len(selector_parts):
            return False
        for index, part in enumerate(selector_parts):
            if flat_fqn[index] != part:
                return False
        return True

    @staticmethod
    def _matches_fqn_leaf(fqn: list[str], term: str, selector_parts: list[str], is_versioned: bool) -> bool:
        """
        Whether `term` matches the fqn's leaf, dbt's shortcut before the positional walk.

        A versioned model's fqn ends `[..., <name>, v<N>]`, and dbt lets the last two segments match on
        either the `.` or `_` delimiter, so `orders`, `orders.v1` and `orders_v1` all hit
        `['probe', 'orders', 'v1']`.
        """
        if not is_versioned:
            return fqn[-1] == term
        if len(fqn) < 2:
            return False
        return fqn[-2] == term or '_'.join(fqn[-2:]) == '_'.join(selector_parts[-2:])

    @classmethod
    def _term_matches(cls, term: str, node_info: dict) -> bool:
        """Whether one emitted selector term matches `node_info`."""
        method, _, value = term.partition(':')
        if not _:
            return cls._fqn_term_matches(term, node_info)
        if method == 'package':
            return (node_info.get('package_name') or '') == value
        if method == 'file':
            # dbt's `FileSelectorMethod` matches the base name *or* its stem, so `file:a.yml` also
            # matches a node declared in `a.yml.yml`. Mirroring only the name would let that collision
            # past `_assert_exact` — confirmed with `dbt ls` on dbt 1.12.0, where the `a.yml` task's
            # selector resolves to the `a.yml.yml` test as well.
            base = cls._base_file_name(node_info.get('original_file_path') or '')
            return value in (base, base.rsplit('.', 1)[0] if '.' in base else base)
        if method == 'resource_type':
            return (node_info.get('resource_type') or '') == value
        if method == 'test_name':
            return ((node_info.get('test_metadata') or {}).get('name') or '') == value
        # `source:` selectors are validated by `_source_select`, which needs no manifest context.
        return True  # pragma: no cover - no other method is emitted

    @staticmethod
    def _selector_index(peers: dict) -> dict:
        """Wraps `peers` in the narrowing index used for exactness checks (see `_SelectorIndex`)."""
        return _SelectorIndex(peers)

    @classmethod
    def _matching_ids(cls, select: str, candidates: dict, ignore_resource_type: bool = False) -> list[str]:
        """
        The ids in `candidates` that `select` matches, by intersecting every term as dbt does.

        `ignore_resource_type` answers a different question: which nodes the selector would reach if
        the resource-type filter were not applied. dbt expands indirect selection *before* intersecting
        that filter, so a test selector that also matches a model drags in that model's attached tests
        under the default eager mode — a collision `resource_type:` cannot undo.

        `candidates` should be a `_SelectorIndex` on any real manifest: scanning every node for every
        node is quadratic, and measurably so — 90 seconds for a 6,000-node manifest. The index narrows the
        scan to the nodes sharing this selector's `package:` and `file:`, which is a handful. A plain dict
        still works, for the unit tests and library callers that pass one.
        """
        terms = [term for term in select.split(',') if not (ignore_resource_type and term.startswith('resource_type:'))]
        scan = candidates.narrow(terms) if isinstance(candidates, _SelectorIndex) else candidates
        matched: list[str] = []
        for full_name, info in scan.items():
            if all(cls._term_matches(term, info) for term in terms):
                matched.append(full_name)
        return matched

    @staticmethod
    def _ambiguous(node_info: dict, select: str, also_matched: list[str]) -> ValueError:
        """
        Builds the error raised when a selector is valid but not exact.

        Distinct from `_unaddressable`: nothing about *this* resource's name is wrong, so the remedy is
        about the collision with its neighbours rather than about selector syntax.
        """
        name = node_info.get('name')
        path = node_info.get('original_file_path')
        return ValueError(
            f'Cannot generate a task for {name!r} ({path}): the only selector dbt offers for it '
            f'({select}) also runs {", ".join(sorted(also_matched))}. dbt has no way to address these '
            f'separately, so the task would run the others too — before their own dependencies have '
            f'completed. Rename {name!r} so that its dotted name neither matches nor prefixes a '
            f"sibling's, or move it to a file of its own."
        )

    @staticmethod
    def _unaddressable(node_info: dict) -> ValueError:
        """
        Builds the error raised when no selector can address a node.

        This message is the whole of what a CLI user sees (`main` reports it without a traceback), so
        it leads with the resource and the remedy and keeps the reasoning to one closing line.
        """
        name = node_info.get('name')
        path = node_info.get('original_file_path')
        return ValueError(
            f'Cannot generate a task for {name!r} ({path}): dbt cannot select it uniquely. '
            f'Rename the resource or its file so that it neither starts nor ends with a dbt graph '
            f'operator (a leading @ or N+, a trailing +N), does not end in .sql, .py or .csv, and '
            f'contains none of a space, comma, colon, slash, brace or one of *?[] — or, for a source, '
            f"a dot. Without a usable name or path, the only terms left match a group of resources, so "
            f"the task could run another task's resource."
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
        dbt_unit_tests = self._merge_unit_test_group_deps(
            self._enabled_only(dbt_manifest.get('unit_tests', {})), dbt_nodes
        )

        # Everything a selector could match, for the exactness check in `_node_select`. Sources are
        # included even though no emitted command runs one: a task's `dbt test` uses dbt's default
        # eager indirect selection, so a selector reaching a source pulls in the tests on it.
        peers = _SelectorIndex({**dbt_nodes, **dbt_unit_tests, **dbt_sources})

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
            peers,
        )

        if bundle:
            tasks.extend(
                self._build_bundled_test_tasks(
                    dbt_nodes,
                    dbt_sources,
                    single_model_tested,
                    task_keys,
                    bundled_test_keys,
                    peers,
                )
            )
            tasks.extend(self._build_standalone_test_tasks(standalone_tests, task_keys, peers))
        elif 'test' in self.task_factories:
            tasks.extend(self._build_unit_test_tasks(dbt_unit_tests, dbt_nodes, task_keys, peers))

        return tasks

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

        Version clones are folded into one representative id (see
        `_unit_test_groups`), so a versioned model's unit test yields one task rather
        than one per version running the whole group.
        """
        groups = self._unit_test_groups(dbt_unit_tests, dbt_nodes)
        return [full_name for full_name, members in groups.items() if members[0] == full_name]

    def _unit_test_groups(self, dbt_unit_tests: dict, dbt_nodes: dict) -> dict[str, list[str]]:
        """
        Maps each emittable unit test id to the sorted group of ids that share its task.

        dbt clones a unit test declaration once per model version, rewriting only `unique_id`,
        `depends_on.nodes[0]` and `version` — the fqn, name and file are identical across the clones.
        No selector separates them: `version:` is not a unit-test discriminator at all (dbt 1.12.0
        accepts only `latest`, `prerelease`, `old` and `none` there, none of which match a unit test),
        so emitting one task per clone gives every task the same selector, and each runs every
        version's assertions while claiming to cover one.

        Grouping them into a single task is therefore not a compromise but the accurate description of
        what dbt will run. The task depends on every version's model, so the assertions still run after
        the models they read. Keyed by the fields dbt leaves identical; `members[0]` is the representative,
        stable across manifest orderings because the group is sorted.

        Only unit tests whose target model is present are grouped, and this is the single authority for
        both the emission decision and task building, so the two cannot disagree. Grouping every
        clone and filtering afterwards would let the representative be one whose own model is absent —
        dropping it would then drop the whole group, silently losing an emittable unit test.
        """
        by_identity: dict[tuple, list[str]] = {}
        for full_name, info in dbt_unit_tests.items():
            model_full_name = self._unit_test_model(info)
            if model_full_name is None or model_full_name not in dbt_nodes:
                continue
            identity = (
                tuple(info.get('fqn') or []),
                info.get('name') or '',
                info.get('package_name') or '',
                info.get('original_file_path') or '',
            )
            by_identity.setdefault(identity, []).append(full_name)
        groups: dict[str, list[str]] = {}
        for members in by_identity.values():
            ordered = sorted(members)
            for member in ordered:
                groups[member] = ordered
        return groups

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
        peers: dict,
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
                self._node_select(node_info, peers=peers),
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
        peers: dict,
    ) -> list[DbtTask]:
        """Emits one bundled `<resource>_test` task per tested resource via `TestTaskFactory.create_bundled_task`."""
        test_factory = self.task_factories['test']
        tasks: list[DbtTask] = []
        for full_name in sorted(nodes_with_tests):
            is_source = full_name.startswith('source.')
            info = dbt_sources[full_name] if is_source else dbt_nodes[full_name]
            bare_name = info['name']
            select = self._node_select(info, source_info=info if is_source else None, peers=peers)
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
        peers: dict,
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
                    self._node_select(test_info, peers=peers),
                    test_info['name'],
                    test_info,
                    test_task_key,
                    task_keys,
                )
            )
        return tasks

    def _merge_unit_test_group_deps(self, dbt_unit_tests: dict, dbt_nodes: dict) -> dict:
        """
        Returns `dbt_unit_tests` with each shared-task group's `depends_on` replaced by the group union.

        Version clones share one task (see `_unit_test_groups`), and that task runs every clone's
        assertions, so it must wait for every clone's model. Merging here — before indexing, gating and
        task building all read the manifest — is what keeps those three in agreement.

        Merging later, at task-build time only, silently broke the cycle guard: `_index_tests_by_resource`
        recorded the representative's *unmerged* refs, so `_extend_deps_with_upstream_tests` still judged
        it safe to gate a later model version on the group. With `orders.v2` depending on `orders.v1` plus
        a unit test, that produced `orders_v2_model -> unit_test_..._v1 -> orders_v2_model`, a two-node
        cycle Databricks rejects at deploy. With the union visible up front, the guard sees that the
        group's refs are not all ancestors of `orders.v2` and declines to add the edge.
        """
        groups = self._unit_test_groups(dbt_unit_tests, dbt_nodes)
        merged_view = dict(dbt_unit_tests)
        for representative, members in groups.items():
            if len(members) < 2 or representative != members[0]:
                continue
            info = dbt_unit_tests[representative]
            merged = self._union_of_deps(dbt_unit_tests, members)
            merged_view[representative] = {**info, 'depends_on': {**info.get('depends_on', {}), 'nodes': merged}}
        return merged_view

    @staticmethod
    def _union_of_deps(dbt_unit_tests: dict, members: list[str]) -> list[str]:
        """The union of `depends_on.nodes` across `members`, preserving first-seen order."""
        merged: list[str] = []
        for member in members:
            for dep in dbt_unit_tests[member].get('depends_on', {}).get('nodes', []):
                if dep not in merged:
                    merged.append(dep)
        return merged

    def _build_unit_test_tasks(
        self,
        dbt_unit_tests: dict,
        dbt_nodes: dict,
        task_keys: dict[str, str],
        peers: dict,
    ) -> list[DbtTask]:
        """
        Emits one task per unit test, selected by its full FQN and gated on the model it tests.
        Only unit tests that received a task key (see `_emitted_unit_test_ids`) are emitted; unit
        tests whose target model is absent from the manifest were never keyed and are skipped, so
        their task can't gate on a model task that is never created. Used in per-test mode; in
        bundled mode a model's bundled test task covers its unit tests via `--indirect-selection cautious`.

        A versioned model's unit-test clones share one task, since no selector separates them (see
        `_unit_test_groups`). Its `depends_on` is already the union of the clones' — merged into the
        manifest view by `_merge_unit_test_group_deps` — so it waits for every version's model, which it
        must, since the selector runs all of their assertions.
        """
        test_factory = self.task_factories['test']
        groups = self._unit_test_groups(dbt_unit_tests, dbt_nodes)

        tasks: list[DbtTask] = []
        for unit_test_full_name, unit_test_info in sorted(dbt_unit_tests.items()):
            if unit_test_full_name not in task_keys:
                continue
            members = groups.get(unit_test_full_name, [unit_test_full_name])
            tasks.append(
                test_factory.create_task(
                    self._node_select(unit_test_info, peers=peers, expected_ids=set(members)),
                    unit_test_info['name'],
                    unit_test_info,
                    task_keys[unit_test_full_name],
                    task_keys,
                )
            )
        return tasks


class _SelectorIndex(dict):
    """
    The manifest's selectable resources, plus indexes for narrowing an exactness check.

    `_assert_exact` runs once per node, so evaluating each selector against every node makes generation
    quadratic — measured at 90 seconds for a 6,000-node manifest, and real projects are larger. Each
    index maps a term value to the nodes it *could* match, so a check scans a handful of candidates
    instead of the manifest.

    Every bucket is a sound **superset** of the true matches: `_matching_ids` still evaluates the full
    predicate on whatever comes back, so narrowing can only cost time, never change the answer.
    `test_selector_index_narrowing_matches_a_full_scan` pins that equivalence against a full scan.

    Subclasses `dict` so it *is* the peers mapping: callers that only iterate or look up by id need not
    know the indexes exist.
    """

    def __init__(self, peers: dict):
        super().__init__(peers)
        self._by_package: dict[str, dict] = {}
        self._by_file: dict[str, dict] = {}
        self._by_test_name: dict[str, dict] = {}
        self._by_fqn_key: dict[str, dict] = {}
        self._tests_by_parent: dict[str, set[str]] = {}
        for full_name, info in peers.items():
            self._add(full_name, info)

    def _add(self, full_name: str, info: dict) -> None:
        """Files one node under every key a selector term could reach it by."""
        self._by_package.setdefault(info.get('package_name') or '', {})[full_name] = info
        # `file:` matches the base name *or* its stem, so a node is reachable under both keys.
        base = _base_file_name(info.get('original_file_path') or '')
        for key in dict.fromkeys((base, base.rsplit('.', 1)[0] if '.' in base else base)):
            self._by_file.setdefault(key, {})[full_name] = info
        test_name = (info.get('test_metadata') or {}).get('name') or ''
        if test_name:
            self._by_test_name.setdefault(test_name, {})[full_name] = info
        for key in self._fqn_keys(info):
            self._by_fqn_key.setdefault(key, {})[full_name] = info
        if (info.get('resource_type') or '') in _TEST_TYPES:
            for dep in info.get('depends_on', {}).get('nodes', []):
                if dep.startswith(_DBT_TEST_TARGET_PREFIXES):
                    self._tests_by_parent.setdefault(dep, set()).add(full_name)

    @staticmethod
    def _fqn_keys(info: dict) -> set[str]:
        """
        Every key under which an fqn term could match this node, mirroring `_is_selected_node`.

        dbt can match a term four ways, and each pins one part of the node to one part of the term:

        * the positional walk needs `flat_fqn[0] == term.split('.')[0]`;
        * the same walk after the node's package is stripped needs the *second* segment's first part;
        * the leaf shortcut needs `fqn[-1] == term` — the whole dotted term, so it is a key in itself;
        * a versioned model additionally matches `fqn[-2] == term` and `'_'.join(fqn[-2:])`.
        """
        fqn = info.get('fqn') or []
        if not fqn:
            return set()
        keys = {_flatten_fqn(fqn)[0], fqn[-1]}
        stripped = _flatten_fqn(fqn[1:])
        if stripped:
            keys.add(stripped[0])
        if info.get('resource_type') == 'model' and info.get('version') is not None and len(fqn) >= 2:
            keys.add(fqn[-2])
            keys.add('_'.join(fqn[-2:]))
        return keys

    def tests_depending_on(self, selected: set[str]) -> set[str]:
        """
        The tests with any parent in `selected` — dbt's eager expansion, from a prebuilt reverse index.

        Scanning every peer per term instead made the exactness check quadratic all over again.
        """
        attached: set[str] = set()
        for parent in selected:
            attached |= self._tests_by_parent.get(parent, set())
        return attached

    def narrow(self, terms: list[str]) -> dict:
        """
        The candidate set for `terms` — a superset of the true matches, so the caller still decides.

        Returns the smallest single bucket rather than intersecting across terms: the intersection is
        also sound (terms are ANDed) but measured slower, because building a set from a bucket the size
        of the manifest costs more than the extra predicate evaluations it saves.
        """
        smallest: dict | None = None
        for term in terms:
            method, _, value = term.partition(':')
            if not _:
                bucket = self._fqn_candidates(term)
            elif method == 'package':
                bucket = self._by_package.get(value, {})
            elif method == 'file':
                bucket = self._by_file.get(value, {})
            elif method == 'test_name':
                bucket = self._by_test_name.get(value, {})
            else:
                continue
            if smallest is None or len(bucket) < len(smallest):
                smallest = bucket
        return self if smallest is None else smallest

    def _fqn_candidates(self, term: str) -> dict:
        """
        The nodes an fqn `term` could match: the union over every key it can be looked up under.

        A union is required for soundness — dbt matches on any one of those keys, so dropping a lookup
        could hide a real collision. That makes this bucket only as narrow as its widest lookup, and
        `parts[0]` is a package name, i.e. most of the manifest. So this is usually the *worst* index and
        `narrow` picks whichever is smallest; it earns its keep for a bare-name selector, where the term
        is a single segment and every lookup is precise.
        """
        parts = term.split('.')
        lookups = {parts[0], term}
        if len(parts) >= 2:
            lookups.add('_'.join(parts[-2:]))
        if len(lookups) == 1:
            return self._by_fqn_key.get(term, {})
        candidates: dict = {}
        for key in lookups:
            candidates.update(self._by_fqn_key.get(key, {}))
        return candidates
