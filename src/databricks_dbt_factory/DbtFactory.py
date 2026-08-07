from dataclasses import dataclass, field, replace
from pathlib import PurePosixPath, PureWindowsPath

from databricks_dbt_factory.TaskFactory import TaskFactory
from databricks_dbt_factory.DbtTask import DbtTask
from databricks_dbt_factory.Utils import build_task_key_maps


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


@dataclass
class _Gating:
    """
    What deciding a node's gating test edges needs, in per-test mode.

    Grouped because the three are only ever read together, and only by
    `_extend_deps_with_upstream_tests`: `tests` is the test index, `ancestors` the dbt-graph reachability
    it is judged against, and `version_groups` the versioned-model grouping behind the one exemption to
    the subset rule. `candidates` collects the edges that exemption produces, for
    `_add_safe_gate_candidates` to settle against the finished task graph.
    """

    tests: dict[str, list[tuple[str, frozenset[str]]]] = field(default_factory=dict)
    ancestors: dict[str, set[str]] = field(default_factory=dict)
    version_groups: dict[str, str] = field(default_factory=dict)
    candidates: dict[str, list[str]] = field(default_factory=dict)


def _reaches(graph: dict[str, set[str]], start: str, target: str) -> bool:
    """Whether `target` is reachable from `start` by following `graph`'s dependency edges."""
    seen, stack = {start}, [start]
    while stack:
        for dep in graph.get(stack.pop(), ()):
            if dep == target:
                return True
            if dep not in seen:
                seen.add(dep)
                stack.append(dep)
    return False


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
    _DBT_TEST_TARGET_PREFIXES = _DBT_TEST_TARGET_PREFIXES

    # Characters that still change how an *explicit* `fqn:` selector is interpreted, so a component
    # containing one cannot be used to address a node. Each verified against dbt 1.12.0 with `dbt ls`.
    #
    #   ' '    union separator (`graph/cli.py` splits the raw spec on spaces) — the fragments then match
    #          independently. dbt rejects a space in a resource *name* outright, but a space in a
    #          *directory* reaches the fqn, and `fqn:probe.my dir.orders` selects nothing.
    #   ','    intersection separator — the component is read as two, matching zero nodes
    #   '*?[]' `fnmatch` pattern syntax, still honoured after the `fqn:` prefix: `fqn:probe.a*star`
    #          selects `a*star` *and* `aXstar`, so the glob is live rather than literal
    #
    # `{}` is the one entry that is *not* about dbt at all, and so survives the move to `fqn:`:
    # Databricks substitutes `{{...}}` dynamic references in a task's dbt commands as plain text before
    # the task runs, so a model under `models/{{job.id}}/` emits a selector that resolves locally and
    # matches nothing once substituted — the task then exits 0 having built nothing.
    #
    # Emitting `fqn:` rather than a bare value is what keeps this list short. dbt's
    # `SelectionCriteria.default_method` dispatches a bare value containing `/` to `MethodName.Path` and
    # one ending `.sql`/`.py`/`.csv` to `MethodName.File`, both of which then match nothing; naming the
    # method explicitly bypasses that heuristic entirely. Verified: bare `probe.orders.sql` and
    # `probe.check/slash` select nothing, while `fqn:probe.orders.sql` and `fqn:probe.check/slash` each
    # resolve to exactly their node. `:` and `{}` are likewise literal under `fqn:`.
    _SELECTOR_METACHARACTERS = frozenset(' ,*?[]{}')

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
        # The `fqn:` prefix does *not* neutralise graph operators: `fqn:probe.orders+1` still selects
        # `orders` and its children, verified with `dbt ls` on dbt 1.12.0. So the boundary check applies to
        # the joined value exactly as before — naming the method only bypasses the *dispatch* heuristic.
        if fqn and cls._is_usable_selector('.'.join(fqn)) and all(cls._is_usable_component(part) for part in fqn):
            # `fqn:` names the method explicitly rather than relying on `SelectionCriteria.default_method`
            # to infer it from the value's shape. That inference is what made a `/` or a `.sql` suffix
            # fatal: dbt read the value as a path or a file name and matched nothing while the task still
            # exited 0. With the prefix, `fqn:probe.orders.sql` and `fqn:probe.check/slash` each resolve
            # to exactly their node — verified with `dbt ls` on dbt 1.12.0.
            terms.append(f'fqn:{".".join(fqn)}')
        elif cls._is_usable_component(name := node_info.get('name') or '') and cls._is_usable_selector(name):
            # The fqn is unusable, so fall back to the bare resource name, which dbt matches against
            # the fqn's leaf. It is the only term that tells apart two nodes sharing a package, a
            # file and a test type — two `not_null` tests in one `schema.yml`, say. Not used *with* a
            # usable fqn, whose leaf already carries it.
            terms.append(f'fqn:{name}')
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

        Every task selects its own resource directly and pins the indirect-selection mode, so this is a
        plain intersection of the emitted terms — no model of dbt's eager expansion is needed.

        That is what `--indirect-selection empty` buys. Under dbt's default eager mode the check had to
        mirror dbt's real pipeline (*expand each component, then intersect* — see
        `NodeSelector.select_nodes_recursively`), because a component could reach a model the intersection
        later excluded while the model's attached tests were added *inside that component* and survived on
        their own terms. Pinning `empty` removes the expansion entirely rather than emulating it: verified
        with `dbt ls` on dbt 1.12.0 that `empty` preserves the direct match for generic, singular,
        `relationships` and unit tests alike. Bundled tasks keep `cautious` and are not checked here —
        sweeping a resource's tests is their purpose.

        The contract is **equality**, not "no surplus". A selector that matches nothing is just as wrong as
        one that matches too much: `dbt test` and `dbt run` both exit 0 on a zero-match selector, so the
        task would go green having asserted or built nothing.

        `expected_ids` names additional ids the selector may legitimately run. The node recognises itself
        by object identity *or* by `unique_id`: identity alone would call a node its own collision if a
        caller passed a copy, and `unique_id` alone fails on hand-written fixtures that omit the field.
        """
        allowed = cls._own_ids(node_info, peers) | (expected_ids or set())
        run = set(cls._matching_ids(select, peers))
        surplus = run - allowed
        if surplus:
            raise cls._ambiguous(node_info, select, sorted(surplus))
        missing = allowed - run
        if missing:
            raise cls._selects_nothing(node_info, select, sorted(missing))

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
        Whether `value` can be used as an `fqn:` selector without a graph operator changing its meaning.

        Only the *trailing* `+N` still matters. dbt's `RAW_SELECTOR_PATTERN` reads a trailing `+N` as child
        depth even after an explicit method prefix, so `fqn:probe.orders+1` selects `orders` *and its
        child* — verified with `dbt ls` on dbt 1.12.0.

        The leading forms do not survive the prefix and are therefore no longer rejected: `fqn:@weird` and
        `fqn:2+orders` each resolve to exactly their node, checked in a project where both models have
        children so an operator would have visibly expanded. Refusing them cost the user a working project
        for nothing.

        An operator inside a segment was never a problem: `pkg.+leading` and `pkg.raw.2+ord` are exact,
        both confirmed with `dbt ls`.
        """
        return not value.rstrip('0123456789').endswith('+')

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
            # A manifest missing `fqn` is not something dbt produces, but a hand-rolled or truncated one
            # can be, and the selector then falls back to the bare name. dbt would still match that name
            # against the fqn's leaf, so compare against the name rather than declining outright —
            # otherwise the exactness check reports a selector that reaches nothing.
            return bool(node_info.get('name')) and node_info.get('name') == term
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
        if not _ or method == 'fqn':
            # A bare value is still accepted so a hand-written selector keeps working; everything the
            # factory emits now carries the explicit `fqn:` method.
            return cls._fqn_term_matches(value if method == 'fqn' else term, node_info)
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
    def _selects_nothing(node_info: dict, select: str, missing: list[str]) -> ValueError:
        """
        Builds the error raised when a selector fails to reach what the task is meant to run.

        Distinct from `_ambiguous`, which is the opposite failure. This one is easy to overlook because it
        is silent at run time: `dbt test` and `dbt run` exit 0 on a selector that matches nothing, so the
        task goes green having asserted or built nothing at all.
        """
        name = node_info.get('name')
        path = node_info.get('original_file_path')
        return ValueError(
            f'Cannot generate a task for {name!r} ({path}): the selector dbt offers for it ({select}) '
            f'does not reach {", ".join(missing)}. dbt exits 0 for a selector that matches nothing, so '
            f'the task would report success having run nothing. This is a bug in selector construction '
            f'rather than something to fix in the project — please report it.'
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
            f'Rename the resource or its file so that it does not end with a dbt graph operator (a '
            f'trailing +N) and contains none of a space, comma, brace or one of *?[] — or, for a '
            f"source, a dot. Without a usable name or path, the only terms left match a group of "
            f"resources, so the task could run another task's resource."
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

        gating = _Gating()
        if not bundle and 'test' in self.task_factories:
            gating = _Gating(
                tests=self._index_tests_by_resource(dbt_nodes, dbt_sources, dbt_unit_tests, task_keys),
                ancestors=self._compute_ancestors(dbt_nodes, dbt_sources),
                version_groups=self._version_group(dbt_nodes),
            )

        tasks = self._build_resource_tasks(
            dbt_nodes,
            bundle,
            task_keys,
            bundled_test_keys,
            gating,
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

        # Last, so the graph walked is the complete one: the candidates gate on unit-test tasks, which
        # only exist after the branch above, and their own deps are what close the loops being detected.
        return self._add_safe_gate_candidates(tasks, gating.candidates)

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

    @classmethod
    def _extend_deps_with_upstream_tests(
        cls,
        node_full_name: str,
        existing_deps: list[str] | None,
        gating: _Gating,
    ) -> tuple[list[str], list[str]]:
        """
        Appends the task keys of tests that can safely gate this node.

        A test `T` gates node `N` only when every ref of `T` is an ancestor of `N`. That is what keeps the
        emitted graph acyclic: it forces every gate edge to respect the dbt graph's own topological order,
        so no combination of edges can close a loop.

        The one exception is a test shared by the *versions* of a single model. `_unit_test_groups` gives a
        versioned model's cloned unit test one task depending on every version, so its refs are
        `{orders.v1, orders.v2}` while a `consumer` referencing only v1 has just `{orders.v1}` among its
        ancestors. The plain subset test therefore dropped the edge and a failing v1 assertion stopped
        blocking `consumer`, contrary to the documented gating behaviour. Such an edge is returned as a
        *candidate* rather than added here: no local predicate can establish it is safe.

        The exemption applies only when the test's refs lie entirely within one version group, which is what
        "shared by the versions of a single model" means — see `_covers_one_version_group`. Any other test,
        including an ordinary cross-model `relationships` test that happens to reference a versioned model,
        falls through to the plain subset rule above.

        That is the lesson worth recording. Testing the cycle condition locally — "no ref of `T` is `N` or
        has `N` as an ancestor" — is sound for one edge in isolation but not for a set of them:
        `gating.ancestors` describes the *dbt* graph, while the edges added here are *task* edges, so once
        several gates exist the reachability it consults no longer matches the graph being built. Two
        interlocking `relationships` tests are enough to close a loop, and so are two versioned models
        whose later versions reference each other's earlier one. Both were verified on dbt 1.12.0 and are
        pinned by `test_interlocking_cross_model_tests_do_not_create_a_cycle`,
        `test_interlocking_tests_on_v_prefixed_models_do_not_create_a_cycle` and
        `test_cross_referencing_versioned_models_do_not_create_a_cycle`.

        So the split is by what can actually be proven: edges satisfying the subset rule are returned as
        deps outright, because that rule makes every one of them respect the dbt graph's topological order
        no matter how many are added. Everything else is a candidate that `_add_safe_gate_candidates`
        admits only if it does not close a loop in the *real, assembled* task graph.

        Returns:
            (deps, candidates): the node's dependency list, and the gating test keys whose safety must be
            settled against the finished graph.
        """
        deps: list[str] = list(existing_deps or [])
        seen = set(deps)
        candidates: list[str] = []
        node_ancestors = gating.ancestors.get(node_full_name, set())
        # `sorted` rather than plain set iteration: the append order below decides the order of the
        # emitted `depends_on`, and a set's is `PYTHONHASHSEED`-dependent. The spec is checked in, so
        # that showed up as a spurious diff on every regeneration. Same reasoning as the sorted passes
        # in `build_task_key_maps`: the output should be a function of the node ids alone.
        for ancestor in sorted(node_ancestors):
            for test_key, test_refs in gating.tests.get(ancestor, []):
                if test_key in seen:
                    continue
                unsatisfied = [ref for ref in test_refs if ref not in node_ancestors]
                if not unsatisfied:
                    deps.append(test_key)
                    seen.add(test_key)
                    continue
                if not cls._covers_one_version_group(test_refs, gating.version_groups):
                    continue
                if not all(
                    cls._version_sibling_of_any(ref, node_ancestors, gating.version_groups) for ref in unsatisfied
                ):
                    continue
                seen.add(test_key)
                if node_full_name in test_refs:
                    # The test covers this very node, so it necessarily runs *after* it — `_unit_test_groups`
                    # makes the shared task wait for every version, including this one. There is no gate to
                    # lose here and never was: a test of `orders.v2` cannot also gate `orders.v2`. Skipping
                    # quietly (rather than offering it as a candidate, which would then be refused) is what
                    # keeps the ordinary `orders.v2 = ref(orders.v1)` layout working.
                    continue
                candidates.append(test_key)
        return deps, candidates

    @staticmethod
    def _version_group(dbt_nodes: dict) -> dict[str, str]:
        """
        Maps each versioned model's id to the `<package>.<name>` group its versions share.

        Read from the manifest's `version` field rather than parsed out of the id. The id is
        `model.<pkg>.<name>.v<version>` with the version rendered verbatim, so its final segment cannot be
        told apart from a model *name*: a dotted version gives `model.probe.orders.v1.1` while an ordinary
        model named `vendors` gives `model.probe.vendors`. Both were confirmed on dbt 1.12.0, where
        `version` is `None` for every non-versioned model.
        """
        groups: dict[str, str] = {}
        for full_name, info in dbt_nodes.items():
            if info.get('resource_type') != 'model' or info.get('version') is None:
                continue
            groups[full_name] = f"{info.get('package_name')}.{info.get('name')}"
        return groups

    @staticmethod
    def _covers_one_version_group(test_refs: frozenset[str], version_groups: dict[str, str]) -> bool:
        """
        Whether every resource `test_refs` names is a version of the *same* model.

        This is the precondition for the version-sibling exemption, which exists only for a test shared by
        the versions of one model — the shape `_unit_test_groups` produces. Checking only that the
        *unsatisfied* refs were version siblings let an ordinary cross-model `relationships` test in: with
        refs `{alpha.v2, xm}` it became a candidate and was then refused, citing a cross-referencing
        versioned pair and a failing unit test for a layout that had neither. Such a test now falls
        through to the plain subset rule, which is what handled it before the exemption existed.
        """
        groups = {version_groups.get(ref) for ref in test_refs}
        return len(groups) == 1 and None not in groups

    @staticmethod
    def _version_sibling_of_any(ref: str, ancestors: set[str], version_groups: dict[str, str]) -> bool:
        """
        Whether `ref` is another *version* of a model already among `ancestors`.

        Used only to keep a shared unit-test task gating a node that references one version of the model it
        covers; every other ref must be an ancestor outright.

        Both sides must be versions of the *same* model, decided by `_version_group`. An earlier revision
        compared ids by substring — `'.v' in ref` plus `startswith(f'{stem}.v')` — which made
        `model.pkg.vendors` a "version sibling" of `model.pkg.visits`, since `'.v'` matches inside
        `.vendors`. That handed the exemption to ordinary non-versioned models whose names merely begin
        with `v`, reopening the cycle the subset rule prevents.
        """
        group = version_groups.get(ref)
        if group is None:
            return False
        return any(ancestor != ref and version_groups.get(ancestor) == group for ancestor in ancestors)

    @staticmethod
    def _add_safe_gate_candidates(tasks: list[DbtTask], candidates: dict[str, list[str]]) -> list[DbtTask]:
        """
        Adds the candidate gate edges, refusing generation if any of them would close a loop.

        This is the check that replaces the local proxies: it walks the real `depends_on` graph — every
        task, gate edges included — so it cannot be fooled by the dbt-graph/task-graph mismatch that made
        each per-edge predicate wrong for a *set* of edges. Candidates are considered in sorted order, so
        the outcome is a function of the task keys alone.

        A candidate that closes a loop is a *refusal*, not a dropped edge. Dropping keeps the graph acyclic
        and is tempting — the subset rule would have dropped the same edge — but the edge is a real quality
        gate, and losing it silently means a model builds even though a unit test covering it failed.
        Worse, which model lost its gate depended only on alphabetical order, so renaming a model
        relocated the missing gate. Refusing matches how `_ambiguous` and `_unaddressable` treat a
        selector whose correctness cannot be established: fail at build time, naming the resources and the
        remedy. Only two versioned models whose later versions cross-reference each other's earlier
        version reach this, and `--bundle-tests` represents that layout without the ambiguity.

        Raises:
            ValueError: when a candidate edge cannot be added without creating a cycle.
        """
        if not candidates:
            return tasks
        graph = {task.task_key: set(task.depends_on or ()) for task in tasks}

        added: dict[str, list[str]] = {}
        for task_key in sorted(candidates):
            if task_key not in graph:
                # Cannot happen today: candidates are keyed by the `task_key` of a task in this very
                # list. Skipping rather than raising keeps a future caller that filters tasks after
                # collecting candidates from failing with a `KeyError` in an unrelated place.
                continue
            for test_key in sorted(candidates[task_key]):
                # The edge is `task_key -> test_key`, so it closes a loop exactly when `test_key`
                # already reaches back to `task_key`.
                if _reaches(graph, test_key, task_key):
                    raise DbtFactory._ungateable(task_key, test_key)
                graph[task_key].add(test_key)
                added.setdefault(task_key, []).append(test_key)
        if not added:
            return tasks

        extended: list[DbtTask] = []
        for task in tasks:
            new_deps = added.get(task.task_key)
            extended.append(replace(task, depends_on=[*(task.depends_on or []), *new_deps]) if new_deps else task)
        return extended

    @staticmethod
    def _ungateable(task_key: str, test_key: str) -> ValueError:
        """
        Builds the error raised when a quality gate cannot be added without creating a cycle.

        Like `_ambiguous` and `_unaddressable`, this is the whole of what a CLI user sees, so it leads with
        the resources and the remedy. It names `--bundle-tests` because that mode genuinely represents the
        layout: it gates on a per-resource test task rather than on a unit-test task shared across a
        model's versions, so no such edge arises.

        The stated cause is reachable only because `_covers_one_version_group` confines candidates to tests
        shared across one model's versions. An earlier revision let a cross-model `relationships` test
        reach here, and the message then named a cross-referencing versioned pair the project did not have.
        Keep the two in step: widening what becomes a candidate means widening this wording too.
        """
        return ValueError(
            f'Cannot generate a gate for {task_key!r} on {test_key!r}: the test covers every version of '
            f'its model, so making {task_key!r} wait for it would also make it wait for itself. This '
            f'happens when two versioned models\' later versions reference each other\'s earlier version. '
            f'Run with --bundle-tests, which gates on a per-resource test task and represents this layout '
            f'exactly, or break the cycle by having one model reference the other\'s latest version. '
            f'Emitting the task without the gate would let {task_key!r} build even though a unit test '
            f'covering it had failed.'
        )

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
        gating: _Gating,
        peers: dict,
    ) -> list[DbtTask]:
        """
        Builds tasks for every non-test resource (plus per-test tasks when not bundling).

        Gate edges the subset rule cannot prove safe are recorded in `gating.candidates`, keyed by task
        key, for `_add_safe_gate_candidates` to settle once every task exists.
        """
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
                task = self._gate_task(task, node_full_name, bundle, bundled_test_key_by_task_key, gating)
            tasks.append(task)
        return tasks

    def _gate_task(
        self,
        task: DbtTask,
        node_full_name: str,
        bundle: bool,
        bundled_test_key_by_task_key: dict[str, str],
        gating: _Gating,
    ) -> DbtTask:
        """Applies the gating policy to one gateable task: bundled rewiring, or upstream test edges."""
        if bundle:
            return replace(task, depends_on=self._rewire_deps(task.depends_on, bundled_test_key_by_task_key))
        if not gating.tests:
            return task
        deps, candidates = self._extend_deps_with_upstream_tests(node_full_name, task.depends_on, gating)
        if candidates:
            gating.candidates[task.task_key] = candidates
        return replace(task, depends_on=deps)

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
            # No fqn: the selector falls back to the bare name, which is the only key that can reach this
            # node. Returning nothing here would drop it from every candidate set, so the exactness check
            # would report a selector that reaches nothing.
            name = info.get('name') or ''
            return {name} if name else set()
        keys = {_flatten_fqn(fqn)[0], fqn[-1]}
        stripped = _flatten_fqn(fqn[1:])
        if stripped:
            keys.add(stripped[0])
        if info.get('resource_type') == 'model' and info.get('version') is not None and len(fqn) >= 2:
            keys.add(fqn[-2])
            keys.add('_'.join(fqn[-2:]))
        return keys

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
            if not _ or method == 'fqn':
                bucket = self._fqn_candidates(value if method == 'fqn' else term)
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
