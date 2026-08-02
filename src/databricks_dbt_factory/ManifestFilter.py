"""Filter a dbt manifest down to a subset of nodes using dbt-style selectors.

This is a self-contained (no dbt dependency) implementation covering the common selector
methods that can be resolved from the manifest alone: bare fqn/name, ``tag:``, ``path:``,
and ``fqn:``, each optionally wrapped in the graph operators ``+``/``@`` (ancestors and/or
descendants) using the manifest's ``parent_map``/``child_map``.

Space-separated selectors are unioned, matching ``dbt ls --select "a b"``. It does not
implement the full dbt selector grammar (set intersections with commas, ``method:config``,
``state:``, etc.); those require dbt's own graph resolution and are out of scope for the
manifest-only approach.

After selecting the matching resource nodes, the manifest is rewritten so it stays
internally consistent: only the selected nodes (plus the sources and tests/unit-tests that
reference them) are kept, and every kept node's ``depends_on`` is pruned to the surviving
set. This mirrors dbt, where unselected upstream models simply are not built as part of the
run.
"""

import sys

from databricks_dbt_factory.Utils import SELECTABLE_TYPES, unit_test_model


class ManifestFilter:
    """Applies a dbt-style ``--select`` expression to a parsed manifest."""

    def __init__(self, select: str):
        self._selectors = select.split()
        if not self._selectors:
            raise ValueError(
                f'Empty --select expression {select!r}: expected at least one selector '
                '(e.g. "tag:daily", "+my_model", "path:models/staging").'
            )

    def apply(self, manifest: dict) -> dict:
        """Returns a shallow-rewritten copy of ``manifest`` scoped to the selection.

        The top-level ``nodes``, ``sources`` and ``unit_tests`` maps are replaced with
        filtered versions; all other manifest keys are passed through unchanged (the factory
        only reads those three).
        """
        nodes = manifest.get('nodes', {})
        sources = manifest.get('sources', {})
        unit_tests = manifest.get('unit_tests', {})

        selectable = {uid: info for uid, info in nodes.items() if info.get('resource_type') in SELECTABLE_TYPES}
        selected = self._select_nodes(selectable, manifest)

        if not selected:
            # An empty selection is legal (the job just gets no tasks), but it is far more often
            # a typo'd selector — and if the target spec is the input template (in-place update)
            # it is silently overwritten with an empty task list. Warn loudly so the mistake is
            # visible, without failing the run.
            print(
                f'WARNING: --select {" ".join(self._selectors)!r} matched no model/seed/snapshot '
                'nodes; the generated job will have no tasks.',
                file=sys.stderr,
            )

        # A source survives only if a selected model/seed/snapshot depends on it. This scopes
        # sources (and their tests) to the selection, so a scoped job never emits
        # `<source>_test` tasks for sources belonging to a deselected domain.
        surviving_sources = self._sources_reachable_from(selected, nodes, sources)

        kept_nodes = {}
        for uid, info in nodes.items():
            kept = self._keep_node(uid, info, selected, surviving_sources)
            if kept is not None:
                kept_nodes[uid] = kept

        kept_sources = {uid: info for uid, info in sources.items() if uid in surviving_sources}

        kept_unit_tests = {uid: info for uid, info in unit_tests.items() if unit_test_model(info) in selected}

        filtered = dict(manifest)
        filtered['nodes'] = kept_nodes
        filtered['sources'] = kept_sources
        filtered['unit_tests'] = kept_unit_tests
        return filtered

    def _keep_node(self, uid: str, info: dict, selected: set[str], surviving_sources: set[str]) -> dict | None:
        """Decides whether a manifest node survives the selection, returning the (possibly
        dep-pruned) node to keep or None to drop it.

        - Selectable resources (model/seed/snapshot) are kept iff selected, with deps pruned.
        - Tests are kept only if every resource they reference survived: model/seed/snapshot
          refs must be selected, and source refs must be reachable from the selection. This
          keeps a test from gating on a node that is no longer generated, and drops a
          source-only test whose source belongs to a deselected domain.
        - Any other node type is dropped (the factory does not turn it into a task).
        """
        resource_type = info.get('resource_type')
        if resource_type in SELECTABLE_TYPES:
            return self._prune_deps(info, selected) if uid in selected else None
        if resource_type == 'test':
            return info if self._refs_within(info, selected, surviving_sources) else None
        return None

    @staticmethod
    def _sources_reachable_from(selected: set[str], nodes: dict, sources: dict) -> set[str]:
        """Full names of sources that a selected model/seed/snapshot directly depends on."""
        reachable: set[str] = set()
        for uid in selected:
            info = nodes.get(uid)
            if info is None:
                continue
            for dep in info.get('depends_on', {}).get('nodes', []):
                if dep.startswith('source.') and dep in sources:
                    reachable.add(dep)
        return reachable

    def _select_nodes(self, selectable: dict, manifest: dict) -> set[str]:
        """Resolves the selector expression to the set of matching selectable node ids."""
        selected: set[str] = set()
        for raw in self._selectors:
            selected |= self._select_one(raw, selectable, manifest)
        return selected

    @staticmethod
    def _strip_operators(raw: str) -> tuple[str, bool, bool, bool]:
        """Splits graph operators off a selector.

        Returns ``(spec, want_ancestors, want_descendants, is_at)``; ``is_at`` is True for the
        ``@`` operator, which the caller needs to distinguish from ``+`` on both sides because
        ``@x`` walks ancestors of x *and its descendants*, not just x's ancestors.
        """
        if raw.startswith('@'):
            # `@x` selects x, its descendants, and the ancestors of x and those descendants.
            return raw[1:], True, True, True
        want_ancestors = raw.startswith('+')
        spec = raw[1:] if want_ancestors else raw
        want_descendants = spec.endswith('+')
        spec = spec[:-1] if want_descendants else spec
        return spec, want_ancestors, want_descendants, False

    def _select_one(self, raw: str, selectable: dict, manifest: dict) -> set[str]:
        spec, want_ancestors, want_descendants, is_at = self._strip_operators(raw)

        matched = {uid for uid, info in selectable.items() if self._matches(spec, info)}

        result = set(matched)
        descendants: set[str] = set()
        if want_descendants:
            descendants = self._walk(matched, manifest.get('child_map', {}), selectable)
            result |= descendants
        if want_ancestors:
            # `@x` walks ancestors of x *and its descendants* (dbt semantics); plain `+x`
            # walks only x's ancestors.
            ancestor_seeds = matched | descendants if is_at else matched
            result |= self._walk(ancestor_seeds, manifest.get('parent_map', {}), selectable)
        return result

    @staticmethod
    def _matches(spec: str, info: dict) -> bool:
        """Whether a single (operator-stripped) selector matches a node."""
        if spec.startswith('tag:'):
            tag = spec[len('tag:') :]
            # `or []`/`or {}` guard against a manifest node serialized with `"tags": null` or
            # `"config": null`, which `DbtFactory` tolerates the same way.
            tags = info.get('tags') or []
            config_tags = (info.get('config') or {}).get('tags') or []
            return tag in tags or tag in config_tags
        if spec.startswith('path:'):
            path = spec[len('path:') :]
            node_path = info.get('original_file_path') or info.get('path') or ''
            return node_path == path or node_path.startswith(path.rstrip('/') + '/')
        if spec.startswith('fqn:'):
            spec = spec[len('fqn:') :]
        # Bare selector: match the node name, its full dotted fqn, or an fqn path prefix.
        fqn = info.get('fqn', [])
        if spec == info.get('name'):
            return True
        dotted = '.'.join(fqn)
        return spec == dotted or dotted.startswith(spec + '.')

    @staticmethod
    def _walk(seeds: set[str], adjacency: dict, selectable: dict) -> set[str]:
        """Transitively walks an adjacency map from seeds, keeping only selectable nodes."""
        reached: set[str] = set()
        stack = [n for seed in seeds for n in adjacency.get(seed, [])]
        while stack:
            current = stack.pop()
            if current in reached:
                continue
            reached.add(current)
            stack.extend(adjacency.get(current, []))
        return {uid for uid in reached if uid in selectable}

    @staticmethod
    def _prune_deps(info: dict, selected: set[str]) -> dict:
        """Returns a copy of a node with its ``depends_on.nodes`` pruned to the selected set.

        Non-selectable deps (e.g. sources, which the factory keys off separately) are left in
        place; only references to dropped selectable resources are removed so no task gates on
        a node that is no longer generated.
        """
        deps = info.get('depends_on', {}).get('nodes')
        if not deps:
            return info
        selectable_prefixes = tuple(t + '.' for t in SELECTABLE_TYPES)
        pruned = []
        for dep in deps:
            if not dep.startswith(selectable_prefixes) or dep in selected:
                pruned.append(dep)
        if pruned == deps:
            return info
        new_info = dict(info)
        new_info['depends_on'] = dict(info.get('depends_on', {}))
        new_info['depends_on']['nodes'] = pruned
        return new_info

    @staticmethod
    def _refs_within(test_info: dict, selected: set[str], surviving_sources: set[str]) -> bool:
        """Whether a test stays attached to the selection: it references at least one surviving
        resource and every resource it references survived.

        A model/seed/snapshot ref must be in ``selected``; a source ref must be in
        ``surviving_sources`` (i.e. reachable from a selected node). A test with a ref to a
        deselected node — of either kind — is dropped so it never gates on, or drags in, a
        resource that is no longer part of the scoped job. A test with no surviving ref at all
        (including a zero-ref singular/custom test) is also dropped: it is not connected to the
        selected subgraph, so a scoped job must not emit it — mirroring dbt, which does not run
        tests unconnected to the selection.
        """
        selectable_prefixes = tuple(t + '.' for t in SELECTABLE_TYPES)
        has_surviving_ref = False
        for dep in test_info.get('depends_on', {}).get('nodes', []):
            if dep.startswith(selectable_prefixes):
                if dep not in selected:
                    return False
                has_surviving_ref = True
            elif dep.startswith('source.'):
                if dep not in surviving_sources:
                    return False
                has_surviving_ref = True
        return has_surviving_ref
