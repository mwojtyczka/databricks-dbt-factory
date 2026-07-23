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
internally consistent: only the selected nodes (plus the tests/unit-tests that reference
them) are kept, and every kept node's ``depends_on`` is pruned to the surviving set. This
mirrors dbt, where unselected upstream models simply are not built as part of the run.
"""

# Resource types the factory turns into resource tasks and that a selector targets directly.
_SELECTABLE_TYPES = frozenset({'model', 'seed', 'snapshot'})


class ManifestFilter:
    """Applies a dbt-style ``--select`` expression to a parsed manifest."""

    def __init__(self, select: str):
        self._selectors = select.split()

    def apply(self, manifest: dict) -> dict:
        """Returns a shallow-rewritten copy of ``manifest`` scoped to the selection.

        The top-level ``nodes``, ``sources`` and ``unit_tests`` maps are replaced with
        filtered versions; all other manifest keys are passed through unchanged (the factory
        only reads those three).
        """
        nodes = manifest.get('nodes', {})
        unit_tests = manifest.get('unit_tests', {})

        selectable = {uid: info for uid, info in nodes.items() if info.get('resource_type') in _SELECTABLE_TYPES}
        selected = self._select_nodes(selectable, manifest)

        kept_nodes = {}
        for uid, info in nodes.items():
            kept = self._keep_node(uid, info, selected)
            if kept is not None:
                kept_nodes[uid] = kept

        kept_unit_tests = {uid: info for uid, info in unit_tests.items() if self._unit_test_model(info) in selected}

        filtered = dict(manifest)
        filtered['nodes'] = kept_nodes
        filtered['unit_tests'] = kept_unit_tests
        return filtered

    def _keep_node(self, uid: str, info: dict, selected: set[str]) -> dict | None:
        """Decides whether a manifest node survives the selection, returning the (possibly
        dep-pruned) node to keep or None to drop it.

        - Selectable resources (model/seed/snapshot) are kept iff selected, with deps pruned.
        - Tests are kept only if every resource they reference survived, so a test never gates
          on a node that is no longer generated.
        - Any other node type is dropped (the factory does not turn it into a task).
        """
        resource_type = info.get('resource_type')
        if resource_type in _SELECTABLE_TYPES:
            return self._prune_deps(info, selected) if uid in selected else None
        if resource_type == 'test':
            return info if self._refs_within(info, selected) else None
        return None

    def _select_nodes(self, selectable: dict, manifest: dict) -> set[str]:
        """Resolves the selector expression to the set of matching selectable node ids."""
        selected: set[str] = set()
        for raw in self._selectors:
            selected |= self._select_one(raw, selectable, manifest)
        return selected

    @staticmethod
    def _strip_operators(raw: str) -> tuple[str, bool, bool]:
        """Splits graph operators off a selector, returning (spec, want_ancestors, want_descendants)."""
        if raw.startswith('@'):
            # `@x` selects x, its descendants, and the ancestors of those descendants.
            return raw[1:], True, True
        want_ancestors = raw.startswith('+')
        spec = raw[1:] if want_ancestors else raw
        want_descendants = spec.endswith('+')
        spec = spec[:-1] if want_descendants else spec
        return spec, want_ancestors, want_descendants

    def _select_one(self, raw: str, selectable: dict, manifest: dict) -> set[str]:
        spec, want_ancestors, want_descendants = self._strip_operators(raw)

        matched = {uid for uid, info in selectable.items() if self._matches(spec, info)}

        result = set(matched)
        if want_ancestors:
            result |= self._walk(matched, manifest.get('parent_map', {}), selectable)
        if want_descendants:
            result |= self._walk(matched, manifest.get('child_map', {}), selectable)
        return result

    @staticmethod
    def _matches(spec: str, info: dict) -> bool:
        """Whether a single (operator-stripped) selector matches a node."""
        if spec.startswith('tag:'):
            tag = spec[len('tag:') :]
            return tag in info.get('tags', []) or tag in info.get('config', {}).get('tags', [])
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
        selectable_prefixes = tuple(t + '.' for t in _SELECTABLE_TYPES)
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
    def _refs_within(test_info: dict, selected: set[str]) -> bool:
        """Whether every selectable resource a test references is in the selected set."""
        for dep in test_info.get('depends_on', {}).get('nodes', []):
            if dep.startswith(tuple(t + '.' for t in _SELECTABLE_TYPES)) and dep not in selected:
                return False
        return True

    @staticmethod
    def _unit_test_model(unit_test_info: dict) -> str | None:
        model = unit_test_info.get('model')
        package = unit_test_info.get('package_name')
        if model and package:
            return f"model.{package}.{model}"
        return None
