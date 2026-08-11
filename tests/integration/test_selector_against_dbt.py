"""Prove generated `--select` arguments against a real dbt, not against our model of it.

Every selector bug found in this project so far survived a reading of dbt's source and was only
caught by running dbt — see AGENTS.md. Unit tests assert the selector *string*, which cannot catch a
wrong belief about what dbt does with it. These tests close that gap: they write dbt projects, have
dbt parse them, run the factory over the real manifest, and feed every emitted selector back through
`dbt ls`, asserting the exact set of unique IDs it resolves to.

The layouts below are the ones that have actually broken this code: a model directory named after a
sibling model, a dotted name flattening onto another node's fqn, a root-level ancestor, an installed
package (whose `original_file_path` is package-relative), a duplicated base name across packages, and
a shared `schema.yml`. `test_generated_selectors_are_exact` is the generative case: it builds
randomised layouts from those same primitives, so a shape nobody thought to enumerate still gets
checked.

dbt is a declared test dependency (see `pyproject.toml`), so it is always available here — these
tests never skip. A skip would defeat the point: it would let a selector regression through on a
machine where dbt happened to be missing.
"""

# pylint: disable=too-many-lines

import functools
import json
import random
import re
import shlex
from pathlib import Path

import pytest
from dbt.cli.main import dbtRunner, dbtRunnerResult

from databricks_dbt_factory.DbtFactory import DbtFactory
from databricks_dbt_factory.Utils import build_task_key_maps
from tests.conftest import create_dbt_factory

PROFILES = """\
probe:
  target: dev
  outputs:
    dev:
      type: databricks
      host: example.databricks.com
      http_path: /sql/1.0/warehouses/x
      token: dummy
      schema: default
"""

MODEL_SQL = 'select 1 as id\n'


def _write_project(
    root: Path,
    model_paths: dict[str, str],
    schema_yml: str | None = None,
    package_model_paths: dict[str, str] | None = None,
) -> None:
    """
    Writes a minimal dbt project.

    `model_paths` maps a path under `models/` to its SQL. `package_model_paths` does the same for an
    installed local package named `other`, whose nodes carry package-relative `original_file_path`s
    and are matched by dbt on their package-stripped fqn — behaviour no root-only layout can exercise.
    """
    project = 'name: probe\nprofile: probe\nversion: "1.0"\nconfig-version: 2\nmodel-paths: ["models"]\n'
    if package_model_paths:
        # A distinct schema keeps the two packages' relations from colliding, which dbt rejects.
        project += 'models:\n  other:\n    +schema: otherschema\n'
        (root / 'packages.yml').write_text('packages:\n  - local: libs/other\n', encoding='utf-8')
        package_root = root / 'libs' / 'other'
        package_root.mkdir(parents=True, exist_ok=True)
        (package_root / 'dbt_project.yml').write_text(
            'name: other\nversion: "1.0"\nconfig-version: 2\nmodel-paths: ["models"]\n', encoding='utf-8'
        )
        for relative_path, sql in package_model_paths.items():
            target = package_root / 'models' / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(sql, encoding='utf-8')
    (root / 'dbt_project.yml').write_text(project, encoding='utf-8')
    (root / 'profiles.yml').write_text(PROFILES, encoding='utf-8')
    for relative_path, sql in model_paths.items():
        target = root / 'models' / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(sql, encoding='utf-8')
    if schema_yml is not None:
        (root / 'models' / 'schema.yml').write_text(schema_yml, encoding='utf-8')
    if package_model_paths:
        result = _dbt(root, 'deps', '--quiet')
        assert result.success, f'dbt deps failed for the generated project: {result.exception}'


def _dbt(root: Path, *args: str) -> dbtRunnerResult:
    """
    Invokes dbt against the project at `root`.

    Passes the project and profile directories explicitly rather than changing the working
    directory: `os.chdir` in a test leaks into everything else in the process, and under
    `pytest-cov` it makes coverage write its data file to the wrong place.
    """
    return dbtRunner().invoke([*args, '--project-dir', str(root), '--profiles-dir', str(root)])


def _parse(root: Path) -> dict:
    """Parses the project with dbt and returns its real manifest, failing loudly if dbt cannot."""
    result = _dbt(root, 'parse', '--quiet')
    assert result.success, f'dbt could not parse the generated project: {result.exception}'
    return json.loads((root / 'target' / 'manifest.json').read_text(encoding='utf-8'))


@functools.lru_cache(maxsize=None)
def _selected_ids(
    root: Path, select: str, resource_type: str | None, indirect: bool = False, indirect_selection: str | None = None
) -> tuple[str, ...]:
    """
    Returns the unique IDs dbt resolves `select` to, as `dbt ls` reports them.

    Memoised: a `dbt ls` invocation costs a few hundred milliseconds, and the same selector recurs
    across per-test and bundled mode, so caching keeps the suite inside the project's test timeout.
    """
    args = ['ls', '--quiet', '--select', select]
    if resource_type:
        args += ['--resource-type', resource_type]
    if indirect:
        args += ['--indirect-selection', 'cautious']
    elif indirect_selection:
        args += ['--indirect-selection', indirect_selection]
    result = _dbt(root, *args)
    assert result.success, f'dbt ls failed for {select!r}: {result.exception}'
    # `dbt ls` returns a list of unique-id strings; the runner's result type is a union across every
    # dbt command, so narrow it here rather than trusting the annotation.
    assert isinstance(result.result, list), f'expected dbt ls to return a list, got {type(result.result)}'
    return tuple(sorted(str(unique_id) for unique_id in result.result))


@functools.lru_cache(maxsize=None)
def _selected_unique_ids(
    root: Path, select: str, resource_type: str | None, indirect_selection: str | None = None
) -> tuple[str, ...]:
    """
    The *unique ids* dbt resolves `select` to, rather than the display names `_selected_ids` returns.

    `dbt ls` prints selector names by default (`probe.orders`), which do not match manifest keys — so a
    comparison against manifest ids has to ask for `--output json --output-keys unique_id` instead.
    """
    args = ['ls', '--quiet', '--select', select, '--output', 'json', '--output-keys', 'unique_id']
    if resource_type:
        args += ['--resource-type', resource_type]
    if indirect_selection:
        args += ['--indirect-selection', indirect_selection]
    result = _dbt(root, *args)
    assert result.success, f'dbt ls failed for {select!r}: {result.exception}'
    assert isinstance(result.result, list), f'expected dbt ls to return a list, got {type(result.result)}'
    return tuple(sorted(json.loads(entry)['unique_id'] for entry in result.result))


def _resource_selectors(manifest: dict, bundle_tests: bool) -> list[tuple[str, str, str]]:
    """
    Runs the factory over `manifest` and returns `(task_key, select, verb)` for every task that
    builds a resource — the tasks that must each touch exactly one node.
    """
    selectors = []
    for task in create_dbt_factory(bundle_tests=bundle_tests).create_tasks(manifest):
        # `shlex.split` rather than `str.split`: the command is a shell string with the selector
        # quoted, and this is exactly how the notebook runner recovers the original argv value — so
        # tokenising the same way also proves that round-trip.
        command = shlex.split(task['dbt_task']['commands'][-1])
        select = command[command.index('--select') + 1]
        selectors.append((task['task_key'], select, command[1]))
    return selectors


def _task_key_to_unique_id(manifest: dict, bundle_tests: bool) -> dict[str, str]:
    """
    Maps each resource task's key back to the manifest id it is supposed to address.

    Built by re-deriving the task key from each id the same way the factory does, rather than by
    parsing the selector. Bundled task keys map to their parent resource here; their exact member sets
    are checked separately by `_bundled_test_ids_by_task_key`.
    """
    nodes = _enabled_entries(manifest.get('nodes', {}))
    unit_tests = _enabled_entries(manifest.get('unit_tests', {}))
    bundle_members = _bundled_test_membership(manifest) if bundle_tests else {}
    bundled_test_ids = {test_id for test_ids in bundle_members.values() for test_id in test_ids}

    task_ids = []
    for full_name, info in nodes.items():
        if info.get('resource_type') in {'model', 'seed', 'snapshot', 'test'} and (
            not bundle_tests or full_name not in bundled_test_ids
        ):
            task_ids.append(full_name)
    if not bundle_tests:
        task_ids.extend(
            full_name
            for full_name, info in unit_tests.items()
            if (model_id := _unit_test_model_id(info)) is not None and model_id in nodes
        )

    task_keys, bundled_test_keys = build_task_key_maps(sorted(task_ids), sorted(bundle_members))
    mapping = {key: full_name for full_name, key in task_keys.items()}
    if bundle_tests:
        mapping.update({key: full_name for full_name, key in bundled_test_keys.items()})
    return mapping


def _enabled_entries(entries: dict) -> dict:
    """Returns the manifest entries dbt can select."""
    return {
        full_name: info for full_name, info in entries.items() if (info.get('config') or {}).get('enabled') is not False
    }


def _unit_test_model_id(unit_test_info: dict) -> str | None:
    """Returns the model id dbt resolved for a unit test."""
    for dep in unit_test_info.get('depends_on', {}).get('nodes', []):
        if dep.startswith('model.'):
            return dep
    model = unit_test_info.get('model')
    package = unit_test_info.get('package_name')
    return f'model.{package}.{model}' if model and package else None


def _bundled_test_membership(manifest: dict) -> dict[str, set[str]]:
    """Groups enabled single-resource data and unit test ids by their exact bundle resource."""
    nodes = _enabled_entries(manifest.get('nodes', {}))
    sources = _enabled_entries(manifest.get('sources', {}))
    unit_tests = _enabled_entries(manifest.get('unit_tests', {}))
    resources = nodes.keys() | sources.keys()
    membership: dict[str, set[str]] = {}

    for test_id, info in nodes.items():
        if info.get('resource_type') != 'test':
            continue
        parents = {
            dep
            for dep in info.get('depends_on', {}).get('nodes', [])
            if dep.startswith(('model.', 'seed.', 'snapshot.', 'source.')) and dep in resources
        }
        if len(parents) == 1:
            membership.setdefault(next(iter(parents)), set()).add(test_id)

    for test_id, info in unit_tests.items():
        model_id = _unit_test_model_id(info)
        if model_id is not None and model_id in nodes:
            membership.setdefault(model_id, set()).add(test_id)
    return membership


def _bundled_test_ids_by_task_key(manifest: dict) -> dict[str, set[str]]:
    """Maps each emitted bundled task key to the exact test and unit-test ids it must run."""
    membership = _bundled_test_membership(manifest)
    if not membership:
        return {}
    expected_by_key = _task_key_to_unique_id(manifest, bundle_tests=True)
    return {
        task_key: membership[resource_id]
        for task_key, resource_id in expected_by_key.items()
        if resource_id in membership and task_key.endswith('_test')
    }


def _selected_by_bundled_commands(tmp_path: Path, task: dict) -> set[str]:
    """Replays every test command in a bundle with its final indirect-selection mode."""
    selected: set[str] = set()
    test_commands = [command for command in task['dbt_task']['commands'] if command.startswith('dbt test ')]
    assert test_commands, f'{task["task_key"]} has no dbt test command'
    for raw_command in test_commands:
        command = shlex.split(raw_command)
        select = command[command.index('--select') + 1]
        modes = [command[index + 1] for index, value in enumerate(command[:-1]) if value == '--indirect-selection']
        assert modes, f'{task["task_key"]} does not pin indirect selection: {raw_command}'
        selected.update(_selected_unique_ids(tmp_path, select, None, indirect_selection=modes[-1]))
    return selected


def _assert_each_task_selects_its_own_node(tmp_path: Path, manifest: dict, bundle_tests: bool) -> None:
    """
    Asserts every resource task resolves to its exact manifest node or bundled member set.

    Each test task is replayed with the indirect-selection mode in its emitted command, so both direct
    `empty` plans and parent-scoped `cautious` plans must resolve to their intended manifest ids.
    """
    expected_by_key = _task_key_to_unique_id(manifest, bundle_tests)
    bundled_by_key = _bundled_test_ids_by_task_key(manifest) if bundle_tests else {}
    unmapped = []
    selectable = {**manifest.get('nodes', {}), **manifest.get('unit_tests', {}), **manifest.get('sources', {})}
    for task in create_dbt_factory(bundle_tests=bundle_tests).create_tasks(manifest):
        task_key = task['task_key']
        if task_key in bundled_by_key:
            bundle_selection = _selected_by_bundled_commands(tmp_path, task)
            expected = bundled_by_key[task_key]
            assert (
                bundle_selection == expected
            ), f'{task_key} runs {sorted(bundle_selection)}, expected exact bundle membership {sorted(expected)}'
            continue

        unique_id = expected_by_key.get(task_key)
        if unique_id is None:
            unmapped.append(task_key)
            continue
        command = shlex.split(task['dbt_task']['commands'][-1])
        select = command[command.index('--select') + 1]
        # Compare against the *manifest unique id*, not the name `dbt ls` displays. Two generic tests can
        # share a display name while separate files keep each selector individually addressable, so a
        # name-based assertion would be satisfied by pointing task A at task B's node.
        resource_type = selectable[unique_id]['resource_type']
        indirect_selection = 'empty'
        if resource_type in {'test', 'unit_test'}:
            modes = [command[index + 1] for index, value in enumerate(command[:-1]) if value == '--indirect-selection']
            indirect_selection = modes[-1]
        node_selection = _selected_unique_ids(tmp_path, select, None, indirect_selection=indirect_selection)
        assert node_selection == (unique_id,), (
            f'{task_key} selects {node_selection} via {select!r}, expected exactly ({unique_id!r},) — '
            f'the node it is named for'
        )
    assert not unmapped, f'no expected node known for {unmapped}; the key mapping has drifted'


# Layouts that have actually broken selector generation in this project.
REGRESSION_LAYOUTS = {
    'sibling-named-directory': {
        'marts/orders.sql': MODEL_SQL,
        'marts/orders/items.sql': MODEL_SQL,
    },
    'root-level-ancestor': {
        'orders.sql': MODEL_SQL,
        'orders/items.sql': MODEL_SQL,
    },
    'dotted-name-collides': {
        'marts/orders.sql': MODEL_SQL,
        'marts/orders.items.sql': MODEL_SQL,
    },
    'dotted-name-equals-nested-fqn': {
        'marts/orders.items.sql': MODEL_SQL,
        'marts/orders/items.sql': MODEL_SQL,
    },
    'deeply-nested': {
        'a.sql': MODEL_SQL,
        'a/b.sql': MODEL_SQL,
        'a/b/c.sql': MODEL_SQL,
    },
}


@pytest.mark.parametrize('layout_name', sorted(REGRESSION_LAYOUTS))
@pytest.mark.parametrize('bundle_tests', [False, True], ids=['per-test', 'bundled'])
def test_regression_layouts_select_exactly_one_node(tmp_path, layout_name, bundle_tests):
    """Each resource task must build exactly the node it is named for, in both modes."""
    _write_project(tmp_path, REGRESSION_LAYOUTS[layout_name])
    manifest = _parse(tmp_path)

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests)


def test_bundled_test_task_unions_only_its_own_resources_tests(tmp_path):
    """
    A bundled `<model>_test` task unions its own test-node selectors and none of the nested sibling's.
    """
    _write_project(
        tmp_path,
        {'marts/orders.sql': MODEL_SQL, 'marts/orders/items.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: orders\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests: [unique, not_null]\n'
            '  - name: items\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests: [unique]\n'
        ),
    )
    manifest = _parse(tmp_path)

    selectors = {
        key: select for key, select, verb in _resource_selectors(manifest, bundle_tests=True) if verb == 'test'
    }
    orders_select = next(select for key, select in selectors.items() if key == 'orders_test')
    selected = _selected_ids(tmp_path, orders_select, resource_type=None, indirect_selection='empty')

    # A schema test's fqn is [package, <test name>] — the models/ subdirectory is not part of it.
    assert 'probe.unique_items_id' not in selected, f'orders_test included the sibling model tests: {selected}'
    assert 'probe.unique_orders_id' in selected
    assert 'probe.not_null_orders_id' in selected
    assert 'probe.marts.orders.items' not in selected, f'orders_test selected the sibling model: {selected}'


def test_tests_sharing_a_schema_file_are_separated_by_test_name(tmp_path):
    """
    Every test in one `schema.yml` shares that path, so `file:` cannot single one out. `test_name:`
    narrows to the generic test type, which separates a `not_null` from a `unique` in the same file.
    """
    _write_project(
        tmp_path,
        {'my tests/a.sql': MODEL_SQL, 'my tests/b.sql': MODEL_SQL},
    )
    (tmp_path / 'models' / 'my tests' / 'schema.yml').write_text(
        'models:\n'
        '  - name: a\n'
        '    columns:\n'
        '      - name: id\n'
        '        data_tests: [not_null]\n'
        '  - name: b\n'
        '    columns:\n'
        '      - name: id\n'
        '        data_tests: [unique]\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    selectors = {key: select for key, select, verb in _resource_selectors(manifest, bundle_tests=False)}
    for task_key, select in selectors.items():
        if not task_key.endswith('_test'):
            continue
        selected = _selected_ids(tmp_path, select, 'test', indirect=False)
        assert len(selected) == 1, f'{task_key} selects {selected} via {select!r}, expected exactly one test'


def test_package_node_matched_by_package_stripped_fqn_is_still_exact(tmp_path):
    """
    dbt compares a node's fqn with its package stripped, so a package model at
    `models/probe/alpha.sql` in package `other` (fqn [other, probe, alpha]) is also matched by the
    root project's `probe.alpha` selector. `package:` is the only term that separates them, since both
    files are named `alpha.sql`. No root-only layout can reach this, which is why it went unnoticed.
    """
    _write_project(
        tmp_path,
        {'alpha.sql': MODEL_SQL},
        package_model_paths={'probe/alpha.sql': MODEL_SQL, 'probe/alpha/nested.sql': MODEL_SQL},
    )
    manifest = _parse(tmp_path)

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


@pytest.mark.parametrize(
    ('file_name', 'note'),
    [
        pytest.param('+leading.sql', 'an operator inside a segment is harmless', id='embedded-operator'),
        pytest.param("customer's.sql", 'a quote breaks shlex in the notebook runner', id='apostrophe'),
    ],
)
def test_awkward_file_names_still_resolve_to_one_node(tmp_path, file_name, note):
    """
    Names that trip dbt's selector grammar or the notebook runner's tokenisation must still address
    exactly one node — by dropping only the unusable term. (`note` records what each name trips.)

    A name dbt reads as a graph operator at the *boundary* (`orders+1`) is not here: it leaves nothing
    that pins the node, so it is refused. See
    `test_a_file_name_alone_is_not_enough_to_address_a_resource`.
    """
    assert note
    _write_project(tmp_path, {file_name: MODEL_SQL, 'orders.sql': MODEL_SQL})
    manifest = _parse(tmp_path)

    for task_key, select, verb in _resource_selectors(manifest, bundle_tests=False):
        if verb != 'run':
            continue
        selected = _selected_ids(tmp_path, select, 'model', indirect=False)
        assert len(selected) == 1, f'{task_key} selects {selected} via {select!r}, expected exactly one node'


def test_singular_test_sharing_a_models_fqn_is_addressable_under_empty(tmp_path):
    """
    `models/beta.sql` and `tests/beta.sql` parse with the same fqn and base name, and the model carries a
    test of its own.

    Under dbt's default eager mode this had to be refused: the test task's selector also reached the
    model, and eager selection then added the model's attached `not_null`, so the task asserted on `beta`
    before `beta_model` had built it. Pinning `--indirect-selection empty` removes that — verified on dbt
    1.12.0, where the same selector returns two tests under eager and one under `empty` — so the layout
    generates.
    """
    _write_project(
        tmp_path,
        {'beta.sql': MODEL_SQL, 'gamma.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: beta\n    columns:\n      - name: id\n        data_tests: [not_null]\n'
            '  - name: gamma\n    columns:\n      - name: id\n        data_tests: [not_null]\n'
        ),
    )
    tests_dir = tmp_path / 'tests'
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / 'beta.sql').write_text("select * from {{ ref('gamma') }} where id is null\n", encoding='utf-8')
    manifest = _parse(tmp_path)

    leaky = 'fqn:probe.beta,package:probe,file:beta.sql,resource_type:test'
    assert len(_selected_unique_ids(tmp_path, leaky, None)) == 2, 'eager no longer leaks; revisit this test'
    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_bundled_selector_runs_only_tests_attached_to_its_resource(tmp_path: Path) -> None:
    _write_project(
        tmp_path,
        {'beta.sql': MODEL_SQL, 'gamma.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: beta\n    columns:\n      - name: id\n        data_tests: [not_null]\n'
            '  - name: gamma\n    columns:\n      - name: id\n        data_tests: [not_null]\n'
        ),
    )
    tests_dir = tmp_path / 'tests'
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / 'beta.sql').write_text("select * from {{ ref('gamma') }} where id is null\n", encoding='utf-8')
    manifest = _parse(tmp_path)

    expected_by_parent: dict[str, set[str]] = {}
    for unique_id, info in manifest['nodes'].items():
        if info['resource_type'] != 'test':
            continue
        model_parents = {dep for dep in info['depends_on']['nodes'] if dep.startswith('model.')}
        if len(model_parents) == 1:
            expected_by_parent.setdefault(next(iter(model_parents)), set()).add(unique_id)

    tasks = {task['task_key']: task for task in create_dbt_factory(bundle_tests=True).create_tasks(manifest)}
    for model_name in ('beta', 'gamma'):
        commands = []
        for raw_command in tasks[f'{model_name}_test']['dbt_task']['commands']:
            if raw_command.startswith('dbt test '):
                commands.append(shlex.split(raw_command))
        selected: set[str] = set()
        for command in commands:
            select = command[command.index('--select') + 1]
            assert select.split() == sorted(select.split()), f'non-deterministic bundled union: {select}'
            result = _dbt(
                tmp_path,
                'ls',
                '--quiet',
                *command[2:],
                '--resource-type',
                'test',
                '--output',
                'json',
                '--output-keys',
                'unique_id',
            )
            assert result.success, f'dbt ls failed for bundled {model_name!r} selector: {result.exception}'
            assert isinstance(result.result, list)
            selected.update(json.loads(entry)['unique_id'] for entry in result.result)
        assert selected == expected_by_parent[f'model.probe.{model_name}']


def test_bundled_selector_unions_are_exact_in_empty_and_cautious_modes(tmp_path: Path) -> None:
    _write_project(
        tmp_path,
        {'alpha.sql': MODEL_SQL, 'beta.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: alpha\n    columns:\n      - name: id\n        data_tests:\n'
            '          - unique\n          - not_null: {name: check}\n'
            '  - name: beta\n    columns:\n      - name: id\n        data_tests:\n'
            '          - not_null: {name: check.nested}\n'
        ),
    )
    manifest = _parse(tmp_path)
    alpha_id = 'model.probe.alpha'
    expected = {
        unique_id
        for unique_id, info in manifest['nodes'].items()
        if info['resource_type'] == 'test' and alpha_id in info.get('depends_on', {}).get('nodes', [])
    }

    alpha_task = next(
        task
        for task in create_dbt_factory(bundle_tests=True).create_tasks(manifest)
        if task['task_key'] == 'alpha_test'
    )
    commands = []
    for raw_command in alpha_task['dbt_task']['commands']:
        if raw_command.startswith('dbt test '):
            commands.append(shlex.split(raw_command))
    modes = [command[command.index('--indirect-selection') + 1] for command in commands]
    assert modes == ['empty', 'cautious']

    selected: set[str] = set()
    for command in commands:
        select = command[command.index('--select') + 1]
        assert select.split() == sorted(select.split())
        result = _dbt(
            tmp_path,
            'ls',
            '--quiet',
            *command[2:],
            '--resource-type',
            'test',
            '--output',
            'json',
            '--output-keys',
            'unique_id',
        )
        assert result.success, f'dbt ls failed for {select!r}: {result.exception}'
        assert isinstance(result.result, list)
        selected.update(json.loads(entry)['unique_id'] for entry in result.result)
    assert selected == expected


def test_bundled_data_and_unit_tests_match_their_exact_manifest_membership(tmp_path):
    _write_project(
        tmp_path,
        {'orders.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: orders\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests: [not_null]\n'
            'unit_tests:\n'
            '  - name: totals\n'
            '    model: orders\n'
            '    given: []\n'
            '    expect: {rows: [{id: 1}]}\n'
        ),
    )
    manifest = _parse(tmp_path)

    data_test_ids = {unique_id for unique_id, info in manifest['nodes'].items() if info['resource_type'] == 'test'}
    unit_test_ids = set(manifest.get('unit_tests', {}))
    assert data_test_ids and unit_test_ids, 'dbt did not parse both test kinds'

    task = next(
        task
        for task in create_dbt_factory(bundle_tests=True).create_tasks(manifest)
        if task['task_key'] == 'orders_test'
    )
    assert _selected_by_bundled_commands(tmp_path, task) == data_test_ids | unit_test_ids
    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=True)


def test_bundled_task_last_indirect_selection_option_controls_dbt(tmp_path):
    """dbt applies the final repeated indirect-selection option emitted by a bundled test task."""
    _write_project(
        tmp_path,
        {'alpha.sql': MODEL_SQL, 'beta.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: alpha\n    columns:\n      - name: id\n        data_tests: [not_null]\n'
            '  - name: beta\n    columns:\n      - name: id\n'
        ),
    )
    tests_dir = tmp_path / 'tests'
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / 'alpha.sql').write_text("select * from {{ ref('beta') }} where id is null\n", encoding='utf-8')
    manifest = _parse(tmp_path)
    factory = create_dbt_factory(bundle_tests=True)
    factory.task_factories['test'].dbt_options = '--target dev --indirect-selection eager'
    task = next(task for task in factory.create_tasks(manifest) if task['task_key'] == 'beta_test')
    command = shlex.split(task['dbt_task']['commands'][-1])
    modes = [command[index + 1] for index, value in enumerate(command[:-1]) if value == '--indirect-selection']
    assert modes == ['eager', 'empty']

    select = command[command.index('--select') + 1]
    eager = _selected_unique_ids(tmp_path, select, 'test', indirect_selection='eager')
    empty = _selected_unique_ids(tmp_path, select, 'test', indirect_selection='empty')
    assert len(eager) == 2
    assert len(empty) == 1

    result = _dbt(
        tmp_path,
        'ls',
        '--quiet',
        *command[2:],
        '--resource-type',
        'test',
        '--output',
        'json',
        '--output-keys',
        'unique_id',
    )
    assert result.success, f'dbt ls failed for emitted options: {result.exception}'
    assert isinstance(result.result, list), f'expected dbt ls to return a list, got {type(result.result)}'
    selected = tuple(sorted(json.loads(entry)['unique_id'] for entry in result.result))
    assert selected == empty
    assert selected != eager


def test_selection_changing_extra_options_are_refused_after_live_dbt_proves_the_risk(tmp_path):
    """Repeated includes union and exclusions can silently widen or empty an otherwise exact selection."""
    _write_project(tmp_path, {'alpha.sql': MODEL_SQL, 'beta.sql': MODEL_SQL})
    _parse(tmp_path)

    union = _dbt(
        tmp_path,
        'ls',
        '--quiet',
        '--select',
        'alpha',
        '--select',
        'beta',
        '--output',
        'json',
        '--output-keys',
        'unique_id',
    )
    assert union.success, f'dbt ls failed for repeated --select: {union.exception}'
    assert isinstance(union.result, list)
    assert tuple(sorted(json.loads(entry)['unique_id'] for entry in union.result)) == (
        'model.probe.alpha',
        'model.probe.beta',
    )

    excluded = _dbt(
        tmp_path,
        'ls',
        '--quiet',
        '--select',
        'alpha',
        '--exclude',
        'alpha',
        '--output',
        'json',
        '--output-keys',
        'unique_id',
    )
    assert excluded.success, f'dbt ls failed for --exclude: {excluded.exception}'
    assert excluded.result == []

    clustered = _dbt(
        tmp_path,
        'ls',
        '--quiet',
        '-xsbeta',
        '--output',
        'json',
        '--output-keys',
        'unique_id',
    )
    assert clustered.success, f'dbt ls failed for clustered -x -s: {clustered.exception}'
    assert isinstance(clustered.result, list)
    assert tuple(json.loads(entry)['unique_id'] for entry in clustered.result) == ('model.probe.beta',)

    for dbt_options in ('--select beta', '--exclude alpha', '-xsbeta'):
        with pytest.raises(ValueError, match='selection'):
            create_dbt_factory(dbt_options=dbt_options)


def test_parse_context_override_is_refused_after_live_dbt_proves_manifest_drift(tmp_path):
    """A runtime vars override can remove a node that an exact manifest-derived selector names."""
    _write_project(
        tmp_path,
        {
            'alpha.sql': "{{ config(enabled=var('enable_alpha', true)) }}\nselect 1 as id\n",
            'beta.sql': MODEL_SQL,
        },
    )
    manifest = _parse(tmp_path)
    assert 'model.probe.alpha' in manifest['nodes']

    alpha_task = next(task for task in create_dbt_factory().create_tasks(manifest) if task['task_key'] == 'alpha_model')
    command = shlex.split(alpha_task['dbt_task']['commands'][-1])
    selector = command[command.index('--select') + 1]
    assert _selected_unique_ids(tmp_path, selector, 'model') == ('model.probe.alpha',)

    drifted = _dbt(
        tmp_path,
        'ls',
        '--quiet',
        '--select',
        selector,
        '--vars',
        '{enable_alpha: false}',
        '--resource-type',
        'model',
        '--output',
        'json',
        '--output-keys',
        'unique_id',
    )
    assert drifted.success, f'dbt ls failed for the vars override: {drifted.exception}'
    assert drifted.result == []

    with pytest.raises(ValueError, match='runtime parse context'):
        create_dbt_factory(dbt_options="--vars '{enable_alpha: false}'")


def test_duplicate_target_is_refused_after_live_dbt_proves_last_target_drift(tmp_path):
    """dbt uses the last repeated target, which can remove a node parsed under the controlled target."""
    _write_project(
        tmp_path,
        {
            'dev_only.sql': "{{ config(enabled=target.name == 'dev') }}\nselect 1 as id\n",
            'shared.sql': MODEL_SQL,
        },
    )
    (tmp_path / 'profiles.yml').write_text(
        PROFILES
        + """\
    prod:
      type: databricks
      host: example.databricks.com
      http_path: /sql/1.0/warehouses/x
      token: dummy
      schema: default
""",
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)
    assert 'model.probe.dev_only' in manifest['nodes']

    dev_only_task = next(
        task for task in create_dbt_factory().create_tasks(manifest) if task['task_key'] == 'dev_only_model'
    )
    command = shlex.split(dev_only_task['dbt_task']['commands'][-1])
    selector = command[command.index('--select') + 1]
    assert _selected_unique_ids(tmp_path, selector, 'model') == ('model.probe.dev_only',)

    drifted = _dbt(
        tmp_path,
        'ls',
        '--quiet',
        '--target',
        'dev',
        '--target',
        'prod',
        '--select',
        selector,
        '--resource-type',
        'model',
        '--output',
        'json',
        '--output-keys',
        'unique_id',
    )
    assert drifted.success, f'dbt ls failed for repeated targets: {drifted.exception}'
    assert drifted.result == []

    with pytest.raises(ValueError, match='at most one target'):
        create_dbt_factory(dbt_options='--target dev --target prod')


@pytest.mark.parametrize(
    ('target_args', 'dbt_options'),
    [
        pytest.param(('--target',), '--target', id='long-missing'),
        pytest.param(('-t',), '-t', id='short-missing'),
        pytest.param(('--target=',), '--target=', id='long-empty-attached'),
        pytest.param(('--target', ''), "--target ''", id='long-empty-separate'),
        pytest.param(('-t', ''), "-t ''", id='short-empty-separate'),
    ],
)
def test_target_without_value_is_refused_after_live_dbt_proves_it_is_incomplete(tmp_path, target_args, dbt_options):
    """A controlled target token must include the value dbt requires."""
    _write_project(tmp_path, {'intended.sql': MODEL_SQL})

    incomplete = _dbt(tmp_path, 'ls', '--quiet', *target_args)
    assert not incomplete.success, f'dbt unexpectedly accepted {dbt_options!r} without a value'

    with pytest.raises(ValueError, match='target requires a nonempty value'):
        create_dbt_factory(dbt_options=dbt_options)


def test_option_value_cannot_hide_a_second_selector_after_command_assembly(tmp_path, monkeypatch):
    """A value-taking global option cannot consume a target flag and expose its value as a selector."""
    _write_project(tmp_path, {'intended.sql': MODEL_SQL, 'other.sql': MODEL_SQL})
    _parse(tmp_path)

    monkeypatch.chdir(tmp_path)
    widened = _dbt(
        tmp_path,
        'ls',
        '--quiet',
        '--select',
        'intended',
        '--log-path',
        '--target',
        '-sother',
        '--output',
        'json',
        '--output-keys',
        'unique_id',
    )
    assert widened.success, f'dbt rejected the full option sequence: {widened.exception}'
    assert isinstance(widened.result, list)
    assert tuple(sorted(json.loads(entry)['unique_id'] for entry in widened.result)) == (
        'model.probe.intended',
        'model.probe.other',
    )

    for dbt_options in ('--log-path --target -sother', '--log-path -t -sother'):
        with pytest.raises(ValueError, match='at most one target'):
            create_dbt_factory(dbt_options=dbt_options)


def test_singular_test_not_sharing_a_models_fqn_is_kept(tmp_path):
    """
    The boundary of the refusal above: a singular test whose name does *not* collide with a model is
    perfectly addressable and must still generate. Refusing every singular test would be far stricter
    than dbt requires.
    """
    _write_project(tmp_path, {'beta.sql': MODEL_SQL})
    tests_dir = tmp_path / 'tests'
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / 'beta_is_sane.sql').write_text("select * from {{ ref('beta') }} where id is null\n", encoding='utf-8')
    manifest = _parse(tmp_path)

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_bundled_source_test_selector_is_exact(tmp_path):
    """A source bundle resolves exactly and gates a model that consumes the source."""
    _write_project(tmp_path, {'downstream.sql': "select * from {{ source('raw', 'orders') }}\n"})
    (tmp_path / 'models' / 'sources.yml').write_text(
        'sources:\n'
        '  - name: raw\n'
        '    schema: default\n'
        '    tables:\n'
        '      - name: orders\n'
        '        columns:\n'
        '          - name: id\n'
        '            data_tests: [not_null]\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)
    tasks = create_dbt_factory(bundle_tests=True).create_tasks(manifest)
    by_key = {task['task_key']: task for task in tasks}

    selectors = [s for _, s, verb in _resource_selectors(manifest, bundle_tests=True) if verb == 'test']
    assert selectors, 'expected a bundled source test task'
    assert by_key['downstream_model']['depends_on'] == [{'task_key': 'raw_orders_test'}]
    assert not any(select.startswith('source:') for select in selectors)
    for select in selectors:
        selected = _selected_ids(tmp_path, select, 'test', indirect_selection='empty')
        assert len(selected) == 1, f'{select!r} selects {selected}, expected exactly one test'


def test_bundled_seed_test_gates_snapshot_that_consumes_the_seed(tmp_path):
    _write_project(
        tmp_path,
        {'placeholder.sql': MODEL_SQL},
        schema_yml="""seeds:
  - name: countries
    columns:
      - name: id
        data_tests: [not_null]
""",
    )
    seeds_dir = tmp_path / 'seeds'
    seeds_dir.mkdir()
    (seeds_dir / 'countries.csv').write_text('id,name\n1,France\n', encoding='utf-8')
    snapshots_dir = tmp_path / 'snapshots'
    snapshots_dir.mkdir()
    (snapshots_dir / 'country_history.sql').write_text(
        """{% snapshot country_history %}
{{ config(target_schema='snapshots', strategy='check', unique_key='id', check_cols=['name']) }}
select * from {{ ref('countries') }}
{% endsnapshot %}
""",
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    tasks = create_dbt_factory(bundle_tests=True).create_tasks(manifest)
    by_key = {task['task_key']: task for task in tasks}

    assert by_key['countries_test']['depends_on'] == [{'task_key': 'countries_seed'}]
    assert by_key['country_history_snapshot']['depends_on'] == [{'task_key': 'countries_test'}]


def test_every_task_selector_resolves_to_one_node(tmp_path):
    """
    Every generated task — models, data tests and unit tests alike — must select exactly one node.

    `--indirect-selection empty` stands in for the resource-type filter each task actually carries:
    without it dbt eagerly adds a selected model's attached tests, which is desirable when the task
    runs but would obscure whether the selector itself is exact. It also avoids guessing dbt's type
    name — a unit test is `unit_test`, not `test`, and a naive mapping filters it out and reports
    zero, which is how an earlier version of this check hid a passing case as a failure.
    """
    _write_project(
        tmp_path,
        {'marts/orders.sql': MODEL_SQL, 'marts/orders/items.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: orders\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests: [unique, not_null]\n'
            'unit_tests:\n'
            '  - name: ut_orders\n'
            '    model: orders\n'
            '    given: []\n'
            '    expect: {rows: [{id: 1}]}\n'
        ),
    )
    manifest = _parse(tmp_path)

    for task_key, select, _verb in _resource_selectors(manifest, bundle_tests=False):
        selected = _selected_ids(tmp_path, select, resource_type=None, indirect_selection='empty')
        assert len(selected) == 1, f'{task_key} selects {selected} via {select!r}, expected exactly one node'


def test_same_type_tests_in_one_file_resolve_individually(tmp_path):
    """
    Two `not_null` tests in one `schema.yml` share a package, a file *and* a test type, so
    `test_name:` cannot separate them. Under a spacey directory the fqn is unusable too, leaving the
    bare resource name as the only discriminator — verified here against dbt rather than asserted.
    """
    _write_project(tmp_path, {'my tests/a.sql': MODEL_SQL, 'my tests/b.sql': MODEL_SQL})
    (tmp_path / 'models' / 'my tests' / 'schema.yml').write_text(
        'models:\n'
        '  - name: a\n'
        '    columns:\n'
        '      - name: id\n'
        '        data_tests: [not_null]\n'
        '  - name: b\n'
        '    columns:\n'
        '      - name: id\n'
        '        data_tests: [not_null]\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    selectors = _resource_selectors(manifest, bundle_tests=False)
    test_selectors = {key: select for key, select, verb in selectors if verb == 'test'}
    assert len(set(test_selectors.values())) == len(
        test_selectors
    ), f'the two same-type tests share a selector: {test_selectors}'
    for task_key, select in test_selectors.items():
        selected = _selected_ids(tmp_path, select, 'test', indirect=False)
        assert len(selected) == 1, f'{task_key} selects {selected} via {select!r}, expected exactly one test'


def test_leading_numeric_graph_operator_in_a_name_is_addressable_under_an_explicit_fqn(tmp_path):
    """
    dbt reads a leading `N+` as parent depth only while it is *inferring* the selector method. Naming the
    method makes it literal, so a custom test name of `2+check` is addressable.

    This layout used to be refused, and it is the sharpest illustration of what the `fqn:` prefix buys: the
    space in the directory kills the fqn path, the shared `schema.yml` kills `file:`, and the leading `2+`
    used to kill the name fallback too — leaving nothing. Verified on dbt 1.12.0: bare `2+check` selects
    nothing, while `fqn:2+check` resolves to exactly `test.probe.2+check`.

    A *trailing* `+N` is still fatal and still refused; see
    `test_a_file_name_alone_is_not_enough_to_address_a_resource` and, for sources,
    `test_source_with_a_trailing_graph_operator_is_refused`.
    """
    _write_project(tmp_path, {'my dir/a.sql': MODEL_SQL, 'my dir/b.sql': MODEL_SQL})
    (tmp_path / 'models' / 'my dir' / 'schema.yml').write_text(
        'models:\n'
        '  - name: a\n'
        '    columns:\n'
        '      - name: id\n'
        '        data_tests:\n'
        '          - not_null: {name: "2+check"}\n'
        '  - name: b\n'
        '    columns:\n'
        '      - name: id\n'
        '        data_tests: [not_null]\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    # The bare value really is unusable — this is why the explicit method matters.
    assert not _selected_unique_ids(tmp_path, '2+check', 'test')
    assert _selected_unique_ids(tmp_path, 'fqn:2+check', 'test')

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_source_with_a_trailing_graph_operator_uses_an_exact_test_selector(tmp_path):
    """
    The whole `source:...` string is one raw selector, so a table ending in `+N` is read as a graph
    operator: `source:probe.raw.orders+1` matches nothing while `dbt test` still exits 0, so the
    source's tests would silently never run. Bundled mode instead selects the attached test node exactly.
    """
    _write_project(tmp_path, {'downstream.sql': "select * from {{ source('raw','orders+1') }}\n"})
    (tmp_path / 'models' / 'sources.yml').write_text(
        'sources:\n'
        '  - name: raw\n'
        '    schema: default\n'
        '    tables:\n'
        '      - name: "orders+1"\n'
        '        identifier: ord\n'
        '        columns:\n'
        '          - name: id\n'
        '            data_tests: [not_null]\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    # The selector we would otherwise have emitted matches nothing, and dbt still exits 0 for it.
    assert not _selected_ids(tmp_path, 'source:probe.raw.orders+1', None, indirect=True)

    selectors = [select for _, select, verb in _resource_selectors(manifest, bundle_tests=True) if verb == 'test']
    assert selectors and not any(select.startswith('source:') for select in selectors)
    assert _selected_unique_ids(tmp_path, selectors[0], 'test', indirect_selection='empty')


def test_source_keeps_an_operator_away_from_the_boundary(tmp_path):
    """
    The mirror image: `2+ord` puts the operator mid-string, where dbt resolves it exactly. Refusing
    it would reject a working project, so the guard must apply to the boundary only.
    """
    _write_project(tmp_path, {'downstream.sql': "select * from {{ source('raw','2+ord') }}\n"})
    (tmp_path / 'models' / 'sources.yml').write_text(
        'sources:\n'
        '  - name: raw\n'
        '    schema: default\n'
        '    tables:\n'
        '      - name: "2+ord"\n'
        '        identifier: ord\n'
        '        columns:\n'
        '          - name: id\n'
        '            data_tests: [not_null]\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    test_selectors = [s for _, s, verb in _resource_selectors(manifest, bundle_tests=True) if verb == 'test']
    assert test_selectors and not any(select.startswith('source:') for select in test_selectors)
    for select in test_selectors:
        assert _selected_ids(tmp_path, select, 'test', indirect_selection='empty'), f'{select!r} matched nothing'


def test_literal_and_incomplete_braces_in_a_source_still_allow_an_exact_test_bundle(tmp_path):
    """Literal source braces do not enter the bundle when the attached test is directly addressable."""
    _write_project(tmp_path, {'downstream.sql': MODEL_SQL})
    (tmp_path / 'models' / 'sources.yml').write_text(
        'sources:\n'
        "  - name: \"raw{{ '{' }}{{ '{' }}draft\"\n"
        '    schema: default\n'
        '    tables:\n'
        '      - name: "orders{v1}"\n'
        '        identifier: ord\n'
        '        columns:\n'
        '          - name: id\n'
        '            data_tests: [not_null]\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    source_id, source = next(iter(manifest['sources'].items()))
    assert source_id == 'source.probe.raw{{draft.orders{v1}'
    assert source['source_name'] == 'raw{{draft'
    assert source['name'] == 'orders{v1}'

    source_select = 'source:probe.raw{{draft.orders{v1}'
    assert _selected_unique_ids(tmp_path, source_select, 'source') == (source_id,)
    expected_tests = tuple(
        sorted(
            unique_id
            for unique_id, info in manifest['nodes'].items()
            if source_id in info.get('depends_on', {}).get('nodes', [])
        )
    )
    assert expected_tests
    test_selectors = [select for _, select, verb in _resource_selectors(manifest, bundle_tests=True) if verb == 'test']
    assert test_selectors and not any(select.startswith('source:') for select in test_selectors)
    assert _selected_unique_ids(tmp_path, test_selectors[0], 'test', indirect_selection='empty') == expected_tests


def test_source_dynamic_reference_in_the_generated_test_name_is_refused(tmp_path):
    """dbt carries the source reference into the test name, so no safe direct selector survives."""
    _write_project(tmp_path, {'downstream.sql': MODEL_SQL})
    (tmp_path / 'models' / 'sources.yml').write_text(
        'sources:\n'
        "  - name: \"{{ '{' }}{{ '{' }}job\"\n"
        '    schema: default\n'
        '    tables:\n'
        "      - name: \"id{{ '}' }}{{ '}' }}\"\n"
        '        identifier: ord\n'
        '        columns:\n'
        '          - name: id\n'
        '            data_tests: [not_null]\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    source_id, source = next(iter(manifest['sources'].items()))
    assert source_id == 'source.probe.{{job.id}}'
    assert source['source_name'] == '{{job'
    assert source['name'] == 'id}}'
    assembled = 'source:probe.{{job.id}}'
    assert _selected_unique_ids(tmp_path, assembled, 'source') == (source_id,)

    with pytest.raises(ValueError, match='Cannot generate a task for'):
        _resource_selectors(manifest, bundle_tests=True)


def test_tests_sharing_a_file_with_no_usable_name_are_refused(tmp_path):
    """
    When neither the fqn nor the bare name survives, `package:`+`file:`+`test_name:` is all that is
    left — and a `schema.yml` holds every test declared in it, so that combination is not exact.
    Emitting it would make one task run another task's test too, the duplicate-build class of bug.

    A bracketed custom test name kills both the fqn and the name (`[...]` is fnmatch syntax).
    """
    _write_project(tmp_path, {'a.sql': MODEL_SQL, 'b.sql': MODEL_SQL})
    (tmp_path / 'models' / 'schema.yml').write_text(
        'models:\n'
        '  - name: a\n'
        '    columns:\n'
        '      - name: id\n'
        '        data_tests:\n'
        '          - not_null: {name: "check[a]id"}\n'
        '  - name: b\n'
        '    columns:\n'
        '      - name: id\n'
        '        data_tests: [not_null]\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    # What the un-guarded selector resolved to: two tests, so two tasks would both run not_null_b_id.
    assert len(_selected_ids(tmp_path, 'package:probe,file:schema.yml,test_name:not_null', 'test', indirect=False)) == 2

    with pytest.raises(ValueError, match='Cannot generate a task for'):
        _resource_selectors(manifest, bundle_tests=False)


def test_a_file_name_alone_is_not_enough_to_address_a_resource(tmp_path):
    """
    `file:` never counts as addressing a resource by itself, even when the file holds exactly one.

    dbt has no `unique_id:` selector — confirmed by brute-forcing every method name it accepts — so a
    selector is always a predicate that may match several nodes, and exactness has to be established per
    node. "This file holds one resource" is instead a property of the surrounding project, and `file:`
    matches a *base name* rather than a path, so it is not one the manifest states directly. A resource
    whose fqn and name are both unusable is therefore refused, with an error naming the remedy.

    `models/orders+1.sql` is the shape that pays for it: the fqn `probe.orders+1` and the bare name both
    end in `+1`, which dbt reads as child depth. `package:probe,file:orders+1.sql` does resolve to
    exactly one node — verified here — so this refusal is deliberately stricter than dbt requires.
    """
    _write_project(tmp_path, {'orders+1.sql': MODEL_SQL, 'orders.sql': MODEL_SQL})
    manifest = _parse(tmp_path)

    # dbt would accept it: the selector we no longer emit is exact.
    assert _selected_ids(tmp_path, 'package:probe,file:orders+1.sql', 'model') == ('probe.orders+1',)

    with pytest.raises(ValueError, match='Cannot generate a task for'):
        _resource_selectors(manifest, bundle_tests=False)


def test_a_usable_name_still_rescues_an_unusable_fqn(tmp_path):
    """
    The boundary of the refusal above: only the *name* has to survive, not the fqn. A space in the
    directory makes the fqn unusable, but `orders` is a fine selector and dbt matches it against the
    fqn's leaf, so these projects keep working. Refusing them too would reject any project with a
    space in a directory name.
    """
    _write_project(tmp_path, {'my dir/orders.sql': MODEL_SQL, 'my dir/items.sql': MODEL_SQL})
    manifest = _parse(tmp_path)

    for task_key, select, _verb in _resource_selectors(manifest, bundle_tests=False):
        selected = _selected_ids(tmp_path, select, 'model')
        assert len(selected) == 1, f'{task_key} selects {selected} via {select!r}, expected exactly one node'


@pytest.mark.parametrize(
    ('source_name', 'table'),
    [
        pytest.param('raw.v1', 'ord', id='dotted-source-name'),
        pytest.param('raw', 'ord.v1', id='dotted-table-name'),
    ],
)
def test_dotted_source_part_uses_an_exact_test_selector(tmp_path, source_name, table):
    """
    `.` delimits dbt's source grammar, which takes at most `pkg.source.table`. A dot inside one part
    makes four, and dbt rejects the selector with a Runtime Error rather than selecting nothing — so
    the source selector would fail at run time. Bundled mode does not emit it; it addresses the test node.

    Asserted from both ends: dbt rejects the naive string, while the emitted selector resolves the test.
    """
    _write_project(tmp_path, {'downstream.sql': f"select * from {{{{ source('{source_name}','{table}') }}}}\n"})
    (tmp_path / 'models' / 'sources.yml').write_text(
        f'sources:\n'
        f'  - name: "{source_name}"\n'
        f'    schema: default\n'
        f'    tables:\n'
        f'      - name: "{table}"\n'
        f'        identifier: ord\n'
        f'        columns:\n'
        f'          - name: id\n'
        f'            data_tests: [not_null]\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    # dbt's own verdict on the selector we would otherwise have emitted.
    rejected = _dbt(tmp_path, 'ls', '--quiet', '--select', f'source:probe.{source_name}.{table}')
    assert not rejected.success, 'dbt accepted a four-part source selector; this test is no longer meaningful'

    selectors = [select for _, select, verb in _resource_selectors(manifest, bundle_tests=True) if verb == 'test']
    assert selectors and not any(select.startswith('source:') for select in selectors)
    assert _selected_unique_ids(tmp_path, selectors[0], 'test', indirect_selection='empty')


def test_disabled_node_left_in_the_manifest_gets_no_task(tmp_path):
    """
    A versioned model whose declared version has no file leaves a *disabled* test node inside the
    manifest's `nodes` (dbt normally files disabled resources under `disabled`, which we never read).
    dbt selects nothing for it and `dbt test` still exits 0, so a task for it would go green having
    asserted nothing. Let dbt build the shape rather than hand-writing the node.
    """
    _write_project(
        tmp_path,
        {'orders.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: orders\n'
            '    latest_version: 2\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests: [not_null]\n'
            '    versions:\n'
            '      - v: 1\n'
            '      - v: 2\n'
        ),
    )
    manifest = _parse(tmp_path)

    # Guard the fixture: if dbt stops leaking the disabled node, this test proves nothing.
    disabled = [info for info in manifest['nodes'].values() if info.get('config', {}).get('enabled') is False]
    assert disabled, 'dbt no longer leaves a disabled node in `nodes`; this test is no longer meaningful'
    assert not _selected_ids(tmp_path, 'probe.not_null_orders_v1_id', 'test', indirect=False)

    for bundle_tests in (False, True):
        for task_key, select, _verb in _resource_selectors(manifest, bundle_tests):
            selected = _selected_ids(tmp_path, select, resource_type=None, indirect_selection='empty')
            assert selected, f'{task_key} selects nothing via {select!r}; the task would pass having done nothing'


# Building blocks for the generative case: names that collide with directory names, names carrying a
# dot, and nesting, which together reproduce every prefix hazard found so far.
_NAME_POOL = ('orders', 'items', 'dim', 'orders.items', 'marts')
_DIR_POOL = ('', 'marts/', 'orders/', 'marts/orders/')


def _random_layout(rng: random.Random) -> dict[str, str]:
    """A randomised set of model paths drawn from the pools above, deduplicated by resource name."""
    layout: dict[str, str] = {}
    used_names: set[str] = set()
    for _ in range(rng.randint(2, 5)):
        name = rng.choice(_NAME_POOL)
        directory = rng.choice(_DIR_POOL)
        # dbt rejects duplicate resource names in one package, so keep them distinct.
        if name in used_names:
            continue
        used_names.add(name)
        layout[f'{directory}{name}.sql'] = MODEL_SQL
    return layout


@pytest.mark.parametrize('seed', range(8))
def test_generated_selectors_are_exact(tmp_path, seed):
    """
    The generative case. For a randomised layout, every resource task's selector must resolve to
    exactly one node — or generation must refuse to emit it. Anything else means a task would build
    the wrong model, or none, at run time.
    """
    rng = random.Random(seed)
    layout = _random_layout(rng)
    if len(layout) < 2:  # pragma: no cover - a degenerate draw carries no information
        pytest.skip('layout collapsed to a single model')
    _write_project(tmp_path, layout)
    manifest = _parse(tmp_path)

    for bundle_tests in (False, True):
        # Every layout here is selector-safe and name-deduplicated, so generation must *succeed*.
        # Tolerating a refusal would let an implementation that rejects everything pass this test;
        # the unisolable cases are asserted explicitly in their own fixtures instead.
        _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests)


def test_random_layouts_are_not_degenerate(tmp_path):
    """
    Guards the generative fixture itself.

    The previous version of this test claimed to check "that the pools cannot produce a duplicate
    resource name" but asserted only `not name.startswith('/')` — true of every pool entry, unrelated to
    duplicates, and unable to fail for any pool content. It also never parsed anything, despite its name.

    What actually matters is that a drawn layout still carries information: `_random_layout` silently
    `continue`s past a duplicate name, so a badly-chosen pool could collapse every draw to one model and
    leave `test_generated_selectors_are_exact` asserting nothing. So check the real property — most seeds
    yield a multi-model layout dbt can parse — rather than a proxy for it.
    """
    sizes = []
    for seed in range(8):
        layout = _random_layout(random.Random(seed))
        sizes.append(len(layout))
    assert sum(1 for size in sizes if size >= 2) >= 6, (
        f'the pools collapse too often to exercise prefix hazards: layout sizes {sizes}. '
        'The generative test would be near-vacuous.'
    )

    # And the pools really do produce the prefix hazards they exist for: a name that is another name's
    # parent directory, and a dotted name that flattens onto a nested fqn.
    assert any('.' in name for name in _NAME_POOL), 'no dotted name; the flattening hazard is unreachable'
    assert any(f'{name}/' in _DIR_POOL for name in _NAME_POOL), 'no name shared with a directory'

    # Finally parse one, so a layout dbt rejects cannot make the generative test vacuous unnoticed.
    biggest = max((_random_layout(random.Random(seed)) for seed in range(8)), key=len)
    _write_project(tmp_path, biggest)
    _parse(tmp_path)


def test_fqn_prefix_collision_between_sibling_tests_is_parent_scoped(tmp_path):
    """
    A test named `check.nested` flattens to `[probe, check, nested]`, so the sibling `check`'s selector
    `probe.check` matches it as a subtree parent.

    Confirmed on dbt 1.12.0 with every discriminator present and identical — the collision does not
    depend on any term being dropped:

        probe.check,package:probe,file:schema.yml,test_name:not_null
          -> ('test.probe.check.d0dfa850a3', 'test.probe.check.nested.484de86d57')

    and no direct term narrows it. The exact parent intersection remains safe under per-term cautious
    expansion because the parent-specific file term admits only the test attached to that parent.
    """
    _write_project(
        tmp_path,
        {'check.sql': MODEL_SQL, 'other.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: check\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests:\n'
            '          - not_null: {name: check}\n'
            '  - name: other\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests:\n'
            '          - not_null: {name: check.nested}\n'
        ),
    )
    manifest = _parse(tmp_path)

    # dbt's own verdict on the selector an earlier revision emitted.
    both = _selected_ids(tmp_path, 'probe.check,package:probe,file:schema.yml,test_name:not_null', 'test')
    assert len(both) == 2, f'expected the prefix collision to select two tests, got {both}'

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_parent_scope_is_refused_when_a_test_term_also_selects_the_parent(tmp_path):
    """
    Both custom tests have the direct selector `fqn:probe.orders`, so their distinct parents appear to
    provide an exact scope. Under `cautious`, however, that fqn term also selects the `orders` model and
    expands its ordinary `not_null` sibling before dbt intersects the selector terms. The resulting task
    cannot be proved to run only the intended custom test and must be refused.
    """
    _write_project(
        tmp_path,
        {'orders.sql': MODEL_SQL, 'other.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: orders\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests:\n'
            '          - not_null: {name: orders}\n'
            '          - not_null\n'
            '  - name: other\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests:\n'
            '          - not_null: {name: orders}\n'
        ),
    )
    manifest = _parse(tmp_path)

    custom_selector = 'fqn:probe.orders,package:probe,file:schema.yml,resource_type:test,test_name:not_null'
    custom_tests = _selected_unique_ids(tmp_path, custom_selector, None, indirect_selection='empty')
    assert len(custom_tests) == 2, f'fixture no longer produces the equal direct selectors: {custom_tests}'

    intended = next(
        unique_id
        for unique_id in custom_tests
        if 'model.probe.orders' in manifest['nodes'][unique_id]['depends_on']['nodes']
    )
    ordinary_sibling = next(
        unique_id
        for unique_id, info in manifest['nodes'].items()
        if info['resource_type'] == 'test'
        and unique_id not in custom_tests
        and 'model.probe.orders' in info['depends_on']['nodes']
    )
    would_be_scoped = 'fqn:probe.orders,package:probe,file:orders.sql,resource_type:model,' f'{custom_selector}'
    cautious_matches = _selected_unique_ids(tmp_path, would_be_scoped, None, indirect_selection='cautious')
    assert intended in cautious_matches
    assert (
        ordinary_sibling in cautious_matches
    ), f'fixture no longer demonstrates cautious sibling expansion: {cautious_matches}'

    with pytest.raises(ValueError, match='also runs'):
        _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_parent_scope_is_refused_when_a_file_term_selects_the_parent(tmp_path):
    """
    The `check` test directly collides with `check.sql` through fqn-prefix and file-stem matching. Its
    snapshot parent appears to isolate it, but the shared `file:check.sql` term directly selects that
    parent and cautiously expands the snapshot's `check.child` sibling into the finished intersection.
    """
    _write_project(tmp_path, {'orders.sql': MODEL_SQL})
    snapshots_dir = tmp_path / 'snapshots' / 'unrelated'
    snapshots_dir.mkdir(parents=True)
    (snapshots_dir / 'check.sql').write_text(
        """{% snapshot other %}
{{ config(target_schema='snapshots', strategy='check', unique_key='id', check_cols=['id']) }}
select 1 as id
{% endsnapshot %}
""",
        encoding='utf-8',
    )
    tests_dir = tmp_path / 'tests'
    tests_dir.mkdir(parents=True)
    (tests_dir / 'check.sql').write_text("select * from {{ ref('other') }} where id is null\n", encoding='utf-8')
    (tests_dir / 'check.child.sql').write_text("select * from {{ ref('other') }} where id is null\n", encoding='utf-8')
    (tests_dir / 'check.sql.sql').write_text("select * from {{ ref('orders') }} where id is null\n", encoding='utf-8')
    manifest = _parse(tmp_path)

    ids_by_path = {
        info['original_file_path']: unique_id
        for unique_id, info in manifest['nodes'].items()
        if info['resource_type'] == 'test'
    }
    intended = ids_by_path['tests/check.sql']
    sibling = ids_by_path['tests/check.child.sql']
    direct = 'fqn:probe.check,package:probe,file:check.sql,resource_type:test'
    direct_matches = _selected_unique_ids(tmp_path, direct, None, indirect_selection='empty')
    assert intended in direct_matches
    assert ids_by_path['tests/check.sql.sql'] in direct_matches

    would_be_scoped = 'fqn:probe.unrelated.check.other,package:probe,file:check.sql,resource_type:snapshot,' f'{direct}'
    cautious_matches = _selected_unique_ids(tmp_path, would_be_scoped, None, indirect_selection='cautious')
    assert intended in cautious_matches
    assert sibling in cautious_matches, f'fixture no longer demonstrates cautious sibling expansion: {cautious_matches}'

    with pytest.raises(ValueError, match='also runs'):
        _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_unit_test_and_data_test_flattening_alike_are_separated_by_resource_type(tmp_path):
    """
    A unit test `unit_orders` on `orders` has fqn `[probe, orders, unit_orders]`; a data test *named*
    `orders.unit_orders` has fqn `[probe, orders.unit_orders]`. These flatten identically, so before
    `resource_type:` was added the unit-test task also selected the data test — which depends on
    `customers`, not `orders`, so it ran before its own model was built.

    Unlike the prefix collision above, this one *is* separable: the two nodes differ in resource type.
    Confirmed on dbt 1.12.0.
    """
    _write_project(
        tmp_path,
        {
            'upstream.sql': MODEL_SQL,
            'orders.sql': "select * from {{ ref('upstream') }}\n",
            'customers.sql': MODEL_SQL,
        },
        schema_yml=(
            'models:\n'
            '  - name: orders\n'
            '    columns:\n'
            '      - name: id\n'
            '  - name: customers\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests:\n'
            '          - not_null: {name: orders.unit_orders}\n'
            'unit_tests:\n'
            '  - name: unit_orders\n'
            '    model: orders\n'
            '    given:\n'
            "      - input: ref('upstream')\n"
            '        rows: [{id: 1}]\n'
            '    expect: {rows: [{id: 1}]}\n'
        ),
    )
    manifest = _parse(tmp_path)
    unit_test = next(iter(manifest['unit_tests'].values()))
    assert unit_test['depends_on']['nodes'] == ['model.probe.orders']

    # Without the resource-type term the two are indistinguishable.
    both = _selected_ids(tmp_path, 'probe.orders.unit_orders,package:probe,file:schema.yml', None)
    assert len(both) == 2, f'expected the flattened fqns to collide, got {both}'

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_versioned_unit_test_clones_have_parent_scoped_tasks(tmp_path):
    """
    dbt clones a unit test once per model version, rewriting only `unique_id`, `depends_on.nodes[0]` and
    `version` — the fqn, name and file are identical. No selector separates them: on dbt 1.12.0
    `version:` accepts only `latest`/`prerelease`/`old`/`none`, none of which match a unit test.

    Each clone therefore uses its exact model version as a parent scope under `cautious`, which yields one
    task per clone without running the sibling version's assertions.
    """
    _write_project(
        tmp_path,
        {'orders_v1.sql': MODEL_SQL, 'orders_v2.sql': MODEL_SQL},
        package_model_paths={
            'probe/orders_v1.sql': MODEL_SQL,
            'probe/orders_v2.sql': MODEL_SQL,
        },
        schema_yml=(
            'models:\n'
            '  - name: orders\n'
            '    latest_version: 2\n'
            '    columns:\n'
            '      - name: id\n'
            '    versions:\n'
            '      - v: 1\n'
            '      - v: 2\n'
            'unit_tests:\n'
            '  - name: ut_orders\n'
            '    model: orders\n'
            '    given: []\n'
            '    expect: {rows: [{id: 1}]}\n'
        ),
    )
    (tmp_path / 'libs' / 'other' / 'models' / 'probe' / 'schema.yml').write_text(
        """\
models:
  - name: orders
    latest_version: 2
    versions:
      - v: 1
      - v: 2
""",
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    # Guard the fixture: if dbt stops cloning with an identical fqn, this test proves nothing.
    clones = [info for info in manifest['unit_tests'].values() if info['name'] == 'ut_orders']
    assert len(clones) == 2, f'expected dbt to clone the unit test per version, got {len(clones)}'
    assert clones[0]['fqn'] == clones[1]['fqn'], 'dbt now varies the fqn per version; revisit parent scoping'
    parent_collision = 'fqn:probe.orders.v1,file:orders_v1.sql,resource_type:model'
    assert (
        len(_selected_unique_ids(tmp_path, parent_collision, 'model', indirect_selection='empty')) == 2
    ), 'fixture no longer makes the installed package collide through its package-stripped fqn'

    tasks = create_dbt_factory(bundle_tests=False).create_tasks(manifest)
    unit_tasks = [task for task in tasks if 'unit_test' in task['task_key']]
    assert len(unit_tasks) == 2, f'expected one task per clone, got {[t["task_key"] for t in unit_tasks]}'
    unique_id_by_task_key = _task_key_to_unique_id(manifest, bundle_tests=False)
    task_key_by_unique_id = {unique_id: task_key for task_key, unique_id in unique_id_by_task_key.items()}
    for task in unit_tasks:
        clone = manifest['unit_tests'][unique_id_by_task_key[task['task_key']]]
        parent_unique_id = clone['depends_on']['nodes'][0]
        assert task['depends_on'] == [{'task_key': task_key_by_unique_id[parent_unique_id]}]
    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_duplicate_test_names_sharing_an_fqn_are_parent_scoped(tmp_path):
    """
    dbt does not require generic-test names to be unique — it disambiguates in the `unique_id` hash only
    — so two models each carrying `not_null: {name: check_id}` produce two test nodes with the *same*
    fqn, name, file and test type. Confirmed on dbt 1.12.0: one selector, two nodes.

    Distinct from the prefix collision: the fqns are equal, not prefix-related. Each task remains exact
    because its parent scope admits only the test attached to that parent.
    """
    _write_project(
        tmp_path,
        {'a.sql': MODEL_SQL, 'b.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: a\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests:\n'
            '          - not_null: {name: check_id}\n'
            '  - name: b\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests:\n'
            '          - not_null: {name: check_id}\n'
        ),
    )
    manifest = _parse(tmp_path)

    fqns = [info['fqn'] for info in manifest['nodes'].values() if info['resource_type'] == 'test']
    assert fqns[0] == fqns[1], f'expected dbt to allow duplicate test names with one fqn, got {fqns}'

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


@pytest.mark.parametrize(
    ('layout', 'schema_yml', 'note'),
    [
        pytest.param(
            {'a.sql': MODEL_SQL},
            (
                'models:\n'
                '  - name: a\n'
                '    columns:\n'
                '      - name: id\n'
                '        data_tests:\n'
                '          - not_null: {name: "check/slash"}\n'
            ),
            'a slash would dispatch a bare value to MethodName.Path',
            id='path-dispatch',
        ),
        pytest.param(
            {'orders.sql.sql': MODEL_SQL, 'plain.sql': MODEL_SQL},
            None,
            'a .sql suffix would dispatch a bare value to MethodName.File',
            id='file-dispatch',
        ),
        pytest.param(
            {'orders.SQL.sql': MODEL_SQL, 'plain.sql': MODEL_SQL},
            None,
            'dbt lowercases before testing the suffix, so .SQL dispatches too',
            id='file-dispatch-uppercase',
        ),
    ],
)
def test_method_dispatching_names_are_addressable_with_an_explicit_fqn(tmp_path, layout, schema_yml, note):
    """
    `SelectionCriteria.default_method` infers the method from a bare value's shape: a path separator makes
    it `MethodName.Path`, and a `.sql`/`.py`/`.csv` suffix (compared case-insensitively) makes it
    `MethodName.File`. Both then match nothing while the task still exits 0.

    Emitting `fqn:` names the method instead of letting dbt guess, so these layouts are addressable rather
    than refused. Verified on dbt 1.12.0: bare `probe.orders.sql` and `probe.check/slash` select nothing,
    while the `fqn:`-prefixed forms each resolve to exactly their node.
    """
    assert note
    _write_project(tmp_path, layout, schema_yml=schema_yml)
    manifest = _parse(tmp_path)

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_dynamic_reference_in_a_path_is_dropped_from_the_selector(tmp_path):
    """
    Databricks substitutes `{{...}}` dynamic references in a dbt task's commands as plain text before the
    task runs, so a model under `models/{{job.id}}/` yields a selector that resolves locally and matches
    nothing once substituted — the task exits 0 having built nothing.

    This is not a dbt-grammar problem, which is why a deny list derived from dbt's grammar could not
    catch it. The fqn term is dropped and the bare name carries the node, so the project still works and
    the emitted selector contains no braces for Databricks to substitute.
    """
    _write_project(tmp_path, {'{{job.id}}/orders.sql': MODEL_SQL, 'plain.sql': MODEL_SQL})
    manifest = _parse(tmp_path)

    # Guard the premise: dbt really does carry the braces into the fqn.
    assert any('{{job.id}}' in info['fqn'] for info in manifest['nodes'].values())

    for task_key, select, _verb in _resource_selectors(manifest, bundle_tests=False):
        assert '{{' not in select, f'{task_key} emits {select!r}, which Databricks would substitute'
    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_literal_braces_are_preserved_and_select_the_real_dbt_node(tmp_path):
    _write_project(tmp_path, {'orders{draft}.sql': MODEL_SQL, 'plain.sql': MODEL_SQL})
    manifest = _parse(tmp_path)

    brace_node = next(info for info in manifest['nodes'].values() if info['name'] == 'orders{draft}')
    assert brace_node['fqn'][-1] == 'orders{draft}'

    selectors = _resource_selectors(manifest, bundle_tests=False)
    brace_select = next(select for _task_key, select, _verb in selectors if '{draft}' in select)
    assert '{draft}' in brace_select
    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_selector_terms_cannot_compose_a_dynamic_reference(tmp_path):
    _write_project(tmp_path, {'orders.sql': MODEL_SQL})
    (tmp_path / 'models' / 'schema}}.yml').write_text(
        'models:\n  - name: orders\n    columns:\n      - name: id\n        data_tests:\n'
        '          - not_null: {name: "check{{"}\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    selectors = _resource_selectors(manifest, bundle_tests=False)
    test_select = next(select for _task_key, select, verb in selectors if verb == 'test')
    assert 'file:schema}}.yml' not in test_select
    assert re.search(r'\{\{[^{}]+\}\}', test_select) is None
    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def _assert_acyclic(manifest, bundle_tests):
    """
    Fails if any emitted task reaches itself through `depends_on`, which Databricks rejects at deploy.

    Acyclicity is necessary but not sufficient: dropping *every* gate edge also satisfies it, so each
    caller is paired with an assertion that the gates which should survive do — see
    `test_a_downstream_model_is_gated_on_the_exact_versioned_unit_test` and
    `test_a_v_named_model_does_not_pick_up_an_unrelated_models_test`.
    """
    tasks = create_dbt_factory(bundle_tests=bundle_tests).create_tasks(manifest)
    graph = {t['task_key']: {d['task_key'] for d in (t.get('depends_on') or [])} for t in tasks}

    def reachable(start):
        seen, stack = set(), [start]
        while stack:
            for dep in graph.get(stack.pop(), ()):
                if dep not in seen:
                    seen.add(dep)
                    stack.append(dep)
        return seen

    cycles = [key for key in graph if key in reachable(key)]
    assert not cycles, f'bundle_tests={bundle_tests} emitted a cyclic depends_on for {cycles}: {graph}'
    return graph


def test_parent_scoped_versioned_unit_tests_do_not_create_a_dependency_cycle(tmp_path):
    """
    Each versioned unit-test clone waits only for its exact model version. A later version depending on an
    earlier one can therefore inherit the earlier version's test without forming a dependency cycle.
    """
    _write_project(
        tmp_path,
        {'orders_v1.sql': MODEL_SQL, 'orders_v2.sql': "select * from {{ ref('orders', v=1) }}\n"},
        schema_yml=(
            'models:\n  - name: orders\n    latest_version: 2\n'
            '    columns:\n      - name: id\n'
            '    versions:\n      - v: 1\n      - v: 2\n'
            'unit_tests:\n  - name: unit_orders\n    model: orders\n'
            '    given: []\n    expect: {rows: [{id: 1}]}\n'
        ),
    )
    manifest = _parse(tmp_path)
    assert len(manifest['unit_tests']) == 2, 'fixture no longer produces two clones'

    # Both modes: `--indirect-selection` changes what a selector resolves to, so bundling reaches a
    # different set of gate edges and has to be asserted separately rather than assumed to follow.
    for bundle_tests in (False, True):
        _assert_acyclic(manifest, bundle_tests)


def test_a_downstream_model_is_gated_on_the_exact_versioned_unit_test(tmp_path):
    """
    A consumer of `orders.v1` is gated on the clone attached to v1, while the unrelated v2 clone remains
    outside its dependency frontier.
    """
    _write_project(
        tmp_path,
        {
            'orders_v1.sql': MODEL_SQL,
            'orders_v2.sql': MODEL_SQL,
            'consumer.sql': "select * from {{ ref('orders', v=1) }}\n",
        },
        schema_yml=(
            'models:\n  - name: orders\n    latest_version: 2\n'
            '    columns:\n      - name: id\n'
            '    versions:\n      - v: 1\n      - v: 2\n'
            '  - name: consumer\n    columns:\n      - name: id\n'
            'unit_tests:\n  - name: unit_orders\n    model: orders\n'
            '    given: []\n    expect: {rows: [{id: 1}]}\n'
        ),
    )
    manifest = _parse(tmp_path)

    tasks = create_dbt_factory(bundle_tests=False).create_tasks(manifest)
    by_key = {t['task_key']: {d['task_key'] for d in (t.get('depends_on') or [])} for t in tasks}
    unit_keys = [key for key in by_key if key.startswith('unit_test')]
    assert unit_keys, f'expected a unit-test task, got {sorted(by_key)}'

    v1_test = next(key for key in unit_keys if 'orders_v1_model' in by_key[key])
    v2_test = next(key for key in unit_keys if 'orders_v2_model' in by_key[key])
    assert v1_test in by_key['consumer_model']
    assert v2_test not in by_key['consumer_model']


def test_backslash_in_a_posix_file_name_keeps_its_file_term(tmp_path):
    """
    dbt splits `original_file_path` with `Path`, which is platform-dependent, so on POSIX a backslash is
    an ordinary character in a file name. Treating it as a separator turned `models/we\\ird.sql` into
    `file:ird.sql`, which `dbt ls` resolves to nothing while the task exits 0 — the same silent no-op the
    Windows handling was added to prevent.
    """
    _write_project(tmp_path, {'we\\ird.sql': MODEL_SQL, 'plain.sql': MODEL_SQL})
    manifest = _parse(tmp_path)

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_file_stem_collision_uses_an_exact_parent_scope(tmp_path):
    """
    `file:a.yml` reaches nodes declared in both `a.yml` and `a.yml.yml`. The shorter test fqn also
    prefixes the nested test, so its direct selector is ambiguous and requires an exact-parent scope.
    """
    _write_project(tmp_path, {'q.sql': MODEL_SQL, 'r.sql': MODEL_SQL})
    (tmp_path / 'models' / 'a.yml').write_text(
        'models:\n  - name: q\n    columns:\n      - name: id\n        data_tests:\n'
        '          - not_null: {name: chk}\n',
        encoding='utf-8',
    )
    (tmp_path / 'models' / 'a.yml.yml').write_text(
        'models:\n  - name: r\n    columns:\n      - name: id\n        data_tests:\n'
        '          - not_null: {name: chk.nested}\n',
        encoding='utf-8',
    )
    manifest = _parse(tmp_path)

    bare = _selected_unique_ids(
        tmp_path, 'probe.chk,package:probe,file:a.yml,resource_type:test,test_name:not_null', None
    )
    explicit = _selected_unique_ids(
        tmp_path, 'fqn:probe.chk,package:probe,file:a.yml,resource_type:test,test_name:not_null', None
    )
    assert len(bare) == 2
    assert explicit == bare

    tasks = create_dbt_factory(bundle_tests=False).create_tasks(manifest)
    test_commands = []
    for task in tasks:
        if task['dbt_task']['commands'][-1].startswith('dbt test'):
            test_commands.append(shlex.split(task['dbt_task']['commands'][-1]))
    modes = [command[command.index('--indirect-selection') + 1] for command in test_commands]
    assert sorted(modes) == ['cautious', 'empty']
    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_singular_test_named_after_a_model_without_other_tests_is_kept(tmp_path):
    """
    Reaching the model is not itself a problem for a direct plan: `resource_type:test` excludes it and
    `--indirect-selection empty` prevents attached-test expansion. The selector resolves to exactly the
    singular test, confirmed with `dbt ls` on dbt 1.12.0.
    """
    _write_project(tmp_path, {'orders.sql': MODEL_SQL})
    tests_dir = tmp_path / 'tests'
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / 'orders.sql').write_text("select * from {{ ref('orders') }} where id is null\n", encoding='utf-8')
    manifest = _parse(tmp_path)

    # dbt reports both by the same name `probe.orders`, so assert per task with the resource type its
    # command carries — the shared name is exactly why `_assert_each_task_selects_its_own_node` cannot
    # distinguish them here.
    assert _selected_ids(tmp_path, 'probe.orders,package:probe,file:orders.sql,resource_type:test', None) == (
        'probe.orders',
    )
    selectors = {verb: select for _key, select, verb in _resource_selectors(manifest, bundle_tests=False)}
    assert set(selectors) == {'run', 'test'}, f'expected a model task and a test task, got {selectors}'
    for verb, select in selectors.items():
        resource_type = 'model' if verb == 'run' else 'test'
        assert _selected_ids(tmp_path, select, resource_type) == (
            'probe.orders',
        ), f'the {verb} task must resolve to exactly its own node via {select!r}'


def _assert_prediction_matches_dbt(tmp_path, manifest, bundle_tests):
    """
    Asserts the factory's exactness model predicts exactly what dbt runs, for every emitted task.

    The strongest available check on `_matching_ids`, because it compares the model against dbt in *both*
    directions: under-predicting lets a collision through, so a task runs another task's resource;
    over-predicting refuses a project dbt handles. A count-based assertion sees neither.

    Each task is replayed with the exact flags its commands carry, including every command and final
    `--indirect-selection` mode in a bundle. A per-test cautious plan is checked directly against the
    manifest id assigned to its task because the direct-selector model does not represent cautious
    expansion.
    """
    peers = {
        **{k: v for k, v in manifest['nodes'].items() if (v.get('config') or {}).get('enabled') is not False},
        **manifest.get('unit_tests', {}),
        **manifest.get('sources', {}),
    }
    index = DbtFactory._selector_index(peers)  # pylint: disable=protected-access
    expected_by_key = _task_key_to_unique_id(manifest, bundle_tests)
    bundled_by_key = _bundled_test_ids_by_task_key(manifest) if bundle_tests else {}
    for task in create_dbt_factory(bundle_tests=bundle_tests).create_tasks(manifest):
        task_key = task['task_key']
        if task_key in bundled_by_key:
            actual = _selected_by_bundled_commands(tmp_path, task) & set(peers)
            expected = bundled_by_key[task_key]
            assert actual == expected, (
                f'{task_key}: dbt runs {sorted(actual)}, expected exact bundle membership ' f'{sorted(expected)}'
            )
            continue

        command = shlex.split(task['dbt_task']['commands'][-1])
        verb = command[1]
        select = command[command.index('--select') + 1]
        if select.startswith('source:'):
            continue
        mode = command[command.index('--indirect-selection') + 1] if '--indirect-selection' in command else None
        resource_type = None if verb == 'test' else {'run': 'model', 'seed': 'seed', 'snapshot': 'snapshot'}[verb]
        # Compare on unique ids: `_selected_ids` returns the *display* names `dbt ls` prints, which do not
        # match manifest keys, so intersecting those with `peers` would silently compare against nothing.
        actual = set(_selected_unique_ids(tmp_path, select, resource_type, indirect_selection=mode)) & set(peers)
        if mode == 'cautious':
            expected = {expected_by_key[task_key]}
            assert actual == expected, (
                f'{task_key}: cautious plan runs {sorted(actual)}, expected exactly {sorted(expected)} '
                f'for {select!r}'
            )
            continue
        predicted = set(DbtFactory._matching_ids(select, index))  # pylint: disable=protected-access
        assert (
            predicted == actual
        ), f'{task_key}: model predicts {sorted(predicted)} but dbt runs {sorted(actual)} for {select!r}'


@pytest.mark.parametrize('bundle_tests', [False, True], ids=['per-test', 'bundled'])
@pytest.mark.parametrize(
    ('layout', 'schema_yml', 'extra'),
    [
        pytest.param({'a.sql': MODEL_SQL, 'b.sql': MODEL_SQL}, None, None, id='plain'),
        pytest.param({'marts/orders.sql': MODEL_SQL, 'marts/orders/items.sql': MODEL_SQL}, None, None, id='nested'),
        pytest.param(
            {'a.sql': MODEL_SQL, 'b.sql': MODEL_SQL},
            'models:\n  - name: a\n    columns:\n      - name: id\n        data_tests: [not_null, unique]\n'
            '  - name: b\n    columns:\n      - name: id\n        data_tests: [not_null]\n',
            None,
            id='attached-tests',
        ),
        pytest.param(
            {'a.sql': MODEL_SQL, 'b.sql': MODEL_SQL},
            'models:\n  - name: a\n    columns:\n      - name: id\n        data_tests:\n'
            '          - not_null: {name: check_id}\n'
            '  - name: b\n    columns:\n      - name: id\n        data_tests:\n'
            '          - not_null: {name: check_id}\n',
            None,
            id='parent-scoped-tests',
        ),
        pytest.param(
            {'beta.sql': MODEL_SQL, 'delta.sql': MODEL_SQL},
            'models:\n  - name: beta\n    columns:\n      - name: id\n        data_tests:\n'
            '          - relationships: {to: ref("delta"), field: id}\n'
            '  - name: delta\n    columns:\n      - name: id\n',
            None,
            id='relationships',
        ),
        pytest.param(
            {'beta.sql': MODEL_SQL},
            None,
            {'tests/beta_is_sane.sql': "select * from {{ ref('beta') }} where id is null\n"},
            id='singular-test',
        ),
        pytest.param(
            {'orders.sql': MODEL_SQL},
            'models:\n  - name: orders\n    columns:\n      - name: id\n        data_tests: [not_null]\n'
            'unit_tests:\n  - name: ut\n    model: orders\n    given: []\n    expect: {rows: [{id: 1}]}\n',
            None,
            id='unit-and-data-test',
        ),
        pytest.param(
            {'orders_v1.sql': MODEL_SQL, 'orders_v2.sql': MODEL_SQL},
            'models:\n  - name: orders\n    latest_version: 2\n    columns:\n      - name: id\n'
            '    versions:\n      - v: 1\n      - v: 2\n',
            None,
            id='versioned',
        ),
        pytest.param(
            {'a.sql': MODEL_SQL, 'b.sql': "select * from {{ ref('a') }}\n", 'c.sql': "select * from {{ ref('b') }}\n"},
            'models:\n  - name: a\n    columns:\n      - name: id\n        data_tests: [not_null]\n'
            '  - name: b\n    columns:\n      - name: id\n        data_tests: [not_null]\n'
            '  - name: c\n    columns:\n      - name: id\n        data_tests: [not_null]\n',
            None,
            id='chained',
        ),
    ],
)
def test_exactness_model_predicts_what_dbt_runs(tmp_path, layout, schema_yml, extra, bundle_tests):
    """
    The exactness check is only as good as its model of dbt's selection, and that model is now the
    load-bearing part of this diff: it decides which projects are refused. So assert it against dbt
    directly, over the shapes that exercise each way dbt can add a node — attached tests, multi-endpoint
    tests, unit tests, versioned clones, and a dependency chain.
    """
    _write_project(tmp_path, layout, schema_yml=schema_yml)
    for relative_path, content in (extra or {}).items():
        target = tmp_path / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding='utf-8')
    manifest = _parse(tmp_path)

    _assert_prediction_matches_dbt(tmp_path, manifest, bundle_tests)


def test_attached_test_leaking_through_a_non_matching_model_is_prevented_by_empty(tmp_path):
    """
    dbt expands indirect selection *per component* and intersects afterwards, so under eager mode a
    component could reach a model the intersection excluded while the model's attached tests were added
    inside that component and survived on their own terms.

    Verified on dbt 1.12.0: with `models/orders.sql` carrying `not_null` and `models/other.sql` carrying
    `not_null: {name: orders}` in the shared `schema.yml`, the second test's selector resolves to *both*
    tests under eager and to exactly one under `empty`. Pinning `empty` therefore makes the layout
    addressable instead of refused, and removes the need to model the expansion at all.
    """
    _write_project(
        tmp_path,
        {'orders.sql': MODEL_SQL, 'other.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: orders\n    columns:\n      - name: id\n        data_tests: [not_null]\n'
            '  - name: other\n    columns:\n      - name: id\n        data_tests:\n'
            '          - not_null: {name: orders}\n'
        ),
    )
    manifest = _parse(tmp_path)

    leaky = 'fqn:probe.orders,package:probe,file:schema.yml,resource_type:test,test_name:not_null'
    assert len(_selected_unique_ids(tmp_path, leaky, None)) == 2, 'eager no longer leaks; revisit this test'
    assert len(_selected_unique_ids(tmp_path, leaky, None, indirect_selection='empty')) == 1

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_multi_endpoint_test_leaking_from_one_endpoint_is_prevented_by_empty(tmp_path):
    """
    dbt's eager rule is "if ANY parent is selected, select the test" — its own words in
    `expand_selection` — so a `relationships` test leaked into a task selecting just one endpoint.

    Verified on dbt 1.12.0: the singular `tests/beta.sql` task's selector resolves to both that test and
    `relationships_beta_id__id__ref_delta_` under eager, and to only its own node under `empty`.
    """
    _write_project(
        tmp_path,
        {'beta.sql': MODEL_SQL, 'delta.sql': MODEL_SQL, 'gamma.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: beta\n    columns:\n      - name: id\n        data_tests:\n'
            '          - relationships: {to: ref("delta"), field: id}\n'
            '  - name: delta\n    columns:\n      - name: id\n'
            '  - name: gamma\n    columns:\n      - name: id\n'
        ),
    )
    tests_dir = tmp_path / 'tests'
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / 'beta.sql').write_text("select * from {{ ref('gamma') }} where id is null\n", encoding='utf-8')
    manifest = _parse(tmp_path)

    leaky = 'fqn:probe.beta,package:probe,file:beta.sql,resource_type:test'
    assert len(_selected_unique_ids(tmp_path, leaky, None)) == 2, 'eager no longer leaks; revisit this test'
    assert len(_selected_unique_ids(tmp_path, leaky, None, indirect_selection='empty')) == 1

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_interlocking_cross_model_tests_do_not_create_a_cycle(tmp_path):
    """
    Two multi-endpoint tests pointing at each other's downstream models.

    A gate is eligible only when every test endpoint is a strict ancestor. The rule respects dbt's
    topological order for the complete set of emitted edges, keeping this layout acyclic.
    """
    _write_project(
        tmp_path,
        {
            'a.sql': MODEL_SQL,
            'c.sql': MODEL_SQL,
            'n.sql': "select * from {{ ref('a') }}\n",
            'b.sql': "select * from {{ ref('c') }}\n",
        },
        schema_yml=(
            'models:\n'
            '  - name: a\n    columns:\n      - name: id\n        data_tests:\n'
            '          - relationships: {to: ref("b"), field: id}\n'
            '  - name: c\n    columns:\n      - name: id\n        data_tests:\n'
            '          - relationships: {to: ref("n"), field: id}\n'
            '  - name: n\n    columns:\n      - name: id\n'
            '  - name: b\n    columns:\n      - name: id\n'
        ),
    )
    manifest = _parse(tmp_path)

    for bundle_tests in (False, True):
        _assert_acyclic(manifest, bundle_tests)


def test_interlocking_tests_on_v_prefixed_models_do_not_create_a_cycle(tmp_path):
    """
    `test_interlocking_cross_model_tests_do_not_create_a_cycle` with every model renamed to start with `v`.

    Eligibility depends only on manifest ancestry, so ordinary names resembling version ids cannot alter
    which tests gate them.
    """
    _write_project(
        tmp_path,
        {
            'va.sql': MODEL_SQL,
            'vc.sql': MODEL_SQL,
            'vn.sql': "select * from {{ ref('va') }}\n",
            'vb.sql': "select * from {{ ref('vc') }}\n",
        },
        schema_yml=(
            'models:\n'
            '  - name: va\n    columns:\n      - name: id\n        data_tests:\n'
            '          - relationships: {to: ref("vb"), field: id}\n'
            '  - name: vc\n    columns:\n      - name: id\n        data_tests:\n'
            '          - relationships: {to: ref("vn"), field: id}\n'
            '  - name: vn\n    columns:\n      - name: id\n'
            '  - name: vb\n    columns:\n      - name: id\n'
        ),
    )
    manifest = _parse(tmp_path)

    for bundle_tests in (False, True):
        _assert_acyclic(manifest, bundle_tests)


def _cross_referencing_versioned_project(tmp_path, first: str = 'alpha', second: str = 'beta') -> dict:
    """Builds two versioned models whose later versions reference each other's earlier version."""
    _write_project(
        tmp_path,
        {
            f'{first}_v1.sql': MODEL_SQL,
            f'{first}_v2.sql': "select * from {{ ref('%s', v=1) }}\n" % second,
            f'{second}_v1.sql': MODEL_SQL,
            f'{second}_v2.sql': "select * from {{ ref('%s', v=1) }}\n" % first,
        },
        schema_yml=(
            'models:\n'
            f'  - name: {first}\n    latest_version: 2\n    columns:\n      - name: id\n'
            '    versions:\n      - v: 1\n      - v: 2\n'
            f'  - name: {second}\n    latest_version: 2\n    columns:\n      - name: id\n'
            '    versions:\n      - v: 1\n      - v: 2\n'
            'unit_tests:\n'
            f'  - name: ut_{first}\n    model: {first}\n    given: []\n    expect: {{rows: [{{id: 1}}]}}\n'
            f'  - name: ut_{second}\n    model: {second}\n    given: []\n    expect: {{rows: [{{id: 1}}]}}\n'
        ),
    )
    return _parse(tmp_path)


def test_cross_referencing_versioned_models_use_exact_clone_gates(tmp_path):
    """
    Two versioned models whose later versions reference each other's earlier version.

    Each later version is gated on the exact clone attached to the earlier version it references. The
    clone tasks depend only on those earlier versions, so both gates are safe at the first frontier.
    """
    manifest = _cross_referencing_versioned_project(tmp_path)

    graph = _assert_acyclic(manifest, bundle_tests=False)
    unit_by_parent = {
        next(dep for dep in dependencies if dep.endswith('_model')): task_key
        for task_key, dependencies in graph.items()
        if task_key.startswith('unit_test_')
    }
    assert unit_by_parent['beta_v1_model'] in graph['alpha_v2_model']
    assert unit_by_parent['alpha_v1_model'] in graph['beta_v2_model']
    _assert_acyclic(manifest, bundle_tests=True)


def test_a_cross_model_data_test_uses_the_safe_subset_rule(tmp_path):
    """
    A `relationships` test spanning a versioned model and a plain one follows the ordinary safe-subset
    rule. A node that is not downstream of both endpoints cannot be gated on that test.
    """
    _write_project(
        tmp_path,
        {
            'alpha_v1.sql': MODEL_SQL,
            'alpha_v2.sql': "select * from {{ ref('nn') }}\n",
            'xm.sql': "select * from {{ ref('alpha', v=1) }}\n",
            'nn.sql': "select * from {{ ref('xm') }}\n",
        },
        schema_yml=(
            'models:\n'
            '  - name: alpha\n    latest_version: 2\n    columns:\n      - name: id\n'
            '    versions:\n      - v: 1\n      - v: 2\n'
            '  - name: xm\n    columns:\n      - name: id\n        data_tests:\n'
            '          - relationships:\n              to: ref(\'alpha\', v=2)\n              field: id\n'
            '  - name: nn\n    columns:\n      - name: id\n'
        ),
    )
    manifest = _parse(tmp_path)

    graph = _assert_acyclic(manifest, bundle_tests=False)

    # And the edge is simply absent, as the subset rule always left it — `nn` is not downstream of
    # `alpha.v2`, so the test cannot gate it either way.
    assert not any(
        'relationships' in dep for dep in graph['nn_model']
    ), f'nn_model deps {sorted(graph["nn_model"])} include a cross-model test it is not downstream of'


def _cross_version_data_test_project(tmp_path) -> dict:
    """
    A `relationships` data test whose endpoints are two versions of one model.

    `nn` sits between the versions (`nn` refs `alpha.v1`, `alpha.v2` refs `nn`), so it is not downstream
    of the test's complete ref set. There is no unit test in this project.
    """
    _write_project(
        tmp_path,
        {
            'alpha_v1.sql': MODEL_SQL,
            'alpha_v2.sql': "select * from {{ ref('nn') }}\n",
            'nn.sql': "select * from {{ ref('alpha', v=1) }}\n",
        },
        schema_yml=(
            'models:\n'
            '  - name: alpha\n    latest_version: 2\n    columns:\n      - name: id\n'
            '    versions:\n'
            '      - v: 1\n        columns:\n          - name: id\n            data_tests:\n'
            '              - relationships:\n                  to: ref(\'alpha\', v=2)\n'
            '                  field: id\n'
            '      - v: 2\n'
            '  - name: nn\n    columns:\n      - name: id\n'
        ),
    )
    return _parse(tmp_path)


def test_a_cross_version_data_test_uses_the_safe_subset_rule(tmp_path):
    """
    A data test spanning two versions is eligible only after both exact versions are strict ancestors.
    `nn` is downstream of v1 but upstream of v2, so it remains ungated and the emitted graph stays acyclic.
    """
    manifest = _cross_version_data_test_project(tmp_path)

    assert not manifest['unit_tests'], 'fixture must contain no unit tests for this to test what it claims'
    graph = _assert_acyclic(manifest, bundle_tests=False)
    assert not any('relationships' in dep for dep in graph['nn_model'])
    _assert_acyclic(manifest, bundle_tests=True)


def test_a_v_named_model_does_not_pick_up_an_unrelated_models_test(tmp_path):
    """
    A downstream model is eligible only for tests whose complete ref set is among its strict ancestors.
    Naming models with a `v` prefix does not change that manifest-graph rule.
    """
    _write_project(
        tmp_path,
        {
            'vendors.sql': MODEL_SQL,
            'visits.sql': MODEL_SQL,
            'downstream.sql': "select * from {{ ref('vendors') }}\n",
        },
        schema_yml=(
            'models:\n'
            '  - name: vendors\n    columns:\n      - name: id\n'
            '  - name: visits\n    columns:\n      - name: id\n        data_tests:\n'
            '          - relationships:\n              to: ref(\'vendors\')\n              field: id\n'
            '  - name: downstream\n    columns:\n      - name: id\n'
        ),
    )
    manifest = _parse(tmp_path)

    # Guard the premise: none of these is a versioned model.
    assert all(info.get('version') is None for info in manifest['nodes'].values() if info['resource_type'] == 'model')

    tasks = create_dbt_factory(bundle_tests=False).create_tasks(manifest)
    deps = {t['task_key']: {d['task_key'] for d in (t.get('depends_on') or [])} for t in tasks}

    assert deps['downstream_model'] == {'vendors_model'}, (
        f'downstream_model deps {sorted(deps["downstream_model"])} include a test of `visits`, which it '
        f'does not depend on'
    )


def _random_gating_project(rng: random.Random) -> tuple[dict[str, str], str]:
    """
    A randomised project of plain and versioned models wired with refs and multi-endpoint tests.

    Aimed at the gating graph rather than at selectors, so it draws the ingredients that produce gate
    edges: `ref()`s between models (which make ancestors), `relationships` tests (whose refs span two
    models, the shape the safe-subset rule judges), and versioned models with unit tests. Half the names
    begin with `v` so ordinary identifiers resembling version segments remain covered.
    """
    names = ['va', 'vb', 'orders', 'items'][: rng.randint(2, 4)]
    versioned = {name for name in names if rng.random() < 0.4}

    files: dict[str, str] = {}
    model_entries: list[str] = []
    unit_tests: list[str] = []
    # Every ref target must already be selectable, so refs only point at previously emitted models.
    emitted: list[tuple[str, bool]] = []

    for name in names:

        def ref_to_earlier() -> str:
            if not emitted or rng.random() < 0.3:
                return MODEL_SQL
            target, target_versioned = rng.choice(emitted)
            if target_versioned:
                return "select * from {{ ref('%s', v=1) }}\n" % target
            return "select * from {{ ref('%s') }}\n" % target

        if name in versioned:
            files[f'{name}_v1.sql'] = ref_to_earlier()
            files[f'{name}_v2.sql'] = ref_to_earlier()
            model_entries.append(
                f'  - name: {name}\n    latest_version: 2\n    columns:\n      - name: id\n'
                f'    versions:\n      - v: 1\n      - v: 2\n'
            )
            if rng.random() < 0.7:
                unit_tests.append(
                    f'  - name: ut_{name}\n    model: {name}\n    given: []\n    expect: {{rows: [{{id: 1}}]}}\n'
                )
        else:
            files[f'{name}.sql'] = ref_to_earlier()
            model_entries.append(f'  - name: {name}\n    columns:\n      - name: id\n')
        emitted.append((name, name in versioned))

    # `relationships` tests come last so both endpoints exist. They are what the subset rule is for: a
    # test whose refs span two models is only safe to gate a node downstream of both.
    for index, entry in enumerate(model_entries):
        other = rng.choice(names)
        if names[index] == other or rng.random() < 0.5:
            continue
        target = f"ref('{other}', v=1)" if other in versioned else f"ref('{other}')"
        # Block style, not `{to: ..., field: id}`: the comma inside a versioned `ref('x', v=1)` is a
        # separator in YAML flow style, which dbt then rejects as a keyword argument to the test macro.
        model_entries[index] = entry.replace(
            '      - name: id\n',
            '      - name: id\n        data_tests:\n'
            f'          - relationships:\n              to: {target}\n              field: id\n',
            1,
        )

    schema = 'models:\n' + ''.join(model_entries)
    if unit_tests:
        schema += 'unit_tests:\n' + ''.join(unit_tests)
    return files, schema


@pytest.mark.parametrize('seed', range(12))
def test_random_gating_layouts_never_emit_a_cycle(tmp_path, seed):
    """
    Randomised ref/test/version wiring asserts that the safe-subset and first-frontier rules always emit
    an acyclic `depends_on` graph in both modes. Gating strength is covered separately by
    `test_a_downstream_model_is_gated_on_the_exact_versioned_unit_test`.
    """
    files, schema = _random_gating_project(random.Random(seed))
    _write_project(tmp_path, files, schema_yml=schema)
    manifest = _parse(tmp_path)

    for bundle_tests in (False, True):
        _assert_acyclic(manifest, bundle_tests)
