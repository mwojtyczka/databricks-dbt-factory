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

import functools
import json
import random
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

    Without this, an assertion can only count what a selector resolved to, not check *which* node it
    found — and "resolves to one node" is satisfied just as well by the wrong node. Replacing
    `_node_select` with a constant selector used to leave the whole suite green for that reason.

    Built by re-deriving the task key from each id the same way the factory does, rather than by
    parsing the selector, so the check is independent of the thing under test.

    In bundled mode the tested resources are passed as `bundled_test_ids` so their `<resource>_test` keys
    are mapped too. An earlier revision passed `[]`, leaving `bundled_test_keys` empty and every bundled
    test task silently unchecked — the `if bundle_tests:` branch could not fire, so the
    `bundle_tests=True` parametrisations only ever exercised the model and seed tasks.
    """
    nodes = {**manifest.get('nodes', {}), **manifest.get('unit_tests', {})}
    ids = [full_name for full_name, info in nodes.items() if (info.get('config') or {}).get('enabled') is not False]
    tested = _tested_resources(manifest, ids) if bundle_tests else set()
    task_keys, bundled_test_keys = build_task_key_maps(sorted(ids), sorted(tested))
    mapping = {key: full_name for full_name, key in task_keys.items()}
    if bundle_tests:
        mapping.update({key: full_name for full_name, key in bundled_test_keys.items()})
    return mapping


def _tested_resources(manifest: dict, ids: list[str]) -> set[str]:
    """The resources that receive a bundled `<resource>_test` task: anything a test depends on."""
    nodes = {**manifest.get('nodes', {}), **manifest.get('unit_tests', {})}
    tested: set[str] = set()
    for full_name in ids:
        info = nodes[full_name]
        if (info.get('resource_type') or '') not in ('test', 'unit_test'):
            continue
        for dep in info.get('depends_on', {}).get('nodes', []):
            if dep in nodes or dep in manifest.get('sources', {}):
                tested.add(dep)
    return tested


def _assert_each_task_selects_its_own_node(tmp_path: Path, manifest: dict, bundle_tests: bool) -> None:
    """
    Asserts every resource task resolves to exactly the node it is named for.

    This is the check the suite was missing: `len(selected) == 1` passes just as happily when a task
    addresses the *wrong* node, so a constant selector substituted for `_node_select` went undetected.
    Comparing against the expected name closes that hole.

    `--indirect-selection empty` isolates the selector from dbt's eager expansion, which would
    otherwise add a selected model's attached tests and obscure what the selector itself matched.
    """
    expected_by_key = _task_key_to_unique_id(manifest, bundle_tests)
    unmapped = []
    for task_key, select, _verb in _resource_selectors(manifest, bundle_tests):
        unique_id = expected_by_key.get(task_key)
        if unique_id is None:
            unmapped.append(task_key)
            continue
        # Compare against the *manifest unique id*, not the name `dbt ls` displays. Two generic tests can
        # share a display name while separate files keep each selector individually addressable, so a
        # name-based assertion would be satisfied by pointing task A at task B's node.
        selected = _selected_unique_ids(tmp_path, select, None, indirect_selection='empty')
        assert selected == (unique_id,), (
            f'{task_key} selects {selected} via {select!r}, expected exactly ({unique_id!r},) — '
            f'the node it is named for'
        )
    # No task may go unchecked: an unmapped key means this helper has drifted from the factory's keying,
    # which is how bundled test tasks previously escaped the assertion entirely.
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


def test_bundled_test_task_sweeps_only_its_own_resources_tests(tmp_path):
    """
    A bundled `<model>_test` task must pick up its own model's tests and none of the nested
    sibling's, which the prefix behaviour would otherwise sweep in.
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
    selected = _selected_ids(tmp_path, orders_select, resource_type=None, indirect=True)

    # A schema test's fqn is [package, <test name>] — the models/ subdirectory is not part of it.
    assert 'probe.unique_items_id' not in selected, f'orders_test swept in the sibling model tests: {selected}'
    assert 'probe.unique_orders_id' in selected
    assert 'probe.not_null_orders_id' in selected
    assert 'probe.marts.orders.items' not in selected, f'orders_test would build the sibling model: {selected}'


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
    """A source's tests are selected by `source:<pkg>.<src>.<table>`; that must resolve exactly."""
    _write_project(tmp_path, {'downstream.sql': MODEL_SQL})
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

    selectors = [s for _, s, verb in _resource_selectors(manifest, bundle_tests=True) if verb == 'test']
    source_selectors = [s for s in selectors if s.startswith('source:')]
    assert source_selectors, f'expected a bundled source test task, got {selectors}'
    for select in source_selectors:
        selected = _selected_ids(tmp_path, select, 'test', indirect=True)
        assert len(selected) == 1, f'{select!r} selects {selected}, expected exactly one test'


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


def test_source_with_a_trailing_graph_operator_is_refused(tmp_path):
    """
    The whole `source:...` string is one raw selector, so a table ending in `+N` is read as a graph
    operator: `source:probe.raw.orders+1` matches nothing while `dbt test` still exits 0, so the
    source's tests would silently never run. Generation must refuse.
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

    with pytest.raises(ValueError, match='Cannot generate a task for'):
        _resource_selectors(manifest, bundle_tests=True)


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

    source_selectors = [s for _, s, verb in _resource_selectors(manifest, bundle_tests=True) if s.startswith('source:')]
    assert source_selectors, 'expected a bundled source test task'
    for select in source_selectors:
        assert _selected_ids(tmp_path, select, 'test', indirect=True), f'{select!r} matched nothing'


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
def test_dotted_source_part_is_refused_rather_than_emitted(tmp_path, source_name, table):
    """
    `.` delimits dbt's source grammar, which takes at most `pkg.source.table`. A dot inside one part
    makes four, and dbt rejects the selector with a Runtime Error rather than selecting nothing — so
    the bundled task would fail at run time. Generation must refuse instead.

    Asserted from both ends: dbt really does reject the naive string, and the factory really does
    refuse to emit it.
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

    with pytest.raises(ValueError, match='Cannot generate a task for'):
        _resource_selectors(manifest, bundle_tests=True)


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


def test_fqn_prefix_collision_between_sibling_tests_is_refused(tmp_path):
    """
    A test named `check.nested` flattens to `[probe, check, nested]`, so the sibling `check`'s selector
    `probe.check` matches it as a subtree parent.

    Confirmed on dbt 1.12.0 with every discriminator present and identical — the collision does not
    depend on any term being dropped:

        probe.check,package:probe,file:schema.yml,test_name:not_null
          -> ('test.probe.check.d0dfa850a3', 'test.probe.check.nested.484de86d57')

    and nothing narrows it: `resource_type:test`, `fqn:probe.check` and all four terms together each
    return both. So `check` must be refused. Without the refusal its task, which depends only on
    `check_model`, would run `other`'s test before `other_model` had built `other`.
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

    with pytest.raises(ValueError, match='also runs'):
        _resource_selectors(manifest, bundle_tests=False)


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
        {'orders.sql': MODEL_SQL, 'customers.sql': MODEL_SQL},
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
            '    given: []\n'
            '    expect: {rows: [{id: 1}]}\n'
        ),
    )
    manifest = _parse(tmp_path)

    # Without the resource-type term the two are indistinguishable.
    both = _selected_ids(tmp_path, 'probe.orders.unit_orders,package:probe,file:schema.yml', None)
    assert len(both) == 2, f'expected the flattened fqns to collide, got {both}'

    _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=False)


def test_versioned_unit_test_clones_share_one_task(tmp_path):
    """
    dbt clones a unit test once per model version, rewriting only `unique_id`, `depends_on.nodes[0]` and
    `version` — the fqn, name and file are identical. No selector separates them: on dbt 1.12.0
    `version:` accepts only `latest`/`prerelease`/`old`/`none`, none of which match a unit test.

    An earlier revision emitted one task per clone, giving both the same selector, so each ran every
    version's assertions while its name claimed one. One task for the group is the honest description,
    and it must wait for every version's model.
    """
    _write_project(
        tmp_path,
        {'orders_v1.sql': MODEL_SQL, 'orders_v2.sql': MODEL_SQL},
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
    manifest = _parse(tmp_path)

    # Guard the fixture: if dbt stops cloning with an identical fqn, this test proves nothing.
    clones = [info for info in manifest['unit_tests'].values() if info['name'] == 'ut_orders']
    assert len(clones) == 2, f'expected dbt to clone the unit test per version, got {len(clones)}'
    assert clones[0]['fqn'] == clones[1]['fqn'], 'dbt now varies the fqn per version; revisit the grouping'

    tasks = create_dbt_factory(bundle_tests=False).create_tasks(manifest)
    unit_tasks = [task for task in tasks if 'unit_test' in task['task_key']]
    assert len(unit_tasks) == 1, f'expected the clones to share one task, got {[t["task_key"] for t in unit_tasks]}'
    assert {dep['task_key'] for dep in unit_tasks[0]['depends_on']} == {'orders_v1_model', 'orders_v2_model'}

    # The shared selector really does resolve to both clones, which is why one task is correct.
    command = shlex.split(unit_tasks[0]['dbt_task']['commands'][-1])
    select = command[command.index('--select') + 1]
    assert len(_selected_ids(tmp_path, select, None, indirect_selection='empty')) == 2


def test_duplicate_test_names_sharing_an_fqn_are_refused(tmp_path):
    """
    dbt does not require generic-test names to be unique — it disambiguates in the `unique_id` hash only
    — so two models each carrying `not_null: {name: check_id}` produce two test nodes with the *same*
    fqn, name, file and test type. Confirmed on dbt 1.12.0: one selector, two nodes.

    Distinct from the prefix collision: the fqns are equal, not prefix-related, so a prefix check alone
    would miss it. Both must be refused.
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

    with pytest.raises(ValueError, match='also runs'):
        _resource_selectors(manifest, bundle_tests=False)


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


def _assert_acyclic(manifest, bundle_tests):
    """
    Fails if any emitted task reaches itself through `depends_on`, which Databricks rejects at deploy.

    Acyclicity is necessary but not sufficient: dropping *every* gate edge also satisfies it, so each
    caller is paired with an assertion that the gates which should survive do — see
    `test_a_downstream_model_is_still_gated_on_a_versioned_unit_test` and
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


def test_versioned_unit_test_group_does_not_create_a_dependency_cycle(tmp_path):
    """
    The clones' shared task waits for every version's model, so its refs must be visible to the
    cycle guard in `_extend_deps_with_upstream_tests` *before* that guard runs.

    Merging them only at task-build time left `_index_tests_by_resource` holding the representative's
    unmerged refs, so the guard still judged it safe to gate `orders.v2` on the group — producing
    `orders_v2_model -> unit_test_..._v1 -> orders_v2_model`, a cycle Databricks rejects at deploy.
    Reached whenever a later model version depends on an earlier one and the model has a unit test.
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


def test_a_downstream_model_is_still_gated_on_a_versioned_unit_test(tmp_path):
    """
    The companion to the cycle test above, and the reason acyclicity alone is not enough: a graph with
    *every* gating edge removed is also acyclic, so that check passes while the quality gate is gone.

    The shared unit-test task waits for both version models, so its refs are `{v1, v2}`. An earlier
    revision required every ref of a test to be an ancestor of the node being gated, which `consumer` —
    referencing only v1 — fails. The edge was dropped and a failing v1 assertion no longer blocked
    `consumer`, contrary to the documented gating behaviour. The guard now tests the cycle condition
    directly, so the gate survives.
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

    assert any(key in by_key['consumer_model'] for key in unit_keys), (
        f'consumer_model deps {sorted(by_key["consumer_model"])} include no unit-test task, so a failing '
        f'v1 assertion would not block it'
    )


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


def test_file_stem_collision_is_refused(tmp_path):
    """
    dbt's `FileSelectorMethod` matches the base name *or* its stem, so `file:a.yml` also matches a node
    declared in `a.yml.yml`. Mirroring only the base name let that collision past the exactness check:
    the `a.yml` task's selector resolved to both tests, running the second before its model was built.
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

    both = _selected_ids(tmp_path, 'probe.chk,package:probe,file:a.yml,resource_type:test,test_name:not_null', None)
    assert len(both) == 2, f'expected the stem match to collide, got {both}'
    with pytest.raises(ValueError, match='also runs'):
        _resource_selectors(manifest, bundle_tests=False)


def test_singular_test_named_after_a_model_without_other_tests_is_kept(tmp_path):
    """
    Reaching the model is not itself a problem: `resource_type:test` keeps dbt from building it, and with
    no other tests on the model the selector resolves to exactly the singular test — confirmed with
    `dbt ls` on dbt 1.12.0. Only the model's *attached* tests leak under eager selection, so refusing this
    layout would reject a project dbt handles. The refusal case is
    `test_singular_test_sharing_a_models_fqn_is_refused`, where the model does carry a test.
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

    Each task is replayed with the exact flags its own command carries — the verb's resource type and its
    `--indirect-selection` mode — so the comparison is against what that task will really run. Bundled
    `<resource>_test` tasks are skipped: sweeping a resource's whole test set with `cautious` is their
    purpose rather than a defect.
    """
    peers = {
        **{k: v for k, v in manifest['nodes'].items() if (v.get('config') or {}).get('enabled') is not False},
        **manifest.get('unit_tests', {}),
        **manifest.get('sources', {}),
    }
    index = DbtFactory._selector_index(peers)  # pylint: disable=protected-access
    for task in create_dbt_factory(bundle_tests=bundle_tests).create_tasks(manifest):
        command = shlex.split(task['dbt_task']['commands'][-1])
        verb = command[1]
        select = command[command.index('--select') + 1]
        if select.startswith('source:'):
            continue
        mode = command[command.index('--indirect-selection') + 1] if '--indirect-selection' in command else None
        if mode == 'cautious':  # a bundled sweep, deliberately multi-node
            continue
        predicted = set(DbtFactory._matching_ids(select, index))  # pylint: disable=protected-access
        resource_type = None if verb == 'test' else {'run': 'model', 'seed': 'seed', 'snapshot': 'snapshot'}[verb]
        # Compare on unique ids: `_selected_ids` returns the *display* names `dbt ls` prints, which do not
        # match manifest keys, so intersecting those with `peers` would silently compare against nothing.
        actual = set(_selected_unique_ids(tmp_path, select, resource_type, indirect_selection=mode)) & set(peers)
        assert (
            predicted == actual
        ), f'{task["task_key"]}: model predicts {sorted(predicted)} but dbt runs {sorted(actual)} for {select!r}'


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

    A gate edge is only safe to add when it respects the dbt graph's topological order, which the subset
    rule enforces. Testing the cycle condition per edge instead is sound in isolation but not for a *set*
    of edges: `ancestors_by_node` describes the dbt graph while the edges added are task edges, so once
    several gates exist the reachability consulted no longer matches the graph being built.

    This layout closes the loop under that weaker rule — verified on dbt 1.12.0, where it produced
    `b_model -> relationships_c... -> n_model -> relationships_a... -> b_model`, which Databricks rejects
    at deploy. `test_a_downstream_model_is_still_gated_on_a_versioned_unit_test` is the companion: acyclic
    alone is satisfied by dropping every gate, so both properties need asserting.
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

    The version-sibling exemption to the subset rule was matched with substring tests — `'.v' in ref` and
    `startswith(f'{stem}.v')` — rather than by checking the final fqn segment is a real version. So for
    `model.probe.vendors`, `'.v'` matched inside `.vendors` and any ancestor under `model.probe.v*`
    counted as a "version sibling", handing ordinary non-versioned models the relaxed rule and reopening
    exactly the loop the subset rule prevents. Verified on dbt 1.12.0: this produced
    `vn_model -> relationships_va... -> vb_model -> relationships_vc... -> vn_model`.
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
    """
    Two versioned models whose later versions reference each other's earlier version.

    `first`/`second` name the models so a caller can vary only their *alphabetical order*, which is what
    decided which model lost its gate back when this layout was resolved by dropping an edge.
    """
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


def test_cross_referencing_versioned_models_are_refused(tmp_path):
    """
    Two versioned models whose later versions reference each other's earlier version.

    Each model's shared unit-test task waits for both of its versions, so the two gates together close a
    loop — `alpha_v2_model -> unit_test...beta_v1 -> beta_v2_model -> unit_test...alpha_v1 ->
    alpha_v2_model`, verified on dbt 1.12.0.

    Dropping one of the two edges also restores acyclicity, and must not be the answer: the dropped gate is
    real, so `beta_v2_model` would build with a failing `ut_beta` assertion and nothing said so. Generation
    refuses instead, as `_ambiguous` does for a selector it cannot prove exact.
    """
    manifest = _cross_referencing_versioned_project(tmp_path)

    with pytest.raises(ValueError, match='Cannot generate a gate'):
        create_dbt_factory(bundle_tests=False).create_tasks(manifest)


def test_the_refusal_does_not_depend_on_model_naming(tmp_path):
    """
    The same layout with the models renamed so their alphabetical order flips.

    Candidates are considered in sorted order, so resolving this by dropping an edge would let a model's
    *name* decide which one goes ungated: renaming `alpha` to `zeta` moves the loss from `beta_v2` to
    `zeta_v2`. The refusal is symmetric, and both spellings must produce it.
    """
    manifest = _cross_referencing_versioned_project(tmp_path, first='zeta', second='beta')

    with pytest.raises(ValueError, match='Cannot generate a gate'):
        create_dbt_factory(bundle_tests=False).create_tasks(manifest)


def test_bundling_handles_the_cross_referencing_versioned_layout(tmp_path):
    """
    The refusal above is specific to per-test mode, and bundling is a real way out of it.

    Bundle mode gates each model on the upstream's `<resource>_test` task rather than on a unit-test task
    shared across versions, so no candidate edge arises and both models keep their gate with no cycle.
    Asserted here so the remedy the refusal message offers is known to work, and because
    `--indirect-selection` changes what a selector resolves to — per AGENTS.md, both modes get checked.
    """
    manifest = _cross_referencing_versioned_project(tmp_path)

    graph = _assert_acyclic(manifest, bundle_tests=True)

    for model in ('alpha_v2_model', 'beta_v2_model'):
        assert any(dep.endswith('_test') for dep in graph[model]), (
            f'{model} deps {sorted(graph[model])} include no test task, so bundling is not the '
            f'workaround the refusal message claims'
        )


def test_a_cross_model_data_test_is_not_treated_as_a_version_group_test(tmp_path):
    """
    A `relationships` test spanning a versioned model and a plain one must not reach the version-sibling
    exemption, which exists only for *"a test shared by the versions of a single model."*

    Checking only that the test's *unsatisfied* refs are version siblings admits it, since one endpoint is a
    versioned model — and then generation refuses a project that is not the exemption's target at all. It
    must fall through to the plain subset rule, which drops the edge; verified on dbt 1.12.0.
    `test_cross_referencing_versioned_models_are_refused` covers the shape that *should* refuse.
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

    # Generation must succeed: this is not the layout the refusal is for.
    graph = _assert_acyclic(manifest, bundle_tests=False)

    # And the edge is simply absent, as the subset rule always left it — `nn` is not downstream of
    # `alpha.v2`, so the test cannot gate it either way.
    assert not any(
        'relationships' in dep for dep in graph['nn_model']
    ), f'nn_model deps {sorted(graph["nn_model"])} include a cross-model test it is not downstream of'


def _single_version_group_data_test_project(tmp_path) -> dict:
    """
    A `relationships` data test whose *both* endpoints are versions of one model, wired so gating cycles.

    `nn` sits between the two versions (`nn` refs `alpha.v1`, `alpha.v2` refs `nn`), so the test — which
    waits for both versions — transitively waits for `nn`. There is no unit test anywhere in this project.
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


@pytest.mark.parametrize(
    'project, ungated_task',
    [
        (_cross_referencing_versioned_project, 'beta_v2_model'),
        (_single_version_group_data_test_project, 'nn_model'),
    ],
    ids=['unit-test-group', 'data-test-group'],
)
def test_the_refusal_claims_no_more_than_it_established(tmp_path, project, ungated_task):
    """
    The refusal identifies the two tasks and stops. The check establishes exactly one fact — adding this
    edge closes a loop — so anything past that is inference, and both fixtures here are counterexamples to
    the obvious guesses.

    `data-test-group` contains no unit test at all, so naming one misdirects the reader; neither fixture is
    a pair of models referencing *each other*; and in `unit-test-group` the test does not depend on the
    gated task in the dbt project — `ut_alpha` refs only `alpha.v1`, and the reachability comes from a
    sibling gate added moments earlier, so telling the reader to remove that dependency points at nothing.
    Each phrase is pinned so it cannot come back.
    """
    manifest = project(tmp_path)

    with pytest.raises(ValueError) as raised:
        create_dbt_factory(bundle_tests=False).create_tasks(manifest)

    message = str(raised.value)
    assert ungated_task in message, f'the message does not name the task it refused to gate: {message}'
    assert '--bundle-tests' in message, f'the message must name a working remedy: {message}'
    for unfounded in ('unit test', 'each other', 'already waits for'):
        assert unfounded not in message, f'message asserts {unfounded!r}, which it has not established: {message}'


def test_a_single_version_group_data_test_that_cycles_is_refused(tmp_path):
    """
    A data test confined to one version group reaches the version-sibling exemption too.

    `_covers_one_version_group` admits any test whose refs are all versions of one model, which is right —
    such a test does gate a node downstream of those versions, and the benign case correctly gains that
    gate. So the exemption is not unit-test-only, and this layout refuses for the same reason a shared unit
    test does. The README describes the refusal by its condition rather than by a single layout because of
    this second shape.
    """
    manifest = _single_version_group_data_test_project(tmp_path)

    assert not manifest['unit_tests'], 'fixture must contain no unit tests for this to test what it claims'

    with pytest.raises(ValueError, match='Cannot generate a gate'):
        create_dbt_factory(bundle_tests=False).create_tasks(manifest)

    # The advertised remedy has to at least *generate*. It is weaker than it looks here: this test spans
    # two resources, so bundling emits it as a standalone task that gates nothing (`--indirect-selection
    # cautious` keeps a multi-endpoint test out of the per-resource bundles). So the escape hatch is real
    # but trades the gate away — which is why the message says bundling "does not create this edge" rather
    # than claiming it preserves the gate.
    graph = _assert_acyclic(manifest, bundle_tests=True)
    assert 'relationships_alpha_v1_id__id__ref_alpha_v_2__test' in graph, 'the test task should still exist'


def test_a_v_named_model_does_not_pick_up_an_unrelated_models_test(tmp_path):
    """
    The other half of the version-sibling substring bug, and the half acyclicity cannot see.

    `_version_sibling_of_any` compared ids with `'.v' in ref` and `startswith(f'{stem}.v')`, so for
    `model.probe.vendors` the `'.v'` matched inside `.vendors` and every `model.probe.v*` counted as a
    version sibling. Besides closing cycles, that relaxation adds gate edges that are merely *wrong*:
    here `downstream` refs only `vendors`, yet it waited on a `relationships` test of `visits` — a model
    it has no dependency on. That edge does not close a loop, so the cycle tests pass with the bug still
    present; only an assertion on the exact deps catches it.

    Nothing about these models is versioned — dbt reports `version: None` for all three on dbt 1.12.0 —
    so the exemption should never have been consulted at all.
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
        f'does not depend on — the version-sibling exemption was applied to non-versioned models'
    )


def _random_gating_project(rng: random.Random) -> tuple[dict[str, str], str]:
    """
    A randomised project of plain and versioned models wired with refs and multi-endpoint tests.

    Aimed at the gating graph rather than at selectors, so it draws the ingredients that produce gate
    edges: `ref()`s between models (which make ancestors), `relationships` tests (whose refs span two
    models, the shape the subset rule judges), and versioned models with unit tests (the one exemption to
    that rule). Half the names begin with `v` because that is what distinguished a version segment from an
    ordinary name in the substring bug.
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
    The generative counterpart to the three enumerated cycle fixtures.

    Every per-edge rule tried here was sound for the layouts someone thought to write down and wrong for
    one nobody had — three rounds, three fresh cycles. So the property is asserted over randomised
    ref/test/version wiring as well: whatever dbt parses, the emitted `depends_on` must be acyclic, in both
    modes. A cycle is not a cosmetic defect — Databricks rejects the job at deploy.

    Gating strength is asserted separately, by `test_a_downstream_model_is_still_gated_on_a_versioned_unit_test`:
    a graph with every gate edge dropped is acyclic too, so this test alone cannot catch over-refusal.
    """
    files, schema = _random_gating_project(random.Random(seed))
    _write_project(tmp_path, files, schema_yml=schema)
    manifest = _parse(tmp_path)

    for bundle_tests in (False, True):
        _assert_acyclic(manifest, bundle_tests)
