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
import itertools
import json
import random
import shlex
from pathlib import Path

import pytest
from dbt.cli.main import dbtRunner, dbtRunnerResult

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

    for task_key, select, verb in _resource_selectors(manifest, bundle_tests):
        resource_type = {'run': 'model', 'seed': 'seed', 'snapshot': 'snapshot'}.get(verb)
        if resource_type is None:  # a test task; covered by the shared-schema test below
            continue
        selected = _selected_ids(tmp_path, select, resource_type, indirect=False)
        assert len(selected) == 1, f'{task_key} selects {selected} via {select!r}, expected exactly one node'


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

    for task_key, select, verb in _resource_selectors(manifest, bundle_tests=False):
        if verb != 'run':
            continue
        selected = _selected_ids(tmp_path, select, 'model', indirect=False)
        assert len(selected) == 1, f'{task_key} selects {selected} via {select!r}, expected exactly one node'


@pytest.mark.parametrize(
    ('file_name', 'note'),
    [
        pytest.param('orders+1.sql', 'a trailing +N is read as child depth', id='numeric-graph-operator'),
        pytest.param('+leading.sql', 'an operator inside a segment is harmless', id='embedded-operator'),
        pytest.param("customer's.sql", 'a quote breaks shlex in the notebook runner', id='apostrophe'),
    ],
)
def test_awkward_file_names_still_resolve_to_one_node(tmp_path, file_name, note):
    """
    Names that trip dbt's selector grammar or the notebook runner's tokenisation must still address
    exactly one node — by dropping only the unusable term. (`note` records what each name trips.)
    """
    assert note
    _write_project(tmp_path, {file_name: MODEL_SQL, 'orders.sql': MODEL_SQL})
    manifest = _parse(tmp_path)

    for task_key, select, verb in _resource_selectors(manifest, bundle_tests=False):
        if verb != 'run':
            continue
        selected = _selected_ids(tmp_path, select, 'model', indirect=False)
        assert len(selected) == 1, f'{task_key} selects {selected} via {select!r}, expected exactly one node'


def test_model_and_singular_test_sharing_an_fqn_both_resolve(tmp_path):
    """
    `models/beta.sql` and `tests/beta.sql` parse with the same fqn and base name, but each task
    carries its own verb and dbt's resource-type filtering keeps them apart — so both are exact and
    neither should be refused.
    """
    _write_project(tmp_path, {'beta.sql': MODEL_SQL})
    tests_dir = tmp_path / 'tests'
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / 'beta.sql').write_text('select 1 as id where false\n', encoding='utf-8')
    manifest = _parse(tmp_path)

    for task_key, select, verb in _resource_selectors(manifest, bundle_tests=False):
        resource_type = 'model' if verb == 'run' else 'test'
        selected = _selected_ids(tmp_path, select, resource_type, indirect=False)
        assert len(selected) == 1, f'{task_key} selects {selected} via {select!r}, expected exactly one node'


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


def test_leading_numeric_graph_operator_in_a_name_is_refused(tmp_path):
    """
    dbt reads a leading `N+` as parent depth, not just a trailing `+N`, so a bare name of `2+check`
    is not the node's name at all — it means "check, plus two levels of its parents".

    Reaching this needs a name that is *also* stuck sharing a file, since a resource owning its file
    is addressed by `package:`+`file:` regardless of its name. A custom test name in a `schema.yml`
    under a spacey directory is that shape: the space kills the fqn, the leading `2+` kills the name,
    and the shared file kills `file:` — so generation must refuse.
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

    # dbt's own reading of the name we would have fallen back to: not this node.
    assert not _selected_ids(tmp_path, '2+check', 'test', indirect=False)

    with pytest.raises(ValueError, match='no selector can address'):
        _resource_selectors(manifest, bundle_tests=False)


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

    with pytest.raises(ValueError, match='no selector can address'):
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

    with pytest.raises(ValueError, match='no selector can address'):
        _resource_selectors(manifest, bundle_tests=False)


def test_single_resource_file_addresses_it_without_a_usable_name(tmp_path):
    """
    The counterpart: an unusable name costs nothing when the resource has its file to itself, since
    `package:`+`file:` then resolves to exactly one node. The refusal above must not generalise to
    this, or an awkward file name would fail a project dbt builds fine.
    """
    _write_project(tmp_path, {'my dir/2+orders.sql': MODEL_SQL, 'my dir/orders.sql': MODEL_SQL})
    manifest = _parse(tmp_path)

    for task_key, select, _verb in _resource_selectors(manifest, bundle_tests=False):
        selected = _selected_ids(tmp_path, select, 'model', indirect=False)
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

    with pytest.raises(ValueError, match='no selector can address'):
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
        selectors = _resource_selectors(manifest, bundle_tests)
        for task_key, select, verb in selectors:
            resource_type = {'run': 'model', 'seed': 'seed', 'snapshot': 'snapshot'}.get(verb)
            if resource_type is None:
                continue
            selected = _selected_ids(tmp_path, select, resource_type, indirect=False)
            assert len(selected) == 1, (
                f'layout {sorted(layout)} task {task_key} selects {selected} via {select!r}; '
                'a resource task must build exactly one node'
            )


def test_every_regression_layout_is_parsable_by_dbt():
    """
    Guards the fixtures themselves: a layout dbt cannot parse would make the tests above vacuous.
    Kept cheap by checking only that the pools cannot produce a duplicate resource name.
    """
    for name, directory in itertools.product(_NAME_POOL, _DIR_POOL):
        assert not name.startswith('/'), f'{directory}{name} would escape the models directory'
