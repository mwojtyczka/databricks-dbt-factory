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
def _selected_unique_ids(root: Path, select: str, resource_type: str | None) -> tuple[str, ...]:
    """
    The *unique ids* dbt resolves `select` to, rather than the display names `_selected_ids` returns.

    `dbt ls` prints selector names by default (`probe.orders`), which do not match manifest keys — so a
    comparison against manifest ids has to ask for `--output json --output-keys unique_id` instead.
    """
    args = ['ls', '--quiet', '--select', select, '--output', 'json', '--output-keys', 'unique_id']
    if resource_type:
        args += ['--resource-type', resource_type]
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


def _dbt_ls_name(unique_id: str, manifest: dict) -> str:
    """
    Translates a manifest id into the name `dbt ls` prints for it.

    `dbt ls` reports a *selector name* rather than a unique id — `probe.orders` for a model, and
    `unit_test:probe.ut_orders` for a unit test — so an id-based expectation has to be converted before
    it can be compared. Derived from the node's own fqn, which is what dbt joins.
    """
    info = {**manifest.get('nodes', {}), **manifest.get('unit_tests', {})}[unique_id]
    if (info.get('resource_type') or '') == 'unit_test':
        return f"unit_test:{info['package_name']}.{info['name']}"
    return '.'.join(info['fqn'])


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
        selected = _selected_ids(tmp_path, select, resource_type=None, indirect_selection='empty')
        expected = _dbt_ls_name(unique_id, manifest)
        # A bundled `<resource>_test` task deliberately runs the resource's whole test set, so its
        # selector addresses the *resource*; `--indirect-selection empty` reduces that to the resource
        # itself, which is what makes the same equality check valid for both kinds of task.
        assert selected == (expected,), (
            f'{task_key} selects {selected} via {select!r}, expected exactly ({expected!r},) — '
            f'the node it is named for ({unique_id})'
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


def test_singular_test_sharing_a_models_fqn_is_refused(tmp_path):
    """
    `models/beta.sql` and `tests/beta.sql` parse with the same fqn and base name. An earlier revision
    emitted both, reasoning that each task's verb plus dbt's resource-type filtering keeps them apart.

    It does not, and the earlier version of this test could not see why: its fixture had no *attached*
    tests, so nothing was there to be swept in. dbt intersects `resource_type:` only after expanding
    indirect selection, so under the default eager mode the singular test's selector also matches the
    model and therefore pulls in the model's own attached tests. Two consequences, both confirmed with
    `dbt ls` on dbt 1.12.0 for the layout below:

        probe.beta,package:probe,file:beta.sql,resource_type:test
          -> ('test.probe.beta', 'test.probe.not_null_beta_id.13481bf6b3')

    `not_null_beta_id` runs twice, and it runs inside `beta_test`, whose only upstream is `gamma_model`
    — so it asserts on `beta` before `beta_model` has built it. No term dbt offers separates them, so
    the singular test must be refused.

    It is the *attached* test leaking that makes this unaddressable, not reaching the model: without
    `not_null` on `beta` the same layout is fine, which
    `test_singular_test_named_after_a_model_without_other_tests_is_kept` pins.
    """
    _write_project(
        tmp_path,
        {'beta.sql': MODEL_SQL, 'gamma.sql': MODEL_SQL},
        schema_yml=(
            'models:\n'
            '  - name: beta\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests: [not_null]\n'
            '  - name: gamma\n'
            '    columns:\n'
            '      - name: id\n'
            '        data_tests: [not_null]\n'
        ),
    )
    tests_dir = tmp_path / 'tests'
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / 'beta.sql').write_text("select * from {{ ref('gamma') }} where id is null\n", encoding='utf-8')
    manifest = _parse(tmp_path)

    # dbt's own verdict on the selector we would otherwise have emitted: two tests, not one.
    swept = _selected_ids(tmp_path, 'probe.beta,package:probe,file:beta.sql,resource_type:test', None)
    assert len(swept) == 2, f'expected the singular test selector to sweep in the model tests, got {swept}'

    with pytest.raises(ValueError, match='also runs test.probe.not_null_beta_id'):
        _resource_selectors(manifest, bundle_tests=False)


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

    with pytest.raises(ValueError, match='Cannot generate a task for'):
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
            'a slash dispatches the selector to MethodName.Path',
            id='path-dispatch',
        ),
        pytest.param(
            {'orders.sql.sql': MODEL_SQL, 'plain.sql': MODEL_SQL},
            None,
            'a .sql suffix dispatches the selector to MethodName.File',
            id='file-dispatch',
        ),
    ],
)
def test_method_dispatching_names_are_refused(tmp_path, layout, schema_yml, note):
    """
    `SelectionCriteria.default_method` picks the selector method from the *value*: a path separator makes
    it `MethodName.Path`, and a `.sql`/`.py`/`.csv` suffix makes it `MethodName.File`. Neither is a dbt
    grammar metacharacter, so an earlier revision emitted both and each matched nothing while the task
    exited 0 — a test that never ran, and a model that was never built. Confirmed on dbt 1.12.0.
    """
    assert note
    _write_project(tmp_path, layout, schema_yml=schema_yml)
    manifest = _parse(tmp_path)

    with pytest.raises(ValueError, match='Cannot generate a task for'):
        _resource_selectors(manifest, bundle_tests=False)


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

    tasks = create_dbt_factory(bundle_tests=False).create_tasks(manifest)
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
    assert not cycles, f'emitted a cyclic depends_on for {cycles}: {graph}'


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


def test_uppercase_sql_suffix_is_refused_like_lowercase(tmp_path):
    """
    dbt's `default_method` lowercases before testing the suffix, so a model named `orders.SQL` is
    dispatched to `MethodName.File` exactly as `orders.sql` is. A case-sensitive guard let it through to a
    selector that matches nothing — verified with `dbt ls` on dbt 1.12.0, where `dbt run` exits 0 having
    built nothing.
    """
    _write_project(tmp_path, {'orders.SQL.sql': MODEL_SQL, 'plain.sql': MODEL_SQL})
    manifest = _parse(tmp_path)

    assert not _selected_ids(tmp_path, 'probe.orders.SQL,package:probe,file:orders.SQL.sql', 'model')
    with pytest.raises(ValueError, match='Cannot generate a task for'):
        _resource_selectors(manifest, bundle_tests=False)


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
    Asserts the exactness model predicts exactly what dbt runs, for every emitted task.

    This is the strongest available check on `_nodes_run_by`, because it compares the model against dbt
    in *both* directions. Under-predicting means a collision slips through and a task runs another task's
    resource; over-predicting means refusing a project dbt handles. A count-based assertion sees neither.

    Bundled `<resource>_test` tasks are skipped: they deliberately sweep a resource's whole test set with
    `--indirect-selection cautious`, so running several nodes is their purpose rather than a defect.
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
        if '--indirect-selection' in command or select.startswith('source:'):
            continue
        expand = verb == 'test'
        predicted = DbtFactory._nodes_run_by(select, index, expand=expand)  # pylint: disable=protected-access
        resource_type = None if verb == 'test' else {'run': 'model', 'seed': 'seed', 'snapshot': 'snapshot'}[verb]
        # Compare on unique ids: `_selected_ids` returns the *display* names `dbt ls` prints, which do not
        # match manifest keys, so intersecting those with `peers` would silently compare against nothing.
        actual = set(_selected_unique_ids(tmp_path, select, resource_type)) & set(peers)
        assert predicted == actual, (
            f'{task["task_key"]}: model predicts {sorted(predicted)} but dbt runs {sorted(actual)} ' f'for {select!r}'
        )


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


def test_attached_test_leaking_through_a_non_matching_model_is_refused(tmp_path):
    """
    dbt expands indirect selection *per component* and intersects afterwards, so a component can reach a
    model that the intersection excludes while the model's attached tests are added inside that component
    and survive on their own terms.

    Verified on dbt 1.12.0: with `models/orders.sql` carrying `not_null` and `models/other.sql` carrying
    `not_null: {name: orders}` in the shared `schema.yml`, the selector for the second test resolves to
    *both* tests. Its task depends only on `other_model`, so `not_null_orders_id` asserts on `orders`
    before `orders_model` has built it. An "intersect, then expand" model cannot see this, because
    `file:schema.yml` excludes `models/orders.sql` before the expansion is considered.
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

    leaked = _selected_ids(
        tmp_path, 'probe.orders,package:probe,file:schema.yml,resource_type:test,test_name:not_null', None
    )
    assert len(leaked) == 2, f'expected the attached test to leak through the model, got {leaked}'

    with pytest.raises(ValueError, match='also runs'):
        _resource_selectors(manifest, bundle_tests=False)


def test_multi_endpoint_test_leaking_from_one_endpoint_is_refused(tmp_path):
    """
    dbt's eager rule is "if ANY parent is selected, select the test" — its own words in
    `expand_selection`. An earlier revision required *every* endpoint (the `cautious` rule) and so
    ignored multi-endpoint tests entirely.

    Verified on dbt 1.12.0: a `relationships` test on `beta` referencing `delta` is pulled in by the
    singular `tests/beta.sql` task's selector, whose only dependency is `gamma_model` — so it races both
    `beta_model` and `delta_model`.
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

    leaked = _selected_ids(tmp_path, 'probe.beta,package:probe,file:beta.sql,resource_type:test', None)
    assert len(leaked) == 2, f'expected the relationships test to leak from one endpoint, got {leaked}'

    with pytest.raises(ValueError, match='also runs'):
        _resource_selectors(manifest, bundle_tests=False)
