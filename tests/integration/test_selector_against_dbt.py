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

import itertools
import json
import os
import random
from pathlib import Path

import pytest
from dbt.cli.main import dbtRunner

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


def _write_project(root: Path, model_paths: dict[str, str], schema_yml: str | None = None) -> None:
    """Writes a minimal dbt project: `model_paths` maps a path under `models/` to its SQL."""
    (root / 'dbt_project.yml').write_text(
        'name: probe\nprofile: probe\nversion: "1.0"\nconfig-version: 2\nmodel-paths: ["models"]\n',
        encoding='utf-8',
    )
    (root / 'profiles.yml').write_text(PROFILES, encoding='utf-8')
    for relative_path, sql in model_paths.items():
        target = root / 'models' / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(sql, encoding='utf-8')
    if schema_yml is not None:
        (root / 'models' / 'schema.yml').write_text(schema_yml, encoding='utf-8')


def _parse(root: Path) -> dict:
    """Parses the project with dbt and returns its real manifest, failing loudly if dbt cannot."""
    previous = os.environ.get('DBT_PROFILES_DIR')
    os.environ['DBT_PROFILES_DIR'] = str(root)
    cwd = Path.cwd()
    try:
        os.chdir(root)
        result = dbtRunner().invoke(['parse', '--quiet'])
        assert result.success, f'dbt could not parse the generated project: {result.exception}'
        return json.loads((root / 'target' / 'manifest.json').read_text(encoding='utf-8'))
    finally:
        os.chdir(cwd)
        if previous is None:
            os.environ.pop('DBT_PROFILES_DIR', None)
        else:
            os.environ['DBT_PROFILES_DIR'] = previous


def _selected_ids(root: Path, select: str, resource_type: str | None, indirect: bool) -> list[str]:
    """Returns the unique IDs dbt resolves `select` to, as `dbt ls` reports them."""
    args = ['ls', '--quiet', '--select', select]
    if resource_type:
        args += ['--resource-type', resource_type]
    if indirect:
        args += ['--indirect-selection', 'cautious']
    cwd = Path.cwd()
    previous = os.environ.get('DBT_PROFILES_DIR')
    os.environ['DBT_PROFILES_DIR'] = str(root)
    try:
        os.chdir(root)
        result = dbtRunner().invoke(args)
        assert result.success, f'dbt ls failed for {select!r}: {result.exception}'
        # `dbt ls` returns a list of unique-id strings; the runner's result type is a union across
        # every dbt command, so narrow it here rather than trusting the annotation.
        assert isinstance(result.result, list), f'expected dbt ls to return a list, got {type(result.result)}'
        return sorted(str(unique_id) for unique_id in result.result)
    finally:
        os.chdir(cwd)
        if previous is None:
            os.environ.pop('DBT_PROFILES_DIR', None)
        else:
            os.environ['DBT_PROFILES_DIR'] = previous


def _resource_selectors(manifest: dict, bundle_tests: bool) -> list[tuple[str, str, str]]:
    """
    Runs the factory over `manifest` and returns `(task_key, select, verb)` for every task that
    builds a resource — the tasks that must each touch exactly one node.
    """
    selectors = []
    for task in create_dbt_factory(bundle_tests=bundle_tests).create_tasks(manifest):
        command = task['dbt_task']['commands'][-1].split()
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


def test_generation_is_rejected_when_tests_share_a_schema_file(tmp_path):
    """
    Every test in one `schema.yml` shares that path, so a `file:` term cannot single one out. When
    that term is the only thing left, generation must fail rather than emit a selector that runs a
    different model's test.
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

    with pytest.raises(ValueError, match='no selector can isolate'):
        _resource_selectors(manifest, bundle_tests=False)


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
        try:
            selectors = _resource_selectors(manifest, bundle_tests)
        except ValueError as error:
            # Refusing to generate is an acceptable outcome; a wrong selector is not.
            assert 'no selector can isolate' in str(error)
            continue
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
