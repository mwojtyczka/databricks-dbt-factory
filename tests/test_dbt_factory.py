import os
import shlex
import subprocess
import sys
import textwrap
from collections.abc import Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
import yaml

from databricks_dbt_factory.DbtFactory import DbtFactory
from databricks_dbt_factory.DbtTask import DbtTaskOptions
from databricks_dbt_factory.job_spec import replace_tasks_in_job_spec
from databricks_dbt_factory.TaskFactory import DbtDependencyResolver, TestTaskFactory as DbtTestTaskFactory
from databricks_dbt_factory.Utils import read_dbt_manifest

BASE_PATH = str(Path(__file__).resolve().parent)


class _IterationCountingSet(set[str]):
    """A set that records explicit scans of its members."""

    def __init__(self, values: set[str]):
        super().__init__(values)
        self.iterations = 0

    def __iter__(self) -> Iterator[str]:
        self.iterations += 1
        return super().__iter__()


def _model(
    package: str,
    name: str,
    depends_on: list[str] | None = None,
    fqn: list[str] | None = None,
    version: int | str | None = None,
    path: str | None = None,
) -> tuple[str, dict]:
    # A versioned model's unique_id carries its version: model.<pkg>.<name>.v<N>.
    full_name = f"model.{package}.{name}" + (f".v{version}" if version is not None else "")
    info: dict = {
        'resource_type': 'model',
        'name': name,
        'package_name': package,
        'fqn': fqn or [package, name],
        'original_file_path': path or f"models/{name}.sql",
        'depends_on': {'nodes': depends_on or []},
    }
    if version is not None:
        info['version'] = version
    return full_name, info


def _test(
    package: str,
    name: str,
    depends_on: list[str],
    severity: str = 'error',
    fqn: list[str] | None = None,
    path: str | None = None,
    test_name: str | None = None,
) -> tuple[str, dict]:
    full_name = f"test.{package}.{name}"
    info: dict = {
        'resource_type': 'test',
        'name': name,
        'package_name': package,
        'fqn': fqn or [package, name],
        # dbt points a schema test at the .yml that declares it, so several tests share one path.
        'original_file_path': path or f"models/{name}.yml",
        'depends_on': {'nodes': depends_on},
        'config': {'severity': severity},
    }
    if test_name is not None:
        # A generic test carries the test type in `test_metadata.name` (`not_null`, `unique`, ...);
        # a singular test has no `test_metadata` at all.
        info['test_metadata'] = {'name': test_name}
    return full_name, info


def _seed(package: str, name: str, fqn: list[str] | None = None, path: str | None = None) -> tuple[str, dict]:
    full_name = f"seed.{package}.{name}"
    return full_name, {
        'resource_type': 'seed',
        'name': name,
        'package_name': package,
        'fqn': fqn or [package, name],
        'original_file_path': path or f"seeds/{name}.csv",
        'depends_on': {'nodes': []},
    }


def _snapshot(
    package: str,
    name: str,
    depends_on: list[str] | None = None,
    fqn: list[str] | None = None,
    path: str | None = None,
) -> tuple[str, dict]:
    full_name = f"snapshot.{package}.{name}"
    return full_name, {
        'resource_type': 'snapshot',
        'name': name,
        'package_name': package,
        'fqn': fqn or [package, name],
        'original_file_path': path or f"snapshots/{name}.sql",
        'depends_on': {'nodes': depends_on or []},
    }


def _analysis(package: str, name: str, fqn: list[str] | None = None, path: str | None = None) -> tuple[str, dict]:
    # dbt files analyses under `nodes` alongside models, but nothing selects them by default.
    full_name = f"analysis.{package}.{name}"
    return full_name, {
        'resource_type': 'analysis',
        'name': name,
        'package_name': package,
        'fqn': fqn or [package, 'analysis', name],
        'original_file_path': path or f"analyses/{name}.sql",
        'depends_on': {'nodes': []},
    }


def _source(package: str, source_name: str, table: str, path: str | None = None) -> tuple[str, dict]:
    full_name = f"source.{package}.{source_name}.{table}"
    return full_name, {
        'resource_type': 'source',
        'name': table,
        'source_name': source_name,
        'package_name': package,
        'fqn': [package, source_name, table],
        'original_file_path': path or f"models/{source_name}.yml",
    }


def _unit_test(
    package: str,
    model: str,
    name: str,
    fqn: list[str] | None = None,
    depends_on: list[str] | None = None,
    path: str | None = None,
    version: int | None = None,
) -> tuple[str, dict]:
    # `version` reproduces dbt's clone of a unit test onto a versioned model: it appends `_v<N>` to the
    # `unique_id` and sets `version`, but leaves `name`, `fqn` and `original_file_path` untouched.
    # Verified on dbt 1.12.0 — a hand-written fixture that varies `name` per version encodes a shape dbt
    # never produces, and would hide the fact that the clones are indistinguishable to any selector.
    suffix = f"_v{version}" if version is not None else ""
    full_name = f"unit_test.{package}.{model}.{name}{suffix}"
    info: dict = {
        'resource_type': 'unit_test',
        'unique_id': full_name,
        'name': name,
        'model': model,
        'package_name': package,
        'fqn': fqn or [package, model, name],
        # dbt declares unit tests in a .yml alongside the model.
        'original_file_path': path or f"models/{model}_unit_tests.yml",
        'depends_on': {'nodes': depends_on or [f"model.{package}.{model}"]},
    }
    if version is not None:
        info['version'] = version
    return full_name, info


def test_same_model_name_across_packages_produces_distinct_bundled_test_tasks(dbt_factory_bundled):
    nodes = dict(
        [
            _model('pkg_a', 'customers'),
            _model('pkg_b', 'customers'),
            _model('pkg_a', 'orders', depends_on=['model.pkg_a.customers', 'model.pkg_b.customers']),
            _test('pkg_a', 'unique_customers_id', ['model.pkg_a.customers']),
            _test('pkg_b', 'not_null_customers_id', ['model.pkg_b.customers']),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert 'pkg_a_customers_test' in by_key
    assert 'pkg_b_customers_test' in by_key
    assert by_key['pkg_a_customers_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg_a.unique_customers_id,package:pkg_a,file:unique_customers_id.yml,resource_type:test --target dev --indirect-selection empty'
    ]
    assert by_key['pkg_b_customers_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg_b.not_null_customers_id,package:pkg_b,file:not_null_customers_id.yml,resource_type:test --target dev --indirect-selection empty'
    ]
    assert by_key['pkg_a_customers_test']['depends_on'] == [{'task_key': 'pkg_a_customers_model'}]
    assert by_key['pkg_b_customers_test']['depends_on'] == [{'task_key': 'pkg_b_customers_model'}]

    assert {dep['task_key'] for dep in by_key['orders_model']['depends_on']} == {
        'pkg_a_customers_test',
        'pkg_b_customers_test',
    }


def test_bundled_test_groups_exact_selectors_by_mode_after_user_dbt_options():
    factory = DbtTestTaskFactory(
        DbtDependencyResolver(),
        DbtTaskOptions(task_type='dbt'),
        '--target dev --indirect-selection eager',
    )

    task = factory.create_bundled_task(
        'orders_test',
        {
            'empty': ['fqn:pkg.unique_orders_id', 'fqn:pkg.not_null_orders_id'],
            'cautious': ['fqn:pkg.orders,resource_type:model,fqn:pkg.check_orders'],
        },
        'orders',
        ['orders_model'],
    )

    assert task.commands == [
        'dbt test --select fqn:pkg.not_null_orders_id --select fqn:pkg.unique_orders_id '
        '--target dev --indirect-selection eager --indirect-selection empty',
        'dbt test --select fqn:pkg.orders,resource_type:model,fqn:pkg.check_orders '
        '--target dev --indirect-selection eager --indirect-selection cautious',
    ]


def test_bundled_test_with_both_modes_and_deps_emits_three_commands():
    factory = DbtTestTaskFactory(
        DbtDependencyResolver(),
        DbtTaskOptions(task_type='dbt', dbt_deps_enabled=True),
        '--target dev',
    )

    task = factory.create_bundled_task(
        'orders_test',
        {'empty': ['fqn:pkg.direct'], 'cautious': ['fqn:pkg.orders,fqn:pkg.scoped']},
        'orders',
        ['orders_model'],
    )

    assert task.commands == [
        'dbt deps --target dev',
        'dbt test --select fqn:pkg.direct --target dev --indirect-selection empty',
        'dbt test --select fqn:pkg.orders,fqn:pkg.scoped --target dev --indirect-selection cautious',
    ]


def test_bundle_mode_model_depending_on_single_resource_test_does_not_raise(dbt_factory_bundled):
    # In bundle mode, single-resource test nodes fold into their resource's bundled task and get no
    # task key of their own. A model that lists such a test in its `depends_on` drops that unkeyed
    # dep during resolution.
    nodes = dict(
        [
            _model('pkg', 'customers'),
            _test('pkg', 'unique_customers_id', ['model.pkg.customers']),
            _model('pkg', 'orders', depends_on=['test.pkg.unique_customers_id']),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_model']['depends_on'] == []


def test_tests_on_seed_produce_task_and_gate_downstream(dbt_factory_bundled):
    nodes = dict(
        [
            _seed('pkg', 'countries'),
            _model('pkg', 'enriched', depends_on=['seed.pkg.countries']),
            _test('pkg', 'unique_countries_code', ['seed.pkg.countries']),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert 'countries_test' in by_key
    assert by_key['countries_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.unique_countries_code,package:pkg,file:unique_countries_code.yml,resource_type:test --target dev --indirect-selection empty'
    ]
    assert by_key['countries_test']['depends_on'] == [{'task_key': 'countries_seed'}]
    assert by_key['enriched_model']['depends_on'] == [{'task_key': 'countries_test'}]


def test_tests_on_snapshot_produce_task_and_gate_downstream(dbt_factory_bundled):
    nodes = dict(
        [
            _snapshot('pkg', 'orders_snap'),
            _model('pkg', 'orders_history', depends_on=['snapshot.pkg.orders_snap']),
            _test('pkg', 'not_null_orders_snap_id', ['snapshot.pkg.orders_snap']),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert 'orders_snap_test' in by_key
    assert by_key['orders_snap_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.not_null_orders_snap_id,package:pkg,file:not_null_orders_snap_id.yml,resource_type:test --target dev --indirect-selection empty'
    ]
    assert by_key['orders_snap_test']['depends_on'] == [{'task_key': 'orders_snap_snapshot'}]
    assert by_key['orders_history_model']['depends_on'] == [{'task_key': 'orders_snap_test'}]


def test_tests_on_source_produce_bundled_task(dbt_factory_bundled):
    nodes = dict(
        [
            _test('pkg', 'unique_raw_customers_id', ['source.pkg.raw.customers']),
        ]
    )
    sources = dict([_source('pkg', 'raw', 'customers')])

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes, 'sources': sources})
    by_key = {t['task_key']: t for t in tasks}

    assert 'raw_customers_test' in by_key
    assert by_key['raw_customers_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.unique_raw_customers_id,package:pkg,file:unique_raw_customers_id.yml,resource_type:test --target dev --indirect-selection empty'
    ]
    assert by_key['raw_customers_test']['depends_on'] == []


@pytest.mark.parametrize(
    ('consumer', 'task_key'),
    [
        pytest.param(_model, 'customers_model', id='model'),
        pytest.param(_snapshot, 'customers_snapshot', id='snapshot'),
    ],
)
def test_bundled_source_test_gates_consumers(dbt_factory_bundled, consumer, task_key):
    source_id, source = _source('pkg', 'raw', 'customers')
    nodes = dict(
        [
            _test('pkg', 'unique_raw_customers_id', [source_id]),
            consumer('pkg', 'customers', depends_on=[source_id]),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes, 'sources': {source_id: source}})
    by_key = {task['task_key']: task for task in tasks}

    assert by_key[task_key]['depends_on'] == [{'task_key': 'raw_customers_test'}]


def test_flat_mode_emits_one_task_per_test_node_and_gates_downstream(dbt_factory):
    # Per-test mode mirrors `dbt build`: downstream models wait on upstream tests, so a
    # failing `severity: error` test skips downstream via Databricks task failure.
    nodes = dict(
        [
            _model('pkg', 'customers'),
            _model('pkg', 'orders', depends_on=['model.pkg.customers']),
            _test('pkg', 'unique_customers_id', ['model.pkg.customers']),
            _test('pkg', 'not_null_customers_id', ['model.pkg.customers']),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert 'unique_customers_id_test' in by_key
    assert 'not_null_customers_id_test' in by_key
    assert 'customers_test' not in by_key

    assert by_key['unique_customers_id_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.unique_customers_id,package:pkg,file:unique_customers_id.yml,resource_type:test --target dev --indirect-selection empty'
    ]
    assert by_key['unique_customers_id_test']['depends_on'] == [{'task_key': 'customers_model'}]
    # orders depends on customers AND every test attached to customers
    assert {dep['task_key'] for dep in by_key['orders_model']['depends_on']} == {
        'customers_model',
        'unique_customers_id_test',
        'not_null_customers_id_test',
    }


def test_flat_mode_cross_model_test_does_not_create_cycle(dbt_factory):
    # Relationship test references BOTH `orders` and `customers`. Without care, extending
    # `orders`'s deps with "tests of upstream (customers)" would pull in the relationship test,
    # which itself depends on `orders` — a direct cycle.
    nodes = dict(
        [
            _model('pkg', 'customers'),
            _model('pkg', 'orders', depends_on=['model.pkg.customers']),
            _model('pkg', 'payments', depends_on=['model.pkg.orders']),
            _test('pkg', 'unique_customers_id', ['model.pkg.customers']),
            _test(
                'pkg',
                'relationships_orders_customer_id__ref_customers',
                ['model.pkg.orders', 'model.pkg.customers'],
            ),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    # orders depends on customers + unique_customers_id, but NOT on the relationship test
    # (that test references orders itself — including it would cycle)
    assert {dep['task_key'] for dep in by_key['orders_model']['depends_on']} == {
        'customers_model',
        'unique_customers_id_test',
    }

    # payments (downstream of orders) picks up the relationship test — safe, payments
    # transitively depends on both orders and customers (the test's refs)
    payments_deps = {dep['task_key'] for dep in by_key['payments_model']['depends_on']}
    assert 'orders_model' in payments_deps
    assert 'relationships_orders_customer_id__ref_customers_test' in payments_deps


def test_flat_mode_transitive_cross_model_test_does_not_create_cycle(dbt_factory):
    # Transitive cycle case: test T refs {A, C} where C is downstream of B which is downstream
    # of A. Extending B's deps with "tests of upstream (A)" must NOT add T, because T depends
    # on C and C depends on B → B → T → C → B cycle. Only nodes downstream of both A and C
    # (i.e. downstream of C) should get T.
    nodes = dict(
        [
            _model('pkg', 'a'),
            _model('pkg', 'b', depends_on=['model.pkg.a']),
            _model('pkg', 'c', depends_on=['model.pkg.b']),
            _model('pkg', 'd', depends_on=['model.pkg.c']),
            _test('pkg', 'relationship_a_c', ['model.pkg.a', 'model.pkg.c']),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    # B's ancestors = {A}. Test T refs = {A, C}. C ∉ ancestors(B) → skip T.
    assert by_key['b_model']['depends_on'] == [{'task_key': 'a_model'}]
    # C's ancestors = {A, B}. C IS in T.refs → skip T (direct self-reference).
    assert by_key['c_model']['depends_on'] == [{'task_key': 'b_model'}]
    # D's ancestors = {A, B, C}. T.refs = {A, C} ⊆ ancestors(D) → add T.
    d_deps = {dep['task_key'] for dep in by_key['d_model']['depends_on']}
    assert d_deps == {'c_model', 'relationship_a_c_test'}


def test_flat_mode_adds_test_gates_only_at_the_first_downstream_frontier(dbt_factory):
    nodes = {}
    for index in range(100):
        name = f'm{index}'
        dependencies = [f'model.pkg.m{index - 1}'] if index else []
        nodes.update([_model('pkg', name, depends_on=dependencies)])
        nodes.update([_test('pkg', f'quality_{name}', [f'model.pkg.{name}'])])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {task['task_key']: task for task in tasks}
    gate_edges = []
    for task_key, task in by_key.items():
        if not task_key.endswith('_model'):
            continue
        for dependency in task['depends_on']:
            if dependency['task_key'].startswith('quality_'):
                gate_edges.append((task_key, dependency['task_key']))

    assert len(gate_edges) == 99
    assert gate_edges == [(f'm{index}_model', f'quality_m{index - 1}_test') for index in range(1, 100)]


def test_flat_mode_handles_a_descending_999_model_chain_with_one_test(dbt_factory):
    nodes = dict(
        _model(
            'pkg',
            f'm{index:04d}',
            depends_on=[f'model.pkg.m{index - 1:04d}'] if index else [],
        )
        for index in reversed(range(999))
    )
    nodes.update([_test('pkg', 'quality_m0000', ['model.pkg.m0000'])])

    tasks = dbt_factory.create_tasks({'nodes': nodes})

    assert len(tasks) == 1_000
    by_key = {task['task_key']: task for task in tasks}
    assert by_key['m0001_model']['depends_on'] == [
        {'task_key': 'm0000_model'},
        {'task_key': 'quality_m0000_test'},
    ]
    assert by_key['m0002_model']['depends_on'] == [{'task_key': 'm0001_model'}]


def test_flat_mode_skips_ancestor_computation_when_no_tests_are_emitted(
    dbt_factory: DbtFactory, monkeypatch: pytest.MonkeyPatch
):
    nodes = dict(
        _model(
            'pkg',
            f'm{index:04d}',
            depends_on=[f'model.pkg.m{index - 1:04d}'] if index else [],
        )
        for index in reversed(range(999))
    )

    def unexpected_ancestor_computation(*_args, **_kwargs):
        pytest.fail('ancestor computation is unnecessary without emitted tests')

    monkeypatch.setattr(dbt_factory, '_compute_ancestors', unexpected_ancestor_computation)

    tasks = dbt_factory.create_tasks({'nodes': nodes})

    assert len(tasks) == 999


@pytest.mark.parametrize('reverse_order', [False, True])
def test_flat_mode_refuses_a_cyclic_manifest_with_a_deterministic_remedy(dbt_factory, reverse_order):
    entries = [
        _model('pkg', 'a', depends_on=['model.pkg.b']),
        _model('pkg', 'b', depends_on=['model.pkg.a']),
        _test('pkg', 'quality_a', ['model.pkg.a']),
    ]
    nodes = dict(reversed(entries) if reverse_order else entries)

    with pytest.raises(ValueError) as error:
        dbt_factory.create_tasks({'nodes': nodes})

    assert str(error.value) == (
        'Cannot compute test gates because the manifest contains the dependency cycle '
        'model.pkg.a -> model.pkg.b -> model.pkg.a. Regenerate the manifest after removing the cycle.'
    )


def test_first_frontier_union_caches_eligible_tests_for_converging_dependencies(
    dbt_factory: DbtFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Eligibility is computed once per resource while converging frontiers inherit the full union."""
    nodes = dict(
        [
            _model('pkg', 'a'),
            _model('pkg', 'b'),
            _model('pkg', 'left', depends_on=['model.pkg.a']),
            _model('pkg', 'right', depends_on=['model.pkg.b']),
            _model('pkg', 'join', depends_on=['model.pkg.left', 'model.pkg.right']),
            _model('pkg', 'consumer_one', depends_on=['model.pkg.join']),
            _model('pkg', 'consumer_two', depends_on=['model.pkg.join']),
            _test('pkg', 'quality_a', ['model.pkg.a']),
            _test('pkg', 'quality_b', ['model.pkg.b']),
            _test('pkg', 'relationship_a_b', ['model.pkg.a', 'model.pkg.b']),
        ]
    )
    compute_ancestors = dbt_factory._compute_ancestors  # pylint: disable=protected-access
    tracked_ancestors: dict[str, _IterationCountingSet] = {}

    def compute_tracked_ancestors(dbt_nodes: dict, dbt_sources: dict) -> dict[str, set[str]]:
        tracked_ancestors.update(
            {
                resource: _IterationCountingSet(ancestors)
                for resource, ancestors in compute_ancestors(dbt_nodes, dbt_sources).items()
            }
        )
        return dict(tracked_ancestors)

    monkeypatch.setattr(DbtFactory, '_compute_ancestors', staticmethod(compute_tracked_ancestors))

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    deps_by_key = {task['task_key']: [dependency['task_key'] for dependency in task['depends_on']] for task in tasks}

    assert deps_by_key['left_model'] == ['a_model', 'quality_a_test']
    assert deps_by_key['right_model'] == ['b_model', 'quality_b_test']
    assert deps_by_key['join_model'] == ['left_model', 'right_model', 'relationship_a_b_test']
    assert deps_by_key['consumer_one_model'] == ['join_model']
    assert deps_by_key['consumer_two_model'] == ['join_model']
    assert tracked_ancestors['model.pkg.join'].iterations == 1


def test_flat_mode_warn_severity_tests_gate_downstream(dbt_factory):
    nodes = dict(
        [
            _model('pkg', 'customers'),
            _model('pkg', 'orders', depends_on=['model.pkg.customers']),
            _test('pkg', 'unique_customers_id', ['model.pkg.customers'], severity='warn'),
            _test('pkg', 'not_null_customers_id', ['model.pkg.customers'], severity='error'),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert 'unique_customers_id_test' in by_key
    assert 'not_null_customers_id_test' in by_key

    assert {dep['task_key'] for dep in by_key['orders_model']['depends_on']} == {
        'customers_model',
        'unique_customers_id_test',
        'not_null_customers_id_test',
    }


def test_flat_mode_test_on_seed_gates_on_seed(dbt_factory):
    nodes = dict(
        [
            _seed('pkg', 'countries'),
            _test('pkg', 'unique_countries_code', ['seed.pkg.countries']),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['unique_countries_code_test']['depends_on'] == [{'task_key': 'countries_seed'}]


def test_bundled_task_factory_assembles_commands(dbt_factory_bundled):
    test_factory = dbt_factory_bundled.task_factories['test']
    task = test_factory.create_bundled_task(
        task_key='customers_test',
        selects_by_indirect_selection={
            'empty': ['fqn:pkg.unique_customers_id', 'fqn:pkg.not_null_customers_id'],
        },
        deps_command_name='customers',
        depends_on=['customers_model'],
    )
    assert task.task_key == 'customers_test'
    assert task.commands == [
        'dbt test --select fqn:pkg.not_null_customers_id --select fqn:pkg.unique_customers_id '
        '--target dev --indirect-selection empty'
    ]
    assert task.depends_on == ['customers_model']


def test_cross_model_test_in_bundled_mode_is_emitted_as_standalone_task(dbt_factory_bundled):
    # The relationship test spans two models, so it must NOT be collapsed into either model's
    # bundled `_tests` task (dbt would hit a TABLE_OR_VIEW_NOT_FOUND on the un-built endpoint).
    # It should emit its own task with deps on both referenced models.
    nodes = dict(
        [
            _model('pkg', 'team_cities'),
            _model('pkg', 'game_details', depends_on=['model.pkg.team_cities']),
            _test('pkg', 'not_null_team_cities_name', ['model.pkg.team_cities']),
            _test(
                'pkg',
                'relationships_game_details_winner__team_city__ref_team_cities_',
                ['model.pkg.game_details', 'model.pkg.team_cities'],
            ),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    # Single-resource test → bundled by its exact selector.
    assert 'team_cities_test' in by_key
    assert by_key['team_cities_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.not_null_team_cities_name,package:pkg,file:not_null_team_cities_name.yml,resource_type:test --target dev --indirect-selection empty'
    ]

    # Cross-resource test → its own task, gated on both referenced models.
    cross_test_key = 'relationships_game_details_winner__team_city__ref_team_cities__test'
    assert cross_test_key in by_key
    assert by_key[cross_test_key]['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.relationships_game_details_winner__team_city__ref_team_cities_,package:pkg,file:relationships_game_details_winner__team_city__ref_team_cities_.yml,resource_type:test --target dev --indirect-selection empty'
    ]
    assert {dep['task_key'] for dep in by_key[cross_test_key]['depends_on']} == {
        'team_cities_model',
        'game_details_model',
    }

    # `game_details` has no single-resource tests, so no bundled `game_details_test` exists.
    assert 'game_details_test' not in by_key


def test_single_package_bundled_test_uses_qualified_select(dbt_factory_bundled):
    nodes = dict(
        [
            _model('pkg_a', 'customers'),
            _model('pkg_a', 'orders', depends_on=['model.pkg_a.customers']),
            _test('pkg_a', 'unique_customers_id', ['model.pkg_a.customers']),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert 'customers_test' in by_key
    assert by_key['customers_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg_a.unique_customers_id,package:pkg_a,file:unique_customers_id.yml,resource_type:test --target dev --indirect-selection empty'
    ]
    assert by_key['orders_model']['depends_on'] == [{'task_key': 'customers_test'}]


def test_duplicate_model_name_across_packages_selects_by_distinct_fqn(dbt_factory):
    # Two packages define a model named `customers`. Selecting by the bare name would make both
    # tasks run `dbt run --select fqn:customers`, executing both models from each task. The full FQN
    # keeps each task scoped to exactly its own node.
    nodes = dict(
        [
            _model('pkg_a', 'customers'),
            _model('pkg_b', 'customers'),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['pkg_a_customers_model']['dbt_task']['commands'] == [
        'dbt run --select fqn:pkg_a.customers,package:pkg_a,file:customers.sql,resource_type:model --target dev'
    ]
    assert by_key['pkg_b_customers_model']['dbt_task']['commands'] == [
        'dbt run --select fqn:pkg_b.customers,package:pkg_b,file:customers.sql,resource_type:model --target dev'
    ]


def test_flat_mode_downstream_dep_rewired_to_disambiguated_collided_key(dbt_factory):
    # A downstream model depending on one of two same-named (collided) models gates on the
    # disambiguated key `pkg_a_customers_model`. A plain `customers_model` here would be a dangling dep.
    nodes = dict(
        [
            _model('pkg_a', 'customers'),
            _model('pkg_b', 'customers'),
            _model('pkg', 'orders', depends_on=['model.pkg_a.customers']),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_model']['depends_on'] == [{'task_key': 'pkg_a_customers_model'}]


def test_model_in_subdirectory_selects_by_full_fqn_flat_mode(dbt_factory):
    # A model in a subdirectory has fqn [pkg, sub, name]. The select must be the full dotted fqn
    # `pkg.sub.name`, not `pkg.name` (which matches nothing).
    nodes = dict([_model('pkg', 'stg_orders', fqn=['pkg', 'staging', 'stg_orders'])])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['stg_orders_model']['dbt_task']['commands'] == [
        'dbt run --select fqn:pkg.staging.stg_orders,package:pkg,file:stg_orders.sql,resource_type:model --target dev'
    ]


def test_model_in_subdirectory_bundles_the_exact_test_selector(dbt_factory_bundled):
    nodes = dict(
        [
            _model('pkg', 'stg_orders', fqn=['pkg', 'staging', 'stg_orders']),
            _test('pkg', 'unique_stg_orders_id', ['model.pkg.stg_orders']),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['stg_orders_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.unique_stg_orders_id,package:pkg,file:unique_stg_orders_id.yml,resource_type:test --target dev --indirect-selection empty'
    ]


def test_flat_mode_unit_test_emits_task_and_gates_downstream(dbt_factory):
    # Unit tests live under the manifest `unit_tests` key. In per-test mode each becomes its own
    # task selected by its full fqn, gated on the model it tests, and downstream models gate on it.
    nodes = dict(
        [
            _model('pkg', 'orders', fqn=['pkg', 'staging', 'orders']),
            _model('pkg', 'summary', depends_on=['model.pkg.orders'], fqn=['pkg', 'marts', 'summary']),
        ]
    )
    unit_tests = dict([_unit_test('pkg', 'orders', 'test_totals', fqn=['pkg', 'staging', 'orders', 'test_totals'])])

    tasks = dbt_factory.create_tasks({'nodes': nodes, 'unit_tests': unit_tests})
    by_key = {t['task_key']: t for t in tasks}

    unit_test_key = 'unit_test_pkg_orders_test_totals'
    assert by_key[unit_test_key]['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.staging.orders.test_totals,package:pkg,file:orders_unit_tests.yml,resource_type:unit_test --target dev --indirect-selection empty'
    ]
    assert by_key[unit_test_key]['depends_on'] == [{'task_key': 'orders_model'}]
    # summary (downstream of orders) gates on the unit test as well as the model
    assert {dep['task_key'] for dep in by_key['summary_model']['depends_on']} == {
        'orders_model',
        unit_test_key,
    }


def test_bundled_mode_model_with_only_unit_test_emits_bundled_task(dbt_factory_bundled):
    # A model whose only test is a unit test still gets a bundled `<model>_test` task with that exact
    # unit-test selector, so it is not silently dropped.
    nodes = dict([_model('pkg', 'orders', fqn=['pkg', 'staging', 'orders'])])
    unit_tests = dict([_unit_test('pkg', 'orders', 'test_totals', fqn=['pkg', 'staging', 'orders', 'test_totals'])])

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes, 'unit_tests': unit_tests})
    by_key = {t['task_key']: t for t in tasks}

    assert 'orders_test' in by_key
    assert by_key['orders_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.staging.orders.test_totals,package:pkg,file:orders_unit_tests.yml,resource_type:unit_test --target dev --indirect-selection empty'
    ]
    assert by_key['orders_test']['depends_on'] == [{'task_key': 'orders_model'}]
    # No standalone unit-test task in bundled mode — the bundled task covers it.
    assert 'unit_test_pkg_orders_test_totals' not in by_key


def test_flat_mode_unit_test_on_absent_model_is_skipped(dbt_factory):
    # A unit test whose target model is not in the manifest is skipped rather than emitting a
    # task that gates on a model task that never exists.
    unit_tests = dict([_unit_test('pkg', 'missing', 'test_totals')])

    tasks = dbt_factory.create_tasks({'nodes': {}, 'unit_tests': unit_tests})
    by_key = {t['task_key']: t for t in tasks}

    assert 'unit_test_pkg_missing_test_totals' not in by_key
    assert by_key == {}


def test_create_job_spec_and_update(dbt_factory):
    run_job_spec_test(
        dbt_factory,
        BASE_PATH + "/test_data/job_definition_no_deps.yaml",
    )


def test_create_job_spec_and_update_with_dbt_deps(dbt_factory_with_deps):
    run_job_spec_test(
        dbt_factory_with_deps,
        BASE_PATH + "/test_data/job_definition.yaml",
    )


def test_create_job_spec_and_update_with_selected_dbt_deps(dbt_factory_with_deps_selected):
    run_job_spec_test(
        dbt_factory_with_deps_selected,
        BASE_PATH + "/test_data/job_definition_deps_selected.yaml",
    )


def test_notebook_task_generation(notebook_factory):
    run_job_spec_test(
        notebook_factory,
        BASE_PATH + "/test_data/job_definition_notebook_no_deps.yaml",
    )


def run_job_spec_test(dbt_factory, expected_job_definition_path):
    """Helper function to test databricks job definition generation."""
    dbt_manifest_path = BASE_PATH + "/test_data/manifest.json"
    input_job_definition_path = BASE_PATH + "/test_data/job_definition_template.yaml"

    with NamedTemporaryFile(suffix=".yaml", delete=False) as temp_file:
        actual_job_definition_path = temp_file.name

    try:
        tasks = dbt_factory.create_tasks(read_dbt_manifest(dbt_manifest_path))
        replace_tasks_in_job_spec(input_job_definition_path, tasks, actual_job_definition_path)

        with open(expected_job_definition_path, "r", encoding="utf-8") as file:
            expected_job_definition = yaml.safe_load(file)

        with open(actual_job_definition_path, "r", encoding="utf-8") as file:
            job_definition = yaml.safe_load(file)

        assert job_definition == expected_job_definition
    finally:
        if os.path.exists(actual_job_definition_path):
            os.remove(actual_job_definition_path)


def test_resolver_uses_task_keys_map():
    node = {"depends_on": {"nodes": ["model.a.orders"]}}
    task_keys = {"model.a.orders": "a_orders_model"}  # disambiguated
    assert DbtDependencyResolver.resolve(node, task_keys) == ["a_orders_model"]


def test_select_intersects_fqn_package_and_file(dbt_factory):
    # Every node is addressed the same way: the three independent facts the manifest gives us about
    # it, intersected with `,` (dbt's AND). No term is exact on its own — the fqn matches nested
    # descendants and, for a package node, matches via its package-stripped fqn; `package:` matches a
    # whole package; `file:` matches a base name in every package — so the intersection is what pins
    # one node. One rule for every resource, rather than a per-shape decision.
    nodes = dict([_model('pkg', 'orders', fqn=['pkg', 'marts', 'orders'], path='models/marts/orders.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_model']['dbt_task']['commands'] == [
        'dbt run --select fqn:pkg.marts.orders,package:pkg,file:orders.sql,resource_type:model --target dev'
    ]


def test_select_of_nested_sibling_is_separated_by_its_file(dbt_factory):
    # `models/marts/orders.sql` beside `models/marts/orders/items.sql`: the fqn `pkg.marts.orders`
    # matches `items` too (dbt matches an fqn as a path prefix), so unintersected it would build
    # `items` in orders' task and again in items' own, concurrently. `file:` separates them.
    nodes = dict(
        [
            _model('pkg', 'orders', fqn=['pkg', 'marts', 'orders'], path='models/marts/orders.sql'),
            _model('pkg', 'items', fqn=['pkg', 'marts', 'orders', 'items'], path='models/marts/orders/items.sql'),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_model']['dbt_task']['commands'] == [
        'dbt run --select fqn:pkg.marts.orders,package:pkg,file:orders.sql,resource_type:model --target dev'
    ]
    assert by_key['items_model']['dbt_task']['commands'] == [
        'dbt run --select fqn:pkg.marts.orders.items,package:pkg,file:items.sql,resource_type:model --target dev'
    ]


def test_select_of_package_node_is_separated_by_its_package(dbt_factory):
    # dbt compares a node's fqn with its package stripped as well, so a package model at
    # `models/probe/alpha.sql` in package `other` (fqn [other, probe, alpha]) is also matched by the
    # root project's `probe.alpha`. `package:` is the term that separates them — and `file:` cannot,
    # since both files are `alpha.sql`.
    nodes = dict(
        [
            _model('probe', 'alpha', fqn=['probe', 'alpha'], path='models/alpha.sql'),
            _model('other', 'alpha', fqn=['other', 'probe', 'alpha'], path='models/probe/alpha.sql'),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    commands = {t['task_key']: t['dbt_task']['commands'][0] for t in tasks}

    assert (
        commands['probe_alpha_model']
        == 'dbt run --select fqn:probe.alpha,package:probe,file:alpha.sql,resource_type:model --target dev'
    )
    assert commands['other_alpha_model'] == (
        'dbt run --select fqn:other.probe.alpha,package:other,file:alpha.sql,resource_type:model --target dev'
    )


def test_bundled_test_uses_the_tests_uniform_exact_selector(dbt_factory_bundled):
    nodes = dict(
        [
            _model('pkg', 'orders', fqn=['pkg', 'marts', 'orders'], path='models/marts/orders.sql'),
            _test('pkg', 'unique_orders_id', ['model.pkg.orders'], path='models/marts/schema.yml'),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.unique_orders_id,package:pkg,file:schema.yml,resource_type:test --target dev --indirect-selection empty'
    ]


@pytest.mark.parametrize(
    ('bad_segment', 'reason'),
    [
        pytest.param('my orders', 'space is a union separator', id='space'),
        pytest.param('orders,archive', 'comma is an intersection separator', id='comma'),
        pytest.param('or[der]s', 'brackets are fnmatch syntax', id='brackets'),
        pytest.param('star*model', 'star is fnmatch syntax', id='star'),
        pytest.param('q?model', 'question mark is fnmatch syntax', id='question-mark'),
    ],
)
def test_unusable_fqn_segment_is_dropped_but_other_terms_remain(dbt_factory, bad_segment, reason):
    # A directory whose name dbt cannot express in a selector costs us the fqn *path*, falling back to the
    # bare name. The remaining terms still address the node, so generation succeeds rather than failing
    # outright. (`reason` documents which dbt rule each character trips.)
    assert reason
    nodes = dict([_model('pkg', 'orders', fqn=['pkg', bad_segment, 'orders'], path=f'models/{bad_segment}/orders.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_model']['dbt_task']['commands'] == [
        'dbt run --select fqn:orders,package:pkg,file:orders.sql,resource_type:model --target dev'
    ]


def test_colon_in_an_fqn_segment_is_kept_now_that_the_method_is_explicit(dbt_factory):
    # A colon used to make the whole value unusable, because dbt read everything before it as a method
    # name. Naming the method ourselves settles that: `fqn:pkg.colon:model.orders` matches literally —
    # verified with `dbt ls` on dbt 1.12.0 — so the fqn term survives and the node keeps its strongest
    # discriminator.
    nodes = dict([_model('pkg', 'orders', fqn=['pkg', 'colon:model', 'orders'], path='models/colon:model/orders.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})

    assert [t['dbt_task']['commands'][0] for t in tasks] == [
        'dbt run --select fqn:pkg.colon:model.orders,package:pkg,file:orders.sql,resource_type:model --target dev'
    ]


def test_fqn_ending_in_a_graph_operator_is_refused(dbt_factory):
    # dbt reads a trailing `+N` as child depth, so `pkg.orders+1` selects `pkg.orders` and its
    # children — the wrong model entirely. The bare name `orders+1` is no better: on its own it *is*
    # the whole raw selector, so it trips the same rule and selects nothing. That leaves only
    # `package:`/`file:`, which address groups, so the resource cannot be addressed and generation
    # refuses. `package:pkg,file:orders+1.sql` happens to resolve to one node here — see
    # `test_file_name_alone_never_addresses_a_resource` for why we do not rely on that.
    nodes = dict([_model('pkg', 'orders+1', fqn=['pkg', 'orders+1'], path='models/orders+1.sql')])

    with pytest.raises(ValueError, match='Cannot generate a task for'):
        dbt_factory.create_tasks({'nodes': nodes})


def test_fqn_starting_with_a_numeric_graph_operator_keeps_its_fqn_term(dbt_factory):
    # A leading `N+` is read as parent depth only when dbt is inferring the method: bare `2+orders` selects
    # `probe.orders` (the sibling plus two levels of its parents) — a wrong-node hit, not an empty one.
    # Under an explicit `fqn:` it is literal, so the fqn term survives and stays exact. Both confirmed with
    # `dbt ls` on dbt 1.12.0.
    nodes = dict([_model('pkg', '2+orders', fqn=['pkg', '2+orders'], path='models/2+orders.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['2+orders_model']['dbt_task']['commands'] == [
        'dbt run --select fqn:pkg.2+orders,package:pkg,file:2+orders.sql,resource_type:model --target dev'
    ]


def test_name_fallback_ending_in_a_graph_operator_is_refused(dbt_factory):
    # The same name with an unusable fqn (a spacey directory) has nothing left to fall back on: a trailing
    # `+1` is read as child depth even under an explicit `fqn:` prefix, so `fqn:orders+1` selects `orders`
    # *and its children* — verified with `dbt ls` on dbt 1.12.0. Two tests share the schema.yml, so `file:`
    # cannot stand in for the name either — refuse rather than emit a task that runs the wrong node.
    #
    # A *leading* `@` or `N+` is no longer refused: those are literal under `fqn:`, so refusing them cost a
    # working project for nothing. See `test_leading_graph_operators_are_literal_under_an_explicit_fqn`.
    nodes = dict(
        [
            _test(
                'pkg',
                'orders+1',
                ['model.pkg.a'],
                fqn=['pkg', 'my dir', 'orders+1'],
                path='models/my dir/schema.yml',
                test_name='not_null',
            ),
            _test(
                'pkg',
                'not_null_b_id',
                ['model.pkg.b'],
                fqn=['pkg', 'my dir', 'not_null_b_id'],
                path='models/my dir/schema.yml',
                test_name='not_null',
            ),
        ]
    )

    with pytest.raises(ValueError, match='Cannot generate a task for'):
        dbt_factory.create_tasks({'nodes': nodes})


def test_file_name_alone_never_addresses_a_resource(dbt_factory):
    # `file:` never counts as addressing a resource, even when the file holds exactly one. dbt has no
    # `unique_id:` selector, so exactness has to be established per node; "this file holds one resource"
    # is a property of the surrounding project, and `file:` matches a base name rather than a path, so
    # it is not one the manifest states directly. The rule is therefore: the fqn or the name must
    # survive.
    #
    # Deliberately stricter than dbt requires — `package:pkg,file:orders+1.sql` does resolve to one node,
    # confirmed with `dbt ls` on dbt 1.12.0 — and the error says to rename the file.
    nodes = dict([_model('pkg', 'orders+1', fqn=['pkg', 'my dir', 'orders+1'], path='models/my dir/orders+1.sql')])

    with pytest.raises(ValueError, match='Cannot generate a task for'):
        dbt_factory.create_tasks({'nodes': nodes})


def test_source_with_a_graph_operator_uses_its_exact_test_selector(dbt_factory_bundled):
    # A bundle addresses the test node directly, so an unusable source selector does not matter when
    # the test itself has an exact selector.
    nodes = dict([_test('pkg', 'source_not_null_raw_orders_id', ['source.pkg.raw.orders+1'])])
    sources = dict([_source('pkg', 'raw', 'orders+1')])

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes, 'sources': sources})

    assert tasks[0]['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.source_not_null_raw_orders_id,package:pkg,file:source_not_null_raw_orders_id.yml,resource_type:test --target dev --indirect-selection empty'
    ]


def test_source_selector_keeps_an_operator_away_from_the_boundary(dbt_factory_bundled):
    # Only the boundary matters. `source:pkg.raw.2+ord` puts the `2+` mid-string, and dbt resolves it
    # exactly (verified with `dbt ls` on dbt 1.12.0), so refusing it would reject a working project.
    nodes = dict([_test('pkg', 'source_not_null_raw_2_ord_id', ['source.pkg.raw.2+ord'])])
    sources = dict([_source('pkg', 'raw', '2+ord')])

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes, 'sources': sources})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['raw_2+ord_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.source_not_null_raw_2_ord_id,package:pkg,file:source_not_null_raw_2_ord_id.yml,resource_type:test --target dev --indirect-selection empty'
    ]


def test_source_dynamic_reference_does_not_enter_an_exact_test_bundle(dbt_factory_bundled):
    source_id, source = _source('pkg', '{{job', 'id}}')
    nodes = dict([_test('pkg', 'source_not_null_raw_orders_id', [source_id])])

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes, 'sources': {source_id: source}})

    command = tasks[0]['dbt_task']['commands'][0]
    assert '{{job.id}}' not in command
    assert command.endswith('--indirect-selection empty')


def test_bundled_union_cannot_compose_a_dynamic_reference(dbt_factory_bundled):
    first_id, first = _test(
        'pkg',
        'a_check',
        ['model.pkg.orders'],
        test_name='kind{{',
    )
    second_id, second = _test(
        'pkg',
        'b}}',
        ['model.pkg.orders'],
    )
    nodes = dict([_model('pkg', 'orders'), (first_id, first), (second_id, second)])

    with pytest.raises(ValueError, match='final selector .* contains a Databricks dynamic value reference'):
        dbt_factory_bundled.create_tasks({'nodes': nodes})


def test_generation_fails_when_only_non_discriminating_terms_survive(dbt_factory):
    # `package:` and `file:` both address *groups* of nodes: a `schema.yml` holds every test declared
    # in it, so `package:+file:+test_name:` can still match several. Only the fqn or the bare name
    # picks out one node, so if neither survives the selector cannot be exact regardless of what else
    # does. Confirmed with `dbt ls` on dbt 1.12.0: a bracketed custom test name alongside a plain
    # `not_null` in one schema.yml yielded `package:probe,file:schema.yml,test_name:not_null`, which
    # resolves to *both* tests — so that task and the other one would each run `not_null_b_id`.
    nodes = dict(
        [
            _test(
                'pkg',
                'check[a]id',
                ['model.pkg.a'],
                fqn=['pkg', 'check[a]id'],
                path='models/schema.yml',
                test_name='not_null',
            ),
            # The sibling that makes `file:schema.yml` ambiguous, exactly as dbt reports it.
            _test(
                'pkg',
                'not_null_b_id',
                ['model.pkg.b'],
                fqn=['pkg', 'not_null_b_id'],
                path='models/schema.yml',
                test_name='not_null',
            ),
        ]
    )

    with pytest.raises(ValueError, match='Cannot generate a task for'):
        dbt_factory.create_tasks({'nodes': nodes})


def test_ambiguous_test_with_a_missing_parent_is_refused(dbt_factory):
    nodes = dict(
        [
            _model('pkg', 'a'),
            _model('pkg', 'b'),
            _test(
                'pkg',
                'check',
                ['model.pkg.a', 'model.pkg.missing'],
                fqn=['pkg', 'check'],
                path='models/schema.yml',
                test_name='not_null',
            ),
            _test(
                'pkg',
                'check.nested',
                ['model.pkg.b'],
                fqn=['pkg', 'check', 'nested'],
                path='models/schema.yml',
                test_name='not_null',
            ),
        ]
    )

    with pytest.raises(ValueError, match='also runs'):
        dbt_factory.create_tasks({'nodes': nodes})


def test_ambiguous_test_with_only_a_missing_parent_is_refused(dbt_factory):
    nodes = dict(
        [
            _model('pkg', 'b'),
            _test(
                'pkg',
                'check',
                ['model.pkg.missing'],
                fqn=['pkg', 'check'],
                path='models/schema.yml',
                test_name='not_null',
            ),
            _test(
                'pkg',
                'check.nested',
                ['model.pkg.b'],
                fqn=['pkg', 'check', 'nested'],
                path='models/schema.yml',
                test_name='not_null',
            ),
        ]
    )

    with pytest.raises(ValueError, match='also runs'):
        dbt_factory.create_tasks({'nodes': nodes})


def test_a_usable_name_still_rescues_an_unusable_fqn(dbt_factory):
    # The boundary of the refusal above: only the fqn *or* the name has to survive. A space in the
    # directory kills the fqn, but `orders` is a fine selector and dbt matches a bare name against the
    # fqn's leaf, so these projects keep working — refusing them would reject any project with a space
    # in a directory name.
    nodes = dict([_model('pkg', 'orders', fqn=['pkg', 'my dir', 'orders'], path='models/my dir/orders.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})

    assert [t['dbt_task']['commands'][0] for t in tasks] == [
        'dbt run --select fqn:orders,package:pkg,file:orders.sql,resource_type:model --target dev'
    ]


def test_graph_operator_inside_a_segment_is_kept(dbt_factory):
    # `@` and `+` are only operators at the selector's start/end. `pkg.+leading` is exact, verified
    # with `dbt ls`, so rejecting it would refuse a project dbt handles fine.
    nodes = dict([_model('pkg', '+leading', fqn=['pkg', '+leading'], path='models/+leading.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['+leading_model']['dbt_task']['commands'] == [
        'dbt run --select fqn:pkg.+leading,package:pkg,file:+leading.sql,resource_type:model --target dev'
    ]


@pytest.mark.parametrize(
    'name',
    [
        pytest.param('orders{draft}', id='literal-braces'),
        pytest.param('orders{{draft', id='unclosed-reference'),
        pytest.param('ordersdraft}}', id='unopened-reference'),
        pytest.param('orders{{draft}', id='mismatched-reference'),
    ],
)
def test_literal_and_incomplete_braces_are_valid_selector_text(dbt_factory, name):
    nodes = dict([_model('pkg', name)])

    tasks = dbt_factory.create_tasks({'nodes': nodes})

    assert shlex.split(tasks[0]['dbt_task']['commands'][0]) == [
        'dbt',
        'run',
        '--select',
        f'fqn:pkg.{name},package:pkg,file:{name}.sql,resource_type:model',
        '--target',
        'dev',
    ]


def test_complete_dynamic_reference_falls_back_or_refuses(dbt_factory):
    safe_name_nodes = dict(
        [_model('pkg', 'orders', fqn=['pkg', '{{job.id}}', 'orders'], path='models/{{job.id}}/orders.sql')]
    )

    tasks = dbt_factory.create_tasks({'nodes': safe_name_nodes})

    assert tasks[0]['dbt_task']['commands'] == [
        'dbt run --select fqn:orders,package:pkg,file:orders.sql,resource_type:model --target dev'
    ]

    unsafe_nodes = dict([_model('pkg', '{{job.id}}', path='models/{{job.id}}.sql')])
    with pytest.raises(ValueError, match='Databricks dynamic value reference'):
        dbt_factory.create_tasks({'nodes': unsafe_nodes})


def test_selector_composition_drops_a_term_that_forms_a_dynamic_reference(dbt_factory):
    nodes = dict(
        [
            _model('pkg', 'orders'),
            _test(
                'pkg',
                'check{{',
                ['model.pkg.orders'],
                path='models/schema}}.yml',
                test_name='not_null',
            ),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    command = next(
        shlex.split(task['dbt_task']['commands'][0])
        for task in tasks
        if task['dbt_task']['commands'][0].startswith('dbt test')
    )
    select = command[command.index('--select') + 1]

    assert select == 'fqn:pkg.check{{,package:pkg,resource_type:test,test_name:not_null'


def test_parent_scoped_selector_refuses_a_composed_dynamic_reference(dbt_factory):
    nodes = dict(
        [
            _model('pkg', 'orders{{'),
            _model('pkg', 'other'),
            _test(
                'pkg',
                'check}}',
                ['model.pkg.orders{{'],
                path='models/schema.yml',
                test_name='not_null',
            ),
            _test(
                'pkg',
                'check}}.nested',
                ['model.pkg.other'],
                fqn=['pkg', 'check}}', 'nested'],
                path='models/schema.yml',
                test_name='not_null',
            ),
        ]
    )

    with pytest.raises(ValueError, match='Databricks dynamic value reference'):
        dbt_factory.create_tasks({'nodes': nodes})


def test_generation_fails_when_no_term_can_address_the_node(dbt_factory):
    # When the resource name itself is unusable, every term carries it — the fqn leaf, the file name,
    # and only `package:` survives, which matches the whole package. Emitting that would build every
    # model in the package, so generation fails loudly with the node and the remedy named.
    nodes = dict([_model('pkg', 'or[der]s', fqn=['pkg', 'or[der]s'], path='models/or[der]s.sql')])

    with pytest.raises(ValueError, match='Cannot generate a task for'):
        dbt_factory.create_tasks({'nodes': nodes})


def test_same_type_tests_in_one_file_get_distinct_selectors(dbt_factory):
    # `test_name:` narrows to the generic test *type*, so two `not_null` tests declared in one
    # `schema.yml` share it. With a spacey directory making the fqn unusable too, both tasks would
    # otherwise emit the same selector and each run the other's test — before the other's model is
    # built, since each task depends only on its own. The node's own `name` separates them.
    nodes = dict(
        [
            _model('pkg', 'a', fqn=['pkg', 'my tests', 'a'], path='models/my tests/a.sql'),
            _model('pkg', 'b', fqn=['pkg', 'my tests', 'b'], path='models/my tests/b.sql'),
            _test(
                'pkg',
                'not_null_a_id',
                ['model.pkg.a'],
                fqn=['pkg', 'my tests', 'not_null_a_id'],
                path='models/my tests/schema.yml',
                test_name='not_null',
            ),
            _test(
                'pkg',
                'not_null_b_id',
                ['model.pkg.b'],
                fqn=['pkg', 'my tests', 'not_null_b_id'],
                path='models/my tests/schema.yml',
                test_name='not_null',
            ),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    commands = {t['task_key']: t['dbt_task']['commands'][0] for t in tasks}

    assert commands['not_null_a_id_test'] != commands['not_null_b_id_test']
    assert commands['not_null_a_id_test'] == (
        'dbt test --select fqn:not_null_a_id,package:pkg,file:schema.yml,resource_type:test,test_name:not_null --target dev --indirect-selection empty'
    )


def test_data_test_selector_includes_its_test_name(dbt_factory):
    # Data tests declared in one `schema.yml` share that file, so `file:` cannot separate them.
    # `test_name:` narrows by the generic test type, which distinguishes a `not_null` from a
    # `unique` in the same file.
    nodes = dict(
        [
            _model('pkg', 'a', fqn=['pkg', 'marts', 'a'], path='models/marts/a.sql'),
            _test(
                'pkg',
                'not_null_a_id',
                ['model.pkg.a'],
                fqn=['pkg', 'marts', 'not_null_a_id'],
                path='models/marts/schema.yml',
                test_name='not_null',
            ),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['not_null_a_id_test']['dbt_task']['commands'] == [
        'dbt test --select fqn:pkg.marts.not_null_a_id,package:pkg,file:schema.yml,resource_type:test,test_name:not_null --target dev --indirect-selection empty'
    ]


def test_singular_test_sharing_a_models_fqn_generates_now_that_selection_is_empty(dbt_factory):
    # `models/beta.sql` and `tests/beta.sql` parse with the same fqn and base name. Under dbt's default
    # eager mode this had to be refused: the test task's selector also reached the model, and eager
    # selection then added the model's *attached* tests, so the task ran `not_null_beta_id` before
    # `beta_model` had built `beta`.
    #
    # Pinning `--indirect-selection empty` removes that entirely — verified with `dbt ls` on dbt 1.12.0,
    # where the same selector returns both tests under eager and only `test.probe.beta` under empty. So the
    # layout is addressable and no longer refused, even with the model carrying its own test.
    model = _model('pkg', 'beta', fqn=['pkg', 'beta'], path='models/beta.sql')
    singular = _test('pkg', 'beta', [], fqn=['pkg', 'beta'], path='tests/beta.sql')
    attached = _test('pkg', 'not_null_beta_id', ['model.pkg.beta'], path='models/schema.yml', test_name='not_null')

    commands = {
        t['task_key']: t['dbt_task']['commands'][0]
        for t in dbt_factory.create_tasks({'nodes': dict([model, singular, attached])})
    }

    assert (
        commands['beta_model']
        == 'dbt run --select fqn:pkg.beta,package:pkg,file:beta.sql,resource_type:model --target dev'
    )
    assert commands['beta_test'] == (
        'dbt test --select fqn:pkg.beta,package:pkg,file:beta.sql,resource_type:test'
        ' --target dev --indirect-selection empty'
    )
    assert commands['not_null_beta_id_test'] == (
        'dbt test --select fqn:pkg.not_null_beta_id,package:pkg,file:schema.yml,resource_type:test,'
        'test_name:not_null --target dev --indirect-selection empty'
    )


def test_selector_is_shell_quoted_when_a_name_contains_a_quote(dbt_factory):
    # `models/customer's.sql` is a legal dbt model. Unquoted, the notebook runner's `shlex.split`
    # raises `ValueError: No closing quotation` and the task dies before dbt runs, so the selector is
    # shell-quoted at command construction.
    nodes = dict([_model('pkg', "customer's", fqn=['pkg', "customer's"], path="models/customer's.sql")])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    command = tasks[0]['dbt_task']['commands'][0]

    assert shlex.split(command) == [
        'dbt',
        'run',
        '--select',
        "fqn:pkg.customer's,package:pkg,file:customer's.sql,resource_type:model",
        '--target',
        'dev',
    ]


def test_select_falls_back_to_the_bare_name_when_the_node_has_no_fqn(dbt_factory):
    # dbt always emits an fqn, but a hand-rolled or truncated manifest may not. The other terms still
    # address the node, so no fqn simply means one fewer term.
    node_id, info = _model('pkg', 'orders')
    del info['fqn']

    tasks = dbt_factory.create_tasks({'nodes': {node_id: info}})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_model']['dbt_task']['commands'] == [
        'dbt run --select fqn:orders,package:pkg,file:orders.sql,resource_type:model --target dev'
    ]


def test_bundled_source_with_an_unusable_name_uses_the_exact_test_selector(dbt_factory_bundled):
    nodes = dict([_test('pkg', 'unique_raw_customers_id', ['source.pkg.raw,archive.customers'])])
    sources = dict([_source('pkg', 'raw,archive', 'customers')])

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes, 'sources': sources})

    assert tasks[0]['dbt_task']['commands'][0].startswith('dbt test --select fqn:pkg.unique_raw_customers_id,')


@pytest.mark.parametrize(
    ('source_name', 'table'),
    [
        pytest.param('raw.v1', 'orders', id='dotted-source-name'),
        pytest.param('raw', 'orders.v1', id='dotted-table-name'),
    ],
)
def test_bundled_source_with_a_dotted_part_uses_the_exact_test_selector(dbt_factory_bundled, source_name, table):
    nodes = dict([_test('pkg', 'unique_raw_orders_id', [f"source.pkg.{source_name}.{table}"])])
    sources = dict([_source('pkg', source_name, table)])

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes, 'sources': sources})

    assert tasks[0]['dbt_task']['commands'][0].startswith('dbt test --select fqn:pkg.unique_raw_orders_id,')


def test_disabled_node_in_the_manifest_gets_no_task(dbt_factory):
    # dbt normally files an `enabled=false` resource under the manifest's `disabled` key, which we
    # never read. But a versioned model whose declared version has no file leaks a *disabled* test
    # into `nodes` (config.enabled=False, depends_on.nodes=[]). dbt refuses to select it, and
    # `dbt test` on a zero-match selector still exits 0 -- so emitting a task for it produces a
    # green task that asserts nothing. Confirmed against dbt 1.12.0.
    live_id, live_info = _test('pkg', 'not_null_orders_v2_id', ['model.pkg.orders.v2'], test_name='not_null')
    dead_id, dead_info = _test('pkg', 'not_null_orders_v1_id', [], test_name='not_null')
    dead_info['config']['enabled'] = False
    nodes = dict([_model('pkg', 'orders', version=2), (live_id, live_info), (dead_id, dead_info)])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert 'not_null_orders_v1_id_test' not in by_key, 'a disabled test must not become a task'
    assert 'not_null_orders_v2_id_test' in by_key
    assert 'orders_v2_model' in by_key


def test_disabled_node_is_not_a_dependency_or_a_bundled_test(dbt_factory_bundled):
    # The disabled test must also drop out of the bundling decision: were it counted, its model
    # would get a bundled `<model>_test` task gating downstream work on a test dbt will not run.
    dead_id, dead_info = _test('pkg', 'not_null_orders_id', ['model.pkg.orders'], test_name='not_null')
    dead_info['config']['enabled'] = False
    nodes = dict(
        [_model('pkg', 'orders'), _model('pkg', 'downstream', depends_on=['model.pkg.orders']), (dead_id, dead_info)]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert 'orders_test' not in by_key, 'a disabled test must not produce a bundled test task'
    assert by_key['downstream_model']['depends_on'] == [{'task_key': 'orders_model'}]


def test_flat_mode_unit_test_on_versioned_model_emits_task(dbt_factory):
    # dbt rewrites a unit test on a versioned model to `unit_test.<pkg>.<model>.<name>_v<N>` with
    # depends_on.nodes[0] = model.<pkg>.<model>.v<N>, while leaving `model` as the bare name. The
    # target must be read from depends_on, not rebuilt as `model.<pkg>.<model>` — that id does not
    # exist in the manifest, so the unit test would be silently dropped.
    nodes = dict(
        [
            _model('pkg', 'dim', fqn=['pkg', 'marts', 'dim', 'v1'], version=1),
            _model('pkg', 'dim', fqn=['pkg', 'marts', 'dim', 'v2'], version=2),
        ]
    )
    unit_tests = dict(
        [
            _unit_test(
                'pkg',
                'dim',
                'ut_a',
                fqn=['pkg', 'models', 'marts', 'dim', 'ut_a'],
                depends_on=['model.pkg.dim.v1'],
                version=1,
            ),
            _unit_test(
                'pkg',
                'dim',
                'ut_a',
                fqn=['pkg', 'models', 'marts', 'dim', 'ut_a'],
                depends_on=['model.pkg.dim.v2'],
                version=2,
            ),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes, 'unit_tests': unit_tests})
    by_key = {t['task_key']: t for t in tasks}

    unit_test_keys = sorted(key for key in by_key if key.startswith('unit_test_'))
    assert unit_test_keys == ['unit_test_pkg_dim_ut_a_v1', 'unit_test_pkg_dim_ut_a_v2']
    assert by_key['unit_test_pkg_dim_ut_a_v1']['depends_on'] == [{'task_key': 'dim_v1_model'}]
    assert by_key['unit_test_pkg_dim_ut_a_v2']['depends_on'] == [{'task_key': 'dim_v2_model'}]
    for task_key in unit_test_keys:
        command = by_key[task_key]['dbt_task']['commands'][-1]
        assert command.endswith('--indirect-selection cautious')


def test_bundled_mode_unit_test_on_versioned_model_emits_bundled_task(dbt_factory_bundled):
    # The resolved versioned model receives a bundle containing its unit-test node.
    nodes = dict(
        [
            _model('pkg', 'dim', fqn=['pkg', 'marts', 'dim', 'v1'], version=1),
            _model('pkg', 'dim', fqn=['pkg', 'marts', 'dim', 'v2'], version=2),
        ]
    )
    unit_tests = dict(
        [
            _unit_test(
                'pkg',
                'dim',
                'ut_a_v2',
                fqn=['pkg', 'models', 'marts', 'dim', 'ut_a'],
                depends_on=['model.pkg.dim.v2'],
            ),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes, 'unit_tests': unit_tests})
    by_key = {t['task_key']: t for t in tasks}

    assert 'dim_v2_test' in by_key
    assert by_key['dim_v2_test']['depends_on'] == [{'task_key': 'dim_v2_model'}]
    # v1 has no tests at all, so it gets no bundled test task.
    assert 'dim_v1_test' not in by_key


def test_unit_test_target_falls_back_to_model_field_when_depends_on_absent(dbt_factory):
    # A manifest written by an older dbt (or a hand-rolled one) may omit depends_on for a unit
    # test. The `model`/`package_name` reconstruction stays as a fallback so those still work.
    nodes = dict([_model('pkg', 'orders', fqn=['pkg', 'staging', 'orders'])])
    unit_test_id, unit_test_info = _unit_test('pkg', 'orders', 'test_totals')
    del unit_test_info['depends_on']

    tasks = dbt_factory.create_tasks({'nodes': nodes, 'unit_tests': {unit_test_id: unit_test_info}})
    by_key = {t['task_key']: t for t in tasks}

    assert 'unit_test_pkg_orders_test_totals' in by_key


def test_single_segment_fqn_does_not_crash_generation(dbt_factory):
    # Matching retries with the node's package stripped, which for a one-segment fqn leaves an empty
    # list. dbt's `is_selected_node` indexes `fqn[-1]`, so the mirror must handle the empty case rather
    # than raise — otherwise a manifest holding such a node crashes generation instead of building it.
    nodes = dict([_model('pkg', 'lonely', fqn=['lonely'])])

    tasks = dbt_factory.create_tasks({'nodes': nodes})

    assert [t['dbt_task']['commands'][0] for t in tasks] == [
        'dbt run --select fqn:lonely,package:pkg,file:lonely.sql,resource_type:model --target dev'
    ]


def test_versioned_models_sharing_a_bare_name_still_generate(dbt_factory):
    # A bare `orders` matches *every* version — confirmed with `dbt ls` on dbt 1.12.0, where `orders`
    # returns both model.probe.orders.v1 and .v2, while `orders.v1` and `orders_v1` return one each
    # (dbt lets the last two fqn segments match on either delimiter). Each version's own fqn is exact,
    # so both tasks must still generate; the mirror seeing only one of them would let a genuine
    # collision through elsewhere.
    nodes = dict(
        [
            _model('pkg', 'orders', fqn=['pkg', 'orders', 'v1'], version=1, path='models/orders_v1.sql'),
            _model('pkg', 'orders', fqn=['pkg', 'orders', 'v2'], version=2, path='models/orders_v2.sql'),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    commands = {t['task_key']: t['dbt_task']['commands'][0] for t in tasks}

    assert commands == {
        'orders_v1_model': 'dbt run --select fqn:pkg.orders.v1,package:pkg,file:orders_v1.sql,resource_type:model'
        ' --target dev',
        'orders_v2_model': 'dbt run --select fqn:pkg.orders.v2,package:pkg,file:orders_v2.sql,resource_type:model'
        ' --target dev',
    }


def test_windows_manifest_path_yields_the_bare_file_name(dbt_factory):
    # dbt builds `original_file_path` with os.path.join, so a manifest parsed on Windows carries
    # backslashes while dbt's own FileSelectorMethod compares `Path(original_file_path).name`. Emitting
    # the whole backslash path would match nothing, so the task would build nothing and still exit 0.
    nodes = dict([_model('pkg', 'orders', fqn=['pkg', 'marts', 'orders'], path='models\\marts\\orders.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})

    assert [t['dbt_task']['commands'][0] for t in tasks] == [
        'dbt run --select fqn:pkg.marts.orders,package:pkg,file:orders.sql,resource_type:model --target dev'
    ]


def test_unit_test_clone_is_emitted_when_its_model_version_is_present(dbt_factory):
    # dbt omits a clone for a disabled model version, so a manifest may contain only one clone of a
    # versioned unit test. The remaining clone is emitted against its exact model version.
    nodes = dict([_model('pkg', 'orders', fqn=['pkg', 'orders', 'v2'], version=2, path='models/orders_v2.sql')])
    unit_tests = dict([_unit_test('pkg', 'orders', 'ut_orders', depends_on=['model.pkg.orders.v2'], version=2)])

    tasks = dbt_factory.create_tasks({'nodes': nodes, 'unit_tests': unit_tests})
    by_key = {t['task_key']: t for t in tasks}

    assert sorted(key for key in by_key if key.startswith('unit_test_')) == ['unit_test_pkg_orders_ut_orders_v2']
    assert by_key['unit_test_pkg_orders_ut_orders_v2']['depends_on'] == [{'task_key': 'orders_v2_model'}]


def test_node_passed_as_a_copy_is_not_its_own_collision(dbt_factory):
    # `_assert_exact` recognises the node by object identity *or* `unique_id`. Identity alone would make
    # a node passed as a copy count as colliding with itself, refusing a perfectly addressable resource.
    # Guards the robustness of that check rather than a dbt behaviour.
    node_id, node = _model('pkg', 'orders')
    node['unique_id'] = node_id
    manifest = {'nodes': {node_id: dict(node)}}  # a *copy* under the same id

    tasks = dbt_factory.create_tasks(manifest)

    assert [t['dbt_task']['commands'][0] for t in tasks] == [
        'dbt run --select fqn:pkg.orders,package:pkg,file:orders.sql,resource_type:model --target dev'
    ]


def test_public_factory_allows_exactly_one_thousand_tasks(dbt_factory):
    nodes = dict(_model('pkg', f'model_{index:04d}') for index in range(1_000))

    tasks = dbt_factory.create_tasks({'nodes': nodes})

    assert len(tasks) == 1_000


def test_public_factory_rejects_more_than_one_thousand_tasks(dbt_factory):
    nodes = dict(_model('pkg', f'model_{index:04d}') for index in range(1_001))

    with pytest.raises(ValueError, match=r'at most 1,000 tasks.*1,001'):
        dbt_factory.create_tasks({'nodes': nodes})


# The selector-index test reaches into internals because "scan every peer" has no public surface.
# pylint: disable=protected-access
def test_selector_index_narrowing_matches_a_full_scan(dbt_factory):
    # `_SelectorIndex` exists only to keep the exactness check off a full manifest scan, which measured
    # 90s on a 6,000-node manifest. It is a pure optimisation, so every bucket must be a superset of the
    # true matches: narrowing may cost time but must never change an answer. Asserted over a layout
    # holding each hazard the buckets are keyed on — a dotted name, a versioned model, a shared
    # `schema.yml`, and a file whose stem is another file's name (`a.yml` / `a.yml.yml`).
    peers = dict(
        [
            _model('pkg', 'orders', fqn=['pkg', 'orders', 'v1'], version=1, path='models/orders_v1.sql'),
            _model('pkg', 'orders.items', fqn=['pkg', 'orders.items'], path='models/a.yml.yml'),
            _model('pkg', 'items', fqn=['pkg', 'orders', 'items'], path='models/a.yml'),
            _test('pkg', 'chk', ['model.pkg.items'], path='models/schema.yml', test_name='not_null'),
            _test('pkg', 'chk.nested', ['model.pkg.orders.items'], path='models/schema.yml', test_name='not_null'),
        ]
    )
    index = DbtFactory._selector_index(peers)

    for info in peers.values():
        select = DbtFactory._node_select(info)
        scanned = sorted(DbtFactory._matching_ids(select, peers))
        narrowed = sorted(DbtFactory._matching_ids(select, index))
        assert narrowed == scanned, f'{select!r} narrowed to {narrowed}, full scan gives {scanned}'


def test_source_term_matches_only_the_named_source():
    peers = dict(
        [
            _source('pkg', 'raw', 'orders'),
            _source('pkg', 'raw', 'customers'),
            _source('other', 'raw', 'orders'),
            _model('pkg', 'orders'),
        ]
    )

    assert DbtFactory._matching_ids('source:pkg.raw.orders', peers) == ['source.pkg.raw.orders']


def test_a_selector_that_reaches_nothing_is_refused(dbt_factory):
    # The exactness check is an *equality*, not "no surplus". A selector matching nothing is as wrong as
    # one matching too much, and far easier to miss: `dbt test` and `dbt run` both exit 0 on a zero-match
    # selector, so the task would go green having asserted or built nothing at all.
    #
    # Provoked by a node whose manifest entry disagrees with itself — the emitted `package:pkg` cannot
    # reach a node the manifest files under a different package. That is a selector-construction bug
    # rather than a project problem, so the message says so.
    node = {
        'resource_type': 'model',
        'unique_id': 'model.pkg.orders',
        'name': 'orders',
        'package_name': 'pkg',
        'fqn': ['pkg', 'orders'],
        'original_file_path': 'models/orders.sql',
        'depends_on': {'nodes': []},
    }
    peers = DbtFactory._selector_index({'model.pkg.orders': dict(node, package_name='other')})

    with pytest.raises(ValueError, match='does not reach model.pkg.orders'):
        DbtFactory._node_select(node, peers=peers)


def test_gating_test_deps_are_ordered_deterministically_across_processes():
    """
    `depends_on` must be a function of the node ids alone, as `Utils.build_task_key_maps` documents for
    task keys. Extending a node's deps walked its ancestors as a `set`, so the order the gating test keys
    were appended in varied with `PYTHONHASHSEED` — one manifest produced six orderings across eight
    seeds. The generated spec is checked in, so that is a spurious diff on every regeneration.

    Run in subprocesses because the seed is fixed at interpreter start-up.
    """
    script = (
        textwrap.dedent(
            """
        import json, sys
        sys.path.insert(0, %r)
        from conftest import create_dbt_factory

        def model(name, deps=()):
            return f'model.pkg.{name}', {'resource_type': 'model', 'name': name, 'package_name': 'pkg',
                'fqn': ['pkg', name], 'original_file_path': f'models/{name}.sql',
                'depends_on': {'nodes': list(deps)}}

        def test(name, deps):
            return f'test.pkg.{name}', {'resource_type': 'test', 'name': name, 'package_name': 'pkg',
                'fqn': ['pkg', name], 'original_file_path': f'models/{name}.yml',
                'depends_on': {'nodes': list(deps)}, 'config': {'severity': 'error'}}

        # Three tests become eligible at the same first frontier, so their append order is observable.
        nodes = dict([
            model('a'), model('b', ['model.pkg.a']), model('c', ['model.pkg.b']),
            model('d', ['model.pkg.c']), model('e', ['model.pkg.d']),
            test('t_z', ['model.pkg.d']), test('t_a', ['model.pkg.d']), test('t_m', ['model.pkg.d']),
        ])
        tasks = create_dbt_factory().create_tasks({'nodes': nodes})
        by_key = {t['task_key']: [d['task_key'] for d in (t.get('depends_on') or [])] for t in tasks}
        print(json.dumps(by_key['e_model']))
        """
        )
        % BASE_PATH
    )

    orderings = set()
    for seed in range(8):
        result = subprocess.run(  # noqa: S603
            [sys.executable, '-c', script],
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, 'PYTHONHASHSEED': str(seed)},
            cwd=str(Path(BASE_PATH).parent),
        )
        assert result.returncode == 0, f'seed {seed} failed:\n{result.stderr}'
        orderings.add(result.stdout.strip())

    assert orderings == {'["d_model", "t_a_test", "t_m_test", "t_z_test"]'}


@pytest.mark.parametrize('name', ['@weird', '2+orders'], ids=['at-prefix', 'numeric-prefix'])
def test_leading_graph_operators_are_literal_under_an_explicit_fqn(dbt_factory, name):
    # dbt's `RAW_SELECTOR_PATTERN` reads a leading `@` or `N+` as a graph operator only when it is
    # inferring the method. Naming the method makes them literal: `fqn:@weird` and `fqn:2+orders` each
    # resolve to exactly their node — verified with `dbt ls` on dbt 1.12.0 in a project where the models
    # have children, so an operator would have visibly expanded the result.
    #
    # A *trailing* `+N` is different and still refused; see
    # `test_name_fallback_ending_in_a_graph_operator_is_refused`.
    nodes = dict([_model('pkg', name, fqn=['pkg', name], path=f'models/{name}.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})

    assert [t['dbt_task']['commands'][0] for t in tasks] == [
        f'dbt run --select fqn:pkg.{name},package:pkg,file:{name}.sql,resource_type:model --target dev'
    ]


def test_bundled_test_for_a_parent_without_a_task_factory_is_refused():
    # `_node_gets_own_task` gates on `resource_type not in task_factories`, so a library caller may
    # legitimately register a subset. In bundled mode the tested parent's task key was then looked up
    # unconditionally, raising `KeyError` — which escapes `main`'s `except (ValueError, FileNotFoundError)`
    # and prints a traceback instead of naming the resource. Per-test mode handles the same manifest.
    resolver = DbtDependencyResolver()
    task_options = DbtTaskOptions(source="GIT", environment_key="Default", task_type="dbt")
    factory = DbtFactory(
        {'test': DbtTestTaskFactory(resolver, task_options, "--target dev")},
        bundle_tests=True,
    )
    seed_name, seed_info = _seed('pkg', 'countries')
    test_name, test_info = _test('pkg', 'not_null_countries_id', [seed_name], test_name='not_null')

    with pytest.raises(ValueError, match=seed_name):
        factory.create_tasks({'nodes': {seed_name: seed_info, test_name: test_info}})
