import os
import shlex
from tempfile import NamedTemporaryFile
from pathlib import Path
import pytest
import yaml

from databricks_dbt_factory.job_spec import replace_tasks_in_job_spec
from databricks_dbt_factory.TaskFactory import DbtDependencyResolver
from databricks_dbt_factory.Utils import read_dbt_manifest


BASE_PATH = str(Path(__file__).resolve().parent)


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


def _source(package: str, source_name: str, table: str) -> tuple[str, dict]:
    full_name = f"source.{package}.{source_name}.{table}"
    return full_name, {
        'resource_type': 'source',
        'name': table,
        'source_name': source_name,
        'package_name': package,
        'fqn': [package, source_name, table],
        'original_file_path': f"models/{source_name}.yml",
    }


def _unit_test(
    package: str,
    model: str,
    name: str,
    fqn: list[str] | None = None,
    depends_on: list[str] | None = None,
    path: str | None = None,
) -> tuple[str, dict]:
    full_name = f"unit_test.{package}.{model}.{name}"
    return full_name, {
        'resource_type': 'unit_test',
        'name': name,
        'model': model,
        'package_name': package,
        'fqn': fqn or [package, model, name],
        # dbt declares unit tests in a .yml alongside the model.
        'original_file_path': path or f"models/{model}_unit_tests.yml",
        'depends_on': {'nodes': depends_on or [f"model.{package}.{model}"]},
    }


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
        'dbt test --select pkg_a.customers,package:pkg_a,file:customers.sql --indirect-selection cautious --target dev'
    ]
    assert by_key['pkg_b_customers_test']['dbt_task']['commands'] == [
        'dbt test --select pkg_b.customers,package:pkg_b,file:customers.sql --indirect-selection cautious --target dev'
    ]
    assert by_key['pkg_a_customers_test']['depends_on'] == [{'task_key': 'pkg_a_customers_model'}]
    assert by_key['pkg_b_customers_test']['depends_on'] == [{'task_key': 'pkg_b_customers_model'}]

    assert {dep['task_key'] for dep in by_key['orders_model']['depends_on']} == {
        'pkg_a_customers_test',
        'pkg_b_customers_test',
    }


def test_bundle_mode_model_depending_on_single_model_test_does_not_raise(dbt_factory_bundled):
    # In bundle mode, single-model test nodes fold into their resource's bundled task and get no
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
        'dbt test --select pkg.countries,package:pkg,file:countries.csv --indirect-selection cautious --target dev'
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
        'dbt test --select pkg.orders_snap,package:pkg,file:orders_snap.sql --indirect-selection cautious --target dev'
    ]
    assert by_key['orders_snap_test']['depends_on'] == [{'task_key': 'orders_snap_snapshot'}]
    assert by_key['orders_history_model']['depends_on'] == [{'task_key': 'orders_snap_test'}]


def test_tests_on_source_produce_standalone_task(dbt_factory_bundled):
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
        'dbt test --select source:pkg.raw.customers --indirect-selection cautious --target dev'
    ]
    assert by_key['raw_customers_test']['depends_on'] == []


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
        'dbt test --select pkg.unique_customers_id,package:pkg,file:unique_customers_id.yml --target dev'
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


def test_flat_mode_warn_severity_tests_do_not_gate_downstream(dbt_factory):
    # Only error-severity tests gate downstream (matches `dbt build`: dbt exits 0 on warn
    # so a gate wouldn't block anyway; we just keep the DAG cleaner).
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

    # Both test tasks still exist (warn tests still run — they just don't gate anything)
    assert 'unique_customers_id_test' in by_key
    assert 'not_null_customers_id_test' in by_key

    # orders gates on customers + the error-severity test, but NOT the warn-severity one
    assert {dep['task_key'] for dep in by_key['orders_model']['depends_on']} == {
        'customers_model',
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
        select='pkg.customers',
        deps_command_name='customers',
        depends_on=['customers_model'],
    )
    assert task.task_key == 'customers_test'
    # `select` is passed through verbatim here — building it is `_fqn_select`'s job, not the factory's.
    assert task.commands == ['dbt test --select pkg.customers --indirect-selection cautious --target dev']
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

    # Single-model test → bundled with cautious selection (relationship test is excluded by dbt)
    assert 'team_cities_test' in by_key
    assert by_key['team_cities_test']['dbt_task']['commands'] == [
        'dbt test --select pkg.team_cities,package:pkg,file:team_cities.sql --indirect-selection cautious --target dev'
    ]

    # Cross-model test → its own task, gated on BOTH referenced models
    cross_test_key = 'relationships_game_details_winner__team_city__ref_team_cities__test'
    assert cross_test_key in by_key
    assert by_key[cross_test_key]['dbt_task']['commands'] == [
        'dbt test --select pkg.relationships_game_details_winner__team_city__ref_team_cities_,package:pkg,file:relationships_game_details_winner__team_city__ref_team_cities_.yml --target dev'
    ]
    assert {dep['task_key'] for dep in by_key[cross_test_key]['depends_on']} == {
        'team_cities_model',
        'game_details_model',
    }

    # `game_details` has no single-model tests, so no bundled `game_details_tests` exists
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
        'dbt test --select pkg_a.customers,package:pkg_a,file:customers.sql --indirect-selection cautious --target dev'
    ]
    assert by_key['orders_model']['depends_on'] == [{'task_key': 'customers_test'}]


def test_duplicate_model_name_across_packages_selects_by_distinct_fqn(dbt_factory):
    # Two packages define a model named `customers`. Selecting by the bare name would make both
    # tasks run `dbt run --select customers`, executing both models from each task. The full FQN
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
        'dbt run --select pkg_a.customers,package:pkg_a,file:customers.sql --target dev'
    ]
    assert by_key['pkg_b_customers_model']['dbt_task']['commands'] == [
        'dbt run --select pkg_b.customers,package:pkg_b,file:customers.sql --target dev'
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
        'dbt run --select pkg.staging.stg_orders,package:pkg,file:stg_orders.sql --target dev'
    ]


def test_model_in_subdirectory_bundled_test_selects_by_full_fqn(dbt_factory_bundled):
    # In bundled mode the `<model>_test` task's select must be the model's full fqn so
    # `dbt test --select <model_fqn> --indirect-selection cautious` actually matches the subdirectory
    # model. `pkg.stg_orders` would match no nodes and silently run zero tests.
    nodes = dict(
        [
            _model('pkg', 'stg_orders', fqn=['pkg', 'staging', 'stg_orders']),
            _test('pkg', 'unique_stg_orders_id', ['model.pkg.stg_orders']),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['stg_orders_test']['dbt_task']['commands'] == [
        'dbt test --select pkg.staging.stg_orders,package:pkg,file:stg_orders.sql --indirect-selection cautious --target dev'
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
        'dbt test --select pkg.staging.orders.test_totals,package:pkg,file:orders_unit_tests.yml --target dev'
    ]
    assert by_key[unit_test_key]['depends_on'] == [{'task_key': 'orders_model'}]
    # summary (downstream of orders) gates on the unit test as well as the model
    assert {dep['task_key'] for dep in by_key['summary_model']['depends_on']} == {
        'orders_model',
        unit_test_key,
    }


def test_bundled_mode_model_with_only_unit_test_emits_bundled_task(dbt_factory_bundled):
    # A model whose only test is a unit test (no data test) must still get a bundled
    # `<model>_test` task. `dbt test --select <model_fqn> --indirect-selection cautious` sweeps
    # in the unit test, so it is not silently dropped.
    nodes = dict([_model('pkg', 'orders', fqn=['pkg', 'staging', 'orders'])])
    unit_tests = dict([_unit_test('pkg', 'orders', 'test_totals', fqn=['pkg', 'staging', 'orders', 'test_totals'])])

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes, 'unit_tests': unit_tests})
    by_key = {t['task_key']: t for t in tasks}

    assert 'orders_test' in by_key
    assert by_key['orders_test']['dbt_task']['commands'] == [
        'dbt test --select pkg.staging.orders,package:pkg,file:orders.sql --indirect-selection cautious --target dev'
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
    assert DbtDependencyResolver.resolve(node, ["model"], task_keys) == ["a_orders_model"]


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
        'dbt run --select pkg.marts.orders,package:pkg,file:orders.sql --target dev'
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
        'dbt run --select pkg.marts.orders,package:pkg,file:orders.sql --target dev'
    ]
    assert by_key['items_model']['dbt_task']['commands'] == [
        'dbt run --select pkg.marts.orders.items,package:pkg,file:items.sql --target dev'
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

    assert commands['probe_alpha_model'] == 'dbt run --select probe.alpha,package:probe,file:alpha.sql --target dev'
    assert commands['other_alpha_model'] == (
        'dbt run --select other.probe.alpha,package:other,file:alpha.sql --target dev'
    )


def test_bundled_test_select_uses_the_same_uniform_form(dbt_factory_bundled):
    nodes = dict(
        [
            _model('pkg', 'orders', fqn=['pkg', 'marts', 'orders'], path='models/marts/orders.sql'),
            _test('pkg', 'unique_orders_id', ['model.pkg.orders'], path='models/marts/schema.yml'),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_test']['dbt_task']['commands'] == [
        'dbt test --select pkg.marts.orders,package:pkg,file:orders.sql --indirect-selection cautious --target dev'
    ]


@pytest.mark.parametrize(
    ('bad_segment', 'reason'),
    [
        pytest.param('my orders', 'space is a union separator', id='space'),
        pytest.param('orders,archive', 'comma is an intersection separator', id='comma'),
        pytest.param('or[der]s', 'brackets are fnmatch syntax', id='brackets'),
        pytest.param('star*model', 'star is fnmatch syntax', id='star'),
        pytest.param('q?model', 'question mark is fnmatch syntax', id='question-mark'),
        pytest.param('colon:model', 'colon is a method prefix', id='colon'),
    ],
)
def test_unusable_fqn_segment_is_dropped_but_other_terms_remain(dbt_factory, bad_segment, reason):
    # A directory whose name dbt cannot express in a selector only costs us the fqn term. The
    # remaining terms still address the node, so generation succeeds rather than failing outright.
    # (`reason` documents which dbt rule each character trips.)
    assert reason
    nodes = dict([_model('pkg', 'orders', fqn=['pkg', bad_segment, 'orders'], path=f'models/{bad_segment}/orders.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_model']['dbt_task']['commands'] == [
        'dbt run --select orders,package:pkg,file:orders.sql --target dev'
    ]


def test_fqn_ending_in_a_graph_operator_is_dropped(dbt_factory):
    # dbt reads a trailing `+N` as child depth, so `pkg.orders+1` selects `pkg.orders` and its
    # children — the wrong model entirely. The bare name `orders+1` is no better: on its own it *is*
    # the whole raw selector, so it trips the same rule and selects nothing. Both are dropped, and
    # `package:`/`file:` still address the node.
    nodes = dict([_model('pkg', 'orders+1', fqn=['pkg', 'orders+1'], path='models/orders+1.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders+1_model']['dbt_task']['commands'] == [
        'dbt run --select package:pkg,file:orders+1.sql --target dev'
    ]


def test_fqn_starting_with_a_numeric_graph_operator_is_dropped(dbt_factory):
    # dbt's `RAW_SELECTOR_PATTERN` reads a *leading* `N+` as parent depth, not just a trailing `+N`.
    # Confirmed with `dbt ls` on dbt 1.12.0: `2+orders` selects `probe.orders` (the sibling, plus two
    # levels of its parents) — a wrong-node hit, not an empty one — so the bare name must be dropped.
    # The fqn `pkg.2+orders` is exact, because the operator is no longer at the selector's boundary.
    nodes = dict([_model('pkg', '2+orders', fqn=['pkg', '2+orders'], path='models/2+orders.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['2+orders_model']['dbt_task']['commands'] == [
        'dbt run --select pkg.2+orders,package:pkg,file:2+orders.sql --target dev'
    ]


def test_name_fallback_starting_with_a_numeric_graph_operator_is_refused(dbt_factory):
    # The same name with an unusable fqn (a spacey directory) has nothing left to fall back on: the
    # bare name `2+orders` *is* the whole raw selector, so dbt reads the leading `2+` as parent depth
    # and selects a different model entirely. Two tests share the schema.yml, so `file:` cannot stand
    # in for the name either — refuse rather than emit a task that runs the wrong node.
    nodes = dict(
        [
            _test(
                'pkg',
                '2+orders',
                ['model.pkg.a'],
                fqn=['pkg', 'my dir', '2+orders'],
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

    with pytest.raises(ValueError, match='no selector can address'):
        dbt_factory.create_tasks({'nodes': nodes})


def test_name_fallback_with_an_operator_is_kept_when_the_file_is_its_own(dbt_factory):
    # The mirror image: the same unusable name, but the resource has its file to itself, so
    # `package:`+`file:` addresses it exactly (`package:probe,file:orders+1.sql` -> one node,
    # confirmed with `dbt ls` on dbt 1.12.0). Refusing here would reject a project dbt handles.
    nodes = dict([_model('pkg', '2+orders', fqn=['pkg', 'my dir', '2+orders'], path='models/my dir/2+orders.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['2+orders_model']['dbt_task']['commands'] == [
        'dbt run --select package:pkg,file:2+orders.sql --target dev'
    ]


def test_source_selector_with_a_graph_operator_is_refused(dbt_factory_bundled):
    # The whole `source:...` string is one raw selector, so it is subject to the same boundary rule as
    # a bare name — but `_source_select` only screened metacharacters. Confirmed with `dbt ls` on dbt
    # 1.12.0: `source:pkg.raw.orders+1` matches nothing and `dbt test` still exits 0, so the source's
    # tests would silently never run.
    nodes = dict([_test('pkg', 'source_not_null_raw_orders_id', ['source.pkg.raw.orders+1'])])
    sources = dict([_source('pkg', 'raw', 'orders+1')])

    with pytest.raises(ValueError, match='no selector can address'):
        dbt_factory_bundled.create_tasks({'nodes': nodes, 'sources': sources})


def test_source_selector_keeps_an_operator_away_from_the_boundary(dbt_factory_bundled):
    # Only the boundary matters. `source:pkg.raw.2+ord` puts the `2+` mid-string, and dbt resolves it
    # exactly (verified with `dbt ls` on dbt 1.12.0), so refusing it would reject a working project.
    nodes = dict([_test('pkg', 'source_not_null_raw_2_ord_id', ['source.pkg.raw.2+ord'])])
    sources = dict([_source('pkg', 'raw', '2+ord')])

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes, 'sources': sources})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['raw_2+ord_test']['dbt_task']['commands'] == [
        'dbt test --select source:pkg.raw.2+ord --indirect-selection cautious --target dev'
    ]


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

    with pytest.raises(ValueError, match='no selector can address'):
        dbt_factory.create_tasks({'nodes': nodes})


def test_graph_operator_inside_a_segment_is_kept(dbt_factory):
    # `@` and `+` are only operators at the selector's start/end. `pkg.+leading` is exact, verified
    # with `dbt ls`, so rejecting it would refuse a project dbt handles fine.
    nodes = dict([_model('pkg', '+leading', fqn=['pkg', '+leading'], path='models/+leading.sql')])

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['+leading_model']['dbt_task']['commands'] == [
        'dbt run --select pkg.+leading,package:pkg,file:+leading.sql --target dev'
    ]


def test_generation_fails_when_no_term_can_address_the_node(dbt_factory):
    # When the resource name itself is unusable, every term carries it — the fqn leaf, the file name,
    # and only `package:` survives, which matches the whole package. Emitting that would build every
    # model in the package, so generation fails loudly with the node and the remedy named.
    nodes = dict([_model('pkg', 'or[der]s', fqn=['pkg', 'or[der]s'], path='models/or[der]s.sql')])

    with pytest.raises(ValueError, match='no selector can address'):
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
        'dbt test --select not_null_a_id,package:pkg,file:schema.yml,test_name:not_null --target dev'
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
        'dbt test --select pkg.marts.not_null_a_id,package:pkg,file:schema.yml,test_name:not_null --target dev'
    ]


def test_model_and_singular_test_sharing_an_fqn_both_generate(dbt_factory):
    # `models/beta.sql` and `tests/beta.sql` parse with the same fqn and base name. Each task carries
    # its own `dbt run` / `dbt test` verb, and dbt's resource-type filtering keeps them apart, so
    # both are addressable and neither should be refused.
    nodes = dict(
        [
            _model('pkg', 'beta', fqn=['pkg', 'beta'], path='models/beta.sql'),
            _test('pkg', 'beta', [], fqn=['pkg', 'beta'], path='tests/beta.sql'),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    commands = {t['task_key']: t['dbt_task']['commands'][0] for t in tasks}

    assert commands['beta_model'] == 'dbt run --select pkg.beta,package:pkg,file:beta.sql --target dev'
    assert commands['beta_test'].startswith('dbt test --select pkg.beta,package:pkg,file:beta.sql')


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
        "pkg.customer's,package:pkg,file:customer's.sql",
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
        'dbt run --select orders,package:pkg,file:orders.sql --target dev'
    ]


def test_bundled_source_selector_is_validated(dbt_factory_bundled):
    # dbt accepts a source named `raw,archive`, but `source:pkg.raw,archive.orders` reads the comma as
    # an intersection separator and selects zero tests while exiting 0. The source branch used to
    # format this string with no validation at all.
    nodes = dict([_test('pkg', 'unique_raw_customers_id', ['source.pkg.raw,archive.customers'])])
    sources = dict([_source('pkg', 'raw,archive', 'customers')])

    with pytest.raises(ValueError, match='no selector can address'):
        dbt_factory_bundled.create_tasks({'nodes': nodes, 'sources': sources})


@pytest.mark.parametrize(
    ('source_name', 'table'),
    [
        pytest.param('raw.v1', 'orders', id='dotted-source-name'),
        pytest.param('raw', 'orders.v1', id='dotted-table-name'),
    ],
)
def test_bundled_source_selector_rejects_a_dotted_part(dbt_factory_bundled, source_name, table):
    # `.` is the delimiter of dbt's *source* grammar, which takes at most three parts, so a dot
    # inside one part pushes the selector to four and dbt refuses it outright:
    #   source:pkg.raw.v1.orders -> "Invalid source selector value" (a Runtime Error, exit != 0)
    # Unlike an fqn segment, a dot here is not shielded by a prefix, so `_is_usable_component` --
    # which only screens dbt's *node* metacharacters -- let it through. Confirmed against dbt 1.12.0
    # with `dbt ls`; see the integration test of the same shape.
    nodes = dict([_test('pkg', 'unique_raw_orders_id', [f"source.pkg.{source_name}.{table}"])])
    sources = dict([_source('pkg', source_name, table)])

    with pytest.raises(ValueError, match='no selector can address'):
        dbt_factory_bundled.create_tasks({'nodes': nodes, 'sources': sources})


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
                'ut_a_v1',
                fqn=['pkg', 'models', 'marts', 'dim', 'ut_a'],
                depends_on=['model.pkg.dim.v1'],
            ),
            _unit_test(
                'pkg',
                'dim',
                'ut_a_v2',
                fqn=['pkg', 'models', 'marts', 'dim', 'ut_a'],
                depends_on=['model.pkg.dim.v2'],
            ),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes, 'unit_tests': unit_tests})
    by_key = {t['task_key']: t for t in tasks}

    # Each versioned unit test gets its own task, gated on the matching model version.
    assert by_key['unit_test_pkg_dim_ut_a_v1']['depends_on'] == [{'task_key': 'dim_v1_model'}]
    assert by_key['unit_test_pkg_dim_ut_a_v2']['depends_on'] == [{'task_key': 'dim_v2_model'}]
    # Known limitation: dbt clones a versioned unit test's fqn verbatim (it rewrites only
    # `unique_id`, `depends_on.nodes[0]` and `version` — see the `# fqn?` comment in dbt's
    # `process_models_for_unit_test`), so both tasks necessarily select the same thing and each
    # runs both versions' assertions. Emitting the tasks at all is the fix here; isolating them
    # needs a version-aware selector, which the fqn cannot express.
    v1_select = by_key['unit_test_pkg_dim_ut_a_v1']['dbt_task']['commands']
    v2_select = by_key['unit_test_pkg_dim_ut_a_v2']['dbt_task']['commands']
    assert (
        v1_select
        == v2_select
        == ['dbt test --select pkg.models.marts.dim.ut_a,package:pkg,file:dim_unit_tests.yml --target dev']
    )


def test_bundled_mode_unit_test_on_versioned_model_emits_bundled_task(dbt_factory_bundled):
    # Same resolution bug in bundled mode: the versioned model must land in `single_model_tested`
    # so its unit test is covered by a bundled `<model>_v<N>_test` task.
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
