import os
from tempfile import NamedTemporaryFile
from pathlib import Path
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
) -> tuple[str, dict]:
    # A versioned model's unique_id carries its version: model.<pkg>.<name>.v<N>.
    full_name = f"model.{package}.{name}" + (f".v{version}" if version is not None else "")
    info: dict = {
        'resource_type': 'model',
        'name': name,
        'package_name': package,
        'fqn': fqn or [package, name],
        'depends_on': {'nodes': depends_on or []},
    }
    if version is not None:
        info['version'] = version
    return full_name, info


def _test(
    package: str, name: str, depends_on: list[str], severity: str = 'error', fqn: list[str] | None = None
) -> tuple[str, dict]:
    full_name = f"test.{package}.{name}"
    return full_name, {
        'resource_type': 'test',
        'name': name,
        'package_name': package,
        'fqn': fqn or [package, name],
        'depends_on': {'nodes': depends_on},
        'config': {'severity': severity},
    }


def _seed(package: str, name: str, fqn: list[str] | None = None) -> tuple[str, dict]:
    full_name = f"seed.{package}.{name}"
    return full_name, {
        'resource_type': 'seed',
        'name': name,
        'package_name': package,
        'fqn': fqn or [package, name],
        'depends_on': {'nodes': []},
    }


def _snapshot(
    package: str, name: str, depends_on: list[str] | None = None, fqn: list[str] | None = None
) -> tuple[str, dict]:
    full_name = f"snapshot.{package}.{name}"
    return full_name, {
        'resource_type': 'snapshot',
        'name': name,
        'package_name': package,
        'fqn': fqn or [package, name],
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
    }


def _unit_test(
    package: str, model: str, name: str, fqn: list[str] | None = None, depends_on: list[str] | None = None
) -> tuple[str, dict]:
    full_name = f"unit_test.{package}.{model}.{name}"
    return full_name, {
        'resource_type': 'unit_test',
        'name': name,
        'model': model,
        'package_name': package,
        'fqn': fqn or [package, model, name],
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
        'dbt test --select pkg_a.customers --indirect-selection cautious --target dev'
    ]
    assert by_key['pkg_b_customers_test']['dbt_task']['commands'] == [
        'dbt test --select pkg_b.customers --indirect-selection cautious --target dev'
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
        'dbt test --select pkg.countries --indirect-selection cautious --target dev'
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
        'dbt test --select pkg.orders_snap --indirect-selection cautious --target dev'
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
        'dbt test --select pkg.unique_customers_id --target dev'
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
        'dbt test --select pkg.team_cities --indirect-selection cautious --target dev'
    ]

    # Cross-model test → its own task, gated on BOTH referenced models
    cross_test_key = 'relationships_game_details_winner__team_city__ref_team_cities__test'
    assert cross_test_key in by_key
    assert by_key[cross_test_key]['dbt_task']['commands'] == [
        'dbt test --select pkg.relationships_game_details_winner__team_city__ref_team_cities_ --target dev'
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
        'dbt test --select pkg_a.customers --indirect-selection cautious --target dev'
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

    assert by_key['pkg_a_customers_model']['dbt_task']['commands'] == ['dbt run --select pkg_a.customers --target dev']
    assert by_key['pkg_b_customers_model']['dbt_task']['commands'] == ['dbt run --select pkg_b.customers --target dev']


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
        'dbt run --select pkg.staging.stg_orders --target dev'
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
        'dbt test --select pkg.staging.stg_orders --indirect-selection cautious --target dev'
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
        'dbt test --select pkg.staging.orders.test_totals --target dev'
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
        'dbt test --select orders,pkg.staging.orders --indirect-selection cautious --target dev'
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


def test_select_is_plain_fqn_when_no_node_is_nested_beneath_it(dbt_factory):
    # The name pin is only needed to separate a node from nodes nested *beneath* its fqn. When no
    # such node exists — the overwhelmingly common case — the plain fqn already matches exactly one
    # node, so the selector stays unpinned and readable.
    nodes = dict(
        [
            _model('pkg', 'orders', fqn=['pkg', 'staging', 'orders']),
            _model('pkg', 'customers', fqn=['pkg', 'staging', 'customers']),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_model']['dbt_task']['commands'] == ['dbt run --select pkg.staging.orders --target dev']
    assert by_key['customers_model']['dbt_task']['commands'] == ['dbt run --select pkg.staging.customers --target dev']


def test_select_of_nested_node_itself_stays_unpinned(dbt_factory):
    # Only the *ancestor* needs pinning. `items` has nothing beneath it, so its own selector is
    # already unambiguous and stays plain.
    nodes = dict(
        [
            _model('pkg', 'orders', fqn=['pkg', 'staging', 'orders']),
            _model('pkg', 'items', fqn=['pkg', 'staging', 'orders', 'items']),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['items_model']['dbt_task']['commands'] == ['dbt run --select pkg.staging.orders.items --target dev']


def test_select_does_not_match_models_nested_under_a_sibling_name(dbt_factory):
    # `models/staging/orders.sql` (fqn pkg.staging.orders) and `models/staging/orders/items.sql`
    # (fqn pkg.staging.orders.items). dbt matches an fqn selector as a positional *prefix*, so a
    # bare `pkg.staging.orders` selects `items` too — building it inside the wrong task, ahead of
    # its own upstreams. Intersecting with the leaf name pins the selector to one node.
    nodes = dict(
        [
            _model('pkg', 'orders', fqn=['pkg', 'staging', 'orders']),
            _model('pkg', 'items', fqn=['pkg', 'staging', 'orders', 'items']),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_model']['dbt_task']['commands'] == ['dbt run --select orders,pkg.staging.orders --target dev']


def test_bundled_test_select_does_not_match_models_nested_under_a_sibling_name(dbt_factory_bundled):
    # Same prefix-overlap hazard for the bundled `<model>_test` task: without the leaf-name
    # intersection, `orders_test` would sweep in `items`' tests as well.
    nodes = dict(
        [
            _model('pkg', 'orders', fqn=['pkg', 'staging', 'orders']),
            _model('pkg', 'items', fqn=['pkg', 'staging', 'orders', 'items']),
            _test('pkg', 'unique_orders_id', ['model.pkg.orders']),
        ]
    )

    tasks = dbt_factory_bundled.create_tasks({'nodes': nodes})
    by_key = {t['task_key']: t for t in tasks}

    assert by_key['orders_test']['dbt_task']['commands'] == [
        'dbt test --select orders,pkg.staging.orders --indirect-selection cautious --target dev'
    ]


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
                fqn=['pkg', 'marts', 'dim', 'ut_a_v1'],
                depends_on=['model.pkg.dim.v1'],
            ),
            _unit_test(
                'pkg',
                'dim',
                'ut_a_v2',
                fqn=['pkg', 'marts', 'dim', 'ut_a_v2'],
                depends_on=['model.pkg.dim.v2'],
            ),
        ]
    )

    tasks = dbt_factory.create_tasks({'nodes': nodes, 'unit_tests': unit_tests})
    by_key = {t['task_key']: t for t in tasks}

    # Each versioned unit test gets its own task, gated on the matching model version.
    assert by_key['unit_test_pkg_dim_ut_a_v1']['depends_on'] == [{'task_key': 'dim_v1_model'}]
    assert by_key['unit_test_pkg_dim_ut_a_v2']['depends_on'] == [{'task_key': 'dim_v2_model'}]


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
                fqn=['pkg', 'marts', 'dim', 'ut_a_v2'],
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
