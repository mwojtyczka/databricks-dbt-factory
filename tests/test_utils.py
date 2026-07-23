from databricks_dbt_factory.Utils import (
    generate_task_key,
    bundled_test_key,
    build_task_key_maps,
    read_dbt_manifest,
)


def test_generate_task_key_readable_per_type():
    assert generate_task_key('model.shop.orders') == 'orders_run'
    assert generate_task_key('seed.shop.countries') == 'countries_seed'
    assert generate_task_key('snapshot.shop.orders_snap') == 'orders_snap_snapshot'
    assert generate_task_key('test.shop.unique_orders_id.9a1') == 'unique_orders_id_test'
    assert generate_task_key('source.shop.raw.customers') == 'raw_customers_test'


def test_generate_task_key_keeps_model_version():
    assert generate_task_key('model.shop.dim.v2') == 'dim_v2_run'


def test_build_task_key_maps_disambiguates_collisions():
    # Same model name in two packages -> keys must differ.
    task_keys, _ = build_task_key_maps(['model.a.orders', 'model.b.orders'])
    assert task_keys['model.a.orders'] != task_keys['model.b.orders']
    assert set(task_keys.values()) == {'a_orders_run', 'b_orders_run'}


def test_build_task_key_maps_unique_names_stay_plain():
    task_keys, _ = build_task_key_maps(['model.a.orders', 'model.a.customers'])
    assert task_keys == {'model.a.orders': 'orders_run', 'model.a.customers': 'customers_run'}


def test_bundled_test_key():
    assert bundled_test_key('model.shop.orders') == 'orders_test'
    assert bundled_test_key('source.shop.raw.customers') == 'raw_customers_test'


def test_read_dbt_manifest_roundtrip(tmp_path):
    p = tmp_path / 'manifest.json'
    p.write_text('{"nodes": {}}', encoding='utf-8')
    assert read_dbt_manifest(str(p)) == {'nodes': {}}
