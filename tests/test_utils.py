import pytest

from databricks_dbt_factory.Utils import build_task_key_map, generate_task_key


def test_generate_task_key_model_drops_package():
    assert generate_task_key("model.pkg.customers") == "customers"


def test_generate_task_key_non_model_prefixes_resource_type():
    assert generate_task_key("test.pkg.unique_id") == "test_unique_id"
    assert generate_task_key("seed.pkg.countries") == "seed_countries"


def test_generate_task_key_short_name_falls_back_to_replace():
    assert generate_task_key("badinput") == "badinput"
    assert generate_task_key("model.pkg") == "model_pkg"


def test_build_task_key_map_no_collisions_uses_short_keys():
    nodes = {
        "model.pkg.customers": {},
        "model.pkg.orders": {},
        "seed.pkg.countries": {},
    }
    result = build_task_key_map(nodes)
    assert result == {
        "model.pkg.customers": "customers",
        "model.pkg.orders": "orders",
        "seed.pkg.countries": "seed_countries",
    }


def test_build_task_key_map_falls_back_to_qualified_on_model_collision():
    nodes = {
        "model.pkg_a.customers": {},
        "model.pkg_b.customers": {},
        "model.pkg_a.orders": {},
    }
    result = build_task_key_map(nodes)
    assert result == {
        "model.pkg_a.customers": "pkg_a_customers",
        "model.pkg_b.customers": "pkg_b_customers",
        "model.pkg_a.orders": "orders",
    }


def test_build_task_key_map_falls_back_on_non_model_collision():
    nodes = {
        "seed.pkg_a.countries": {},
        "seed.pkg_b.countries": {},
    }
    result = build_task_key_map(nodes)
    assert result == {
        "seed.pkg_a.countries": "seed_pkg_a_countries",
        "seed.pkg_b.countries": "seed_pkg_b_countries",
    }


def test_generate_task_key_handles_four_part_source_ids():
    assert generate_task_key("source.pkg.raw.customers") == "source_raw_customers"


def test_build_task_key_map_accepts_iterable_and_handles_sources():
    full_names = ["model.pkg.customers", "source.pkg.raw.customers"]
    result = build_task_key_map(full_names)
    assert result == {
        "model.pkg.customers": "customers",
        "source.pkg.raw.customers": "source_raw_customers",
    }


def test_build_task_key_map_raises_when_qualified_form_still_collides():
    nodes = {
        "model.pkg_a.customers": {},
        "model.pkg_b.customers": {},
        "model.other.pkg_a_customers": {},
    }
    with pytest.raises(ValueError, match="Unable to generate unique task_key"):
        build_task_key_map(nodes)
