from typing import Iterable


def generate_task_key(dbt_node_full_name: str) -> str:
    """
    Generates the short, preferred task key for a dbt node's fully qualified name.

    Models use the bare node name; other resource types are prefixed with the resource type
    (e.g. `test_foo`, `seed_countries`, `source_raw_customers`). The package segment is dropped
    — callers that need collision-safe keys should go through `build_task_key_map`.

    Args:
        dbt_node_full_name (str): Fully qualified dbt node name, e.g. `model.my_project.customers`.

    Returns:
        str: The short task key.
    """
    parts = dbt_node_full_name.split('.')
    if len(parts) < 3:
        return dbt_node_full_name.replace('.', '_')

    resource_type, _package, *rest = parts
    node_name = '_'.join(rest)

    if resource_type == "model":
        return node_name
    return f"{resource_type}_{node_name}"


def _qualified_task_key(dbt_node_full_name: str) -> str:
    """
    Generates the package-qualified task key used as a collision fallback.

    For models, the form is `{package}_{node_name}`; for other resource types it's
    `{resource_type}_{package}_{node_name}`. Only called by `build_task_key_map` when two
    nodes collide on their short key.

    Args:
        dbt_node_full_name (str): Fully qualified dbt node name.

    Returns:
        str: The qualified task key.
    """
    parts = dbt_node_full_name.split('.')
    if len(parts) < 3:
        return dbt_node_full_name.replace('.', '_')

    resource_type, package, *rest = parts
    node_name = '_'.join(rest)

    if resource_type == "model":
        return f"{package}_{node_name}"
    return f"{resource_type}_{package}_{node_name}"


def build_task_key_map(full_names: Iterable[str]) -> dict[str, str]:
    """
    Builds a mapping from dbt node full names to unique Databricks task keys.

    Uses the short key from `generate_task_key` by default; when two nodes collide on a short
    key, only the colliding nodes fall back to the package-qualified form from
    `_qualified_task_key`. Raises `ValueError` if the qualified form still collides (extremely
    rare — catches edge cases where a qualified key of one node matches an unrelated node's
    short key).

    Args:
        full_names (Iterable[str]): Fully qualified dbt node names (from `manifest['nodes']`
            and/or `manifest['sources']`).

    Returns:
        dict[str, str]: Mapping from full name to unique task key.
    """
    short_keys: dict[str, str] = {full_name: generate_task_key(full_name) for full_name in full_names}

    by_short: dict[str, list[str]] = {}
    for full_name, short_key in short_keys.items():
        by_short.setdefault(short_key, []).append(full_name)

    final: dict[str, str] = {}
    for short_key, full_names_for_key in by_short.items():
        if len(full_names_for_key) == 1:
            final[full_names_for_key[0]] = short_key
        else:
            for full_name in full_names_for_key:
                final[full_name] = _qualified_task_key(full_name)

    seen: dict[str, str] = {}
    for full_name, key in final.items():
        if key in seen:
            raise ValueError(
                f"Unable to generate unique task_key {key!r}: collides between {seen[key]!r} and {full_name!r}"
            )
        seen[key] = full_name

    return final
