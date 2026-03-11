def generate_task_key(dbt_node_full_name: str, parent_model_name: str | None = None) -> str:
    """Generates a Databricks job task key from a fully qualified dbt node name.

    Models use just the node name, tests are prefixed with the parent model name,
    and other types (seed, snapshot) are prefixed with the resource type.

    Args:
        dbt_node_full_name: Fully qualified dbt node name (e.g. "model.project.daily_orders").
        parent_model_name: For test nodes, the name of the parent model.
    """
    parts = dbt_node_full_name.split('.')
    if len(parts) < 3:
        return dbt_node_full_name.replace('.', '_')

    resource_type, _, node_name = parts[0], parts[1], parts[2]

    if resource_type == "model":
        return node_name
    if resource_type == "test":
        prefix = parent_model_name or "test"
        return f"{prefix}_test_{node_name}"
    return f"{resource_type}_{node_name}"
