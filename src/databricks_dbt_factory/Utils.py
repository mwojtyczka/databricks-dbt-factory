def generate_task_key(dbt_node_full_name: str) -> str:
    parts = dbt_node_full_name.split('.')
    if len(parts) < 3:
        return dbt_node_full_name.replace('.', '_')

    resource_type, _, node_name = parts[0], parts[1], parts[2]

    if resource_type == "model":
        return node_name
    return f"{resource_type}_{node_name}"
