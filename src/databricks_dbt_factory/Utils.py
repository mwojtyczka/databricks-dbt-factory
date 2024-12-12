def generate_task_key(dbt_node_full_name: str) -> str:
    """
    Generates a task key from a node name and making sure it can be used as a task key.

    Args:
        dbt_node_full_name (str): The name of the node.

    Returns:
        str: The generated task key.
    """
    return dbt_node_full_name.replace('.', '_')
