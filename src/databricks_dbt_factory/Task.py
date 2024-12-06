from dataclasses import dataclass


@dataclass(frozen=True)
class Task:
    """Represents a task in the Databricks job definition."""
    task_key: str
    dbt_commands: list[str]
    depends_on: list[str] | None = None

    def to_dict(self) -> dict:
        """Converts the Task to a dictionary suitable for the job definition."""
        return {
            'task_key': self.task_key,
            'dbt_task': {
                'commands': self.dbt_commands,
                'project_directory': "",
                'catalog': 'main',
                'schema': 'default',
                'warehouse_id': '475b94ddc7cd5211',
            },
            'environment_key': 'Default',
            'depends_on': [{'task_key': dep} for dep in (self.depends_on or [])]
        }
