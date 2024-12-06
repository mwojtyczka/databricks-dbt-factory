from dataclasses import dataclass


@dataclass(frozen=True)
class TaskOptions:
    warehouse_id: str = ""
    source: str = 'GIT'
    project_directory: str = ""
    catalog: str = "main"
    schema: str = "default"
    environment_key: str = "Default"


@dataclass(frozen=True)
class Task:
    """Represents a task in the Databricks job definition."""

    task_key: str
    dbt_commands: list[str]
    options: TaskOptions
    depends_on: list[str] | None = None

    def to_dict(self) -> dict:
        """Converts the Task to a dictionary suitable for the job definition."""
        return {
            'task_key': self.task_key,
            'dbt_task': {
                'commands': self.dbt_commands,
                'project_directory': self.options.project_directory,
                'catalog': self.options.catalog,
                'schema': self.options.schema,
                'warehouse_id': self.options.warehouse_id,
                'source': self.options.source,
            },
            'environment_key': self.options.environment_key,
            'depends_on': [{'task_key': dep} for dep in (self.depends_on or [])],
        }
