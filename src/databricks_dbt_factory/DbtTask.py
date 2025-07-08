from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DbtTaskOptions:
    environment_key: str = "Default"  # serverless env
    """The key of an environment. It has to be unique within a job."""

    catalog: str | None = None
    """Optional name of the catalog to use. The value is the top level in the 3-level namespace of
    Unity Catalog (catalog / schema / relation). The catalog value can only be specified if a
    warehouse_id is specified. Requires dbt-databricks >= 1.1.1."""

    profiles_directory: str | None = None
    """Optional (relative) path to the profiles directory. Can only be specified if no warehouse_id is
    specified. If no warehouse_id is specified and this folder is unset, the root directory is used."""

    project_directory: str | None = None
    """Path to the project directory. Optional for Git sourced tasks, in which case if no value is
    provided, the root of the Git repository is used."""

    schema: str | None = None
    """Optional schema to write to. This parameter is only used when a warehouse_id is also provided.
    If not provided, the `default` schema is used."""

    source: str | None = None
    """Optional location type of the project directory. When set to `WORKSPACE`, the project will be
    retrieved from the local Databricks workspace. When set to `GIT`, the project will be retrieved
    from a Git repository defined in `git_source`. If the value is empty, the task will use `GIT` if
    `git_source` is defined and `WORKSPACE` otherwise.
    
    * `WORKSPACE`: Project is located in Databricks workspace. * `GIT`: Project is located in cloud
    Git provider."""

    warehouse_id: str | None = None
    """ID of the SQL warehouse to connect to. If provided, we automatically generate and provide the
    profile and connection details to dbt. It can be overridden on a per-command basis by using the
    `--profiles-dir` command line argument."""

    dbt_deps_enabled: bool = False
    """Optional flag to enable dbt deps to be run before each task. Defaults to False."""

    dbt_tasks_deps: list[str] = field(default_factory=list)
    """Optional comma separated list of tasks that requires dbt debs. Only in effect if dbt_deps_enabled is enabled."""


@dataclass(frozen=True)
class DbtTask:
    """Represents a dbt task in the Databricks job definition."""

    task_key: str
    commands: list[str]
    options: DbtTaskOptions
    depends_on: list[str] | None = None

    def to_dict(self) -> dict:
        """Converts the Task to a dictionary suitable for the job definition."""
        spec: dict[str, Any] = {
            'task_key': self.task_key,
            'dbt_task': {
                'commands': self.commands,
            },
            'environment_key': self.options.environment_key,
            'depends_on': [{'task_key': dep} for dep in (self.depends_on or [])],
        }

        if self.options.source:
            spec['dbt_task']['source'] = self.options.source

        if self.options.project_directory:
            spec['dbt_task']['project_directory'] = self.options.project_directory

        if self.options.schema:
            spec['dbt_task']['schema'] = self.options.schema

        if self.options.warehouse_id:  # not required if using "None (Manual) / Serverless"
            spec['dbt_task']['warehouse_id'] = self.options.warehouse_id

        if self.options.catalog:  # catalog can only be specified if warehouse_id is specified
            spec['dbt_task']['catalog'] = self.options.catalog

        if self.options.profiles_directory:  # only if no warehouse_id is specified
            spec['dbt_task']['profiles_directory'] = self.options.profiles_directory

        return spec
