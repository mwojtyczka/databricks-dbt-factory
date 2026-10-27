import copy
import stat
from dataclasses import dataclass
from pathlib import Path

import yaml

from databricks_dbt_factory.file_io import atomic_write_bytes
from databricks_dbt_factory.job_partition import JobPartition

_ORCHESTRATOR_ONLY = frozenset(
    {
        "schedule",
        "trigger",
        "continuous",
        "email_notifications",
        "webhook_notifications",
        "notification_settings",
        "health",
    }
)
_PARTITION_JOB_ONLY = frozenset({"environments", "job_clusters"})


@dataclass(frozen=True)
class JobSpecArtifact:
    """A fully prepared job spec ready for atomic publication."""

    content: bytes
    destination: Path
    mode: int


def replace_tasks_in_job_spec(
    input_job_spec_path: str,
    new_tasks: list[dict],
    target_job_spec_path: str,
    new_job_name: str | None = None,
) -> None:
    """Replace the tasks field in a Databricks job definition YAML file. The first job is only updated.

    Args:
        input_job_spec_path (str): Path to the job definition YAML file.
        new_tasks (dict): New tasks to replace the existing tasks in the job definition file.
        target_job_spec_path (str): Path to save the updated job definition file.
        new_job_name (str, optional): The name of the job to update. Defaults to None.

    Raises:
        ValueError: If the input has no job or the target is not a regular non-symlink file.
    """
    rendered = render_job_spec(input_job_spec_path, new_tasks, new_job_name)
    destination = resolve_job_spec_destination(target_job_spec_path)
    artifact = prepare_job_spec(rendered, input_job_spec_path, destination)
    write_job_spec(artifact)


def render_job_spec(
    input_job_spec_path: str,
    new_tasks: list[dict],
    new_job_name: str | None = None,
) -> str:
    """Renders the updated job definition as YAML, without writing anything.

    The CLI prepares both the job spec and content-addressed runner before publishing either artifact.

    Args:
        input_job_spec_path (str): Path to the job definition YAML file.
        new_tasks (dict): New tasks to replace the existing tasks in the job definition file.
        new_job_name (str, optional): The name of the job to update. Defaults to None.

    Raises:
        ValueError: If the file is not valid YAML, contains no jobs, holds a non-mapping at any level
            of `resources.jobs.<job>`, or `new_job_name` is the key of a different existing job (renaming
            the first job onto it would silently drop that job). A `ValueError` rather than a `KeyError`
            so `main` can report it as a user-fixable problem without also catching the bare `KeyError`s
            that an unexpected manifest shape raises from the factory — those are bugs, and swallowing
            them turns a diagnosable traceback into `error: 'resource_type'`.
    """
    job_definition, jobs, job_key = _load_template(input_job_spec_path, new_job_name)
    jobs[job_key]["tasks"] = new_tasks  # Replace tasks field
    if _has_reuse(new_tasks):
        _reuse_settings(jobs[job_key])
    return _dump(job_definition)


def render_partitioned_job_spec(
    input_job_spec_path: str,
    partitions: list[JobPartition],
    new_job_name: str | None = None,
) -> str:
    """Renders the first job as an orchestrator running one job per partition, without writing anything.

    The orchestrator keeps the template's triggers and notifications, the partition jobs get its compute, and
    both get everything else.

    Raises:
        ValueError: as `render_job_spec` does, or if a partition job's key is already taken.
    """
    job_definition, jobs, job_key = _load_template(input_job_spec_path, new_job_name)
    template = jobs[job_key]
    reuse = any(_has_reuse(partition.tasks) for partition in partitions)
    if reuse:
        _reuse_settings(template)
    keys = {partition.name: f"{job_key}_{partition.name}" for partition in partitions}
    if taken := sorted(set(keys.values()) & set(jobs)):
        raise ValueError(
            f"Cannot split job {job_key!r} in {input_job_spec_path}: it already defines {', '.join(taken)}."
        )

    jobs[job_key] = {key: value for key, value in template.items() if key not in _PARTITION_JOB_ONLY} | {
        "tasks": [_run_job_task(partition, keys[partition.name], reuse) for partition in partitions]
    }
    shared = {key: value for key, value in template.items() if key not in _ORCHESTRATOR_ONLY}
    for partition in partitions:
        name = f"{template.get('name', job_key)}_{partition.name}"
        jobs[keys[partition.name]] = copy.deepcopy(shared) | {"name": name, "tasks": partition.tasks}
    return _dump(job_definition)


def _run_job_task(partition: JobPartition, job_key: str, reuse: bool = False) -> dict:
    return {
        "task_key": partition.name,
        "depends_on": [{"task_key": dependency} for dependency in partition.depends_on],
        "run_job_task": {"job_id": f"${{resources.jobs.{job_key}.id}}"}
        | ({"job_parameters": {"factory_force_rebuild": "{{job.parameters.factory_force_rebuild}}"}} if reuse else {}),
    }


def _has_reuse(tasks: list[dict]) -> bool:
    return any("factory_reuse_models" in task.get("notebook_task", {}).get("base_parameters", {}) for task in tasks)


def _reuse_settings(job: dict) -> None:
    """Reject pushdown overrides of Factory task parameters and overlapping owning-job runs."""
    if job.get("max_concurrent_runs", 1) != 1:
        raise ValueError("reuse requires max_concurrent_runs: 1; remove the conflicting concurrency setting")
    parameters = job.get("parameters", [])
    if not isinstance(parameters, list) or any(not isinstance(parameter, dict) for parameter in parameters):
        raise ValueError("reuse requires a list of job parameters")
    for parameter in parameters:
        name = parameter.get("name", "")
        if name.startswith("factory_") or name in {"dbt_commands", "project_directory", "profiles_directory"}:
            raise ValueError(f"reuse reserves job parameter {name!r}; remove its conflicting template definition")
    job["max_concurrent_runs"] = 1
    job["parameters"] = [*parameters, {"name": "factory_force_rebuild", "default": "false"}]


def _load_template(input_job_spec_path: str, new_job_name: str | None) -> tuple[dict, dict, str]:
    """Loads and validates the template, renaming its first job; returns the definition, its jobs and that key."""
    with open(input_job_spec_path, "r", encoding="utf-8") as file:
        try:
            job_definition = yaml.safe_load(file)
        except yaml.YAMLError as error:
            raise ValueError(f"Could not parse {input_job_spec_path} as YAML: {error}") from error

    # *Every* level this function dereferences is checked here, before any of it is used — validating one
    # level at a time just leaves the next one exposed. The code below calls `.get`/`.pop`, indexes by key
    # and assigns into the job, so a non-mapping anywhere raises `AttributeError`/`TypeError`, which escapes
    # `main`'s `except (ValueError, FileNotFoundError)` and prints a traceback for a malformed *input file*
    # — the outcome this guard exists to prevent.
    resources = job_definition.get("resources") if isinstance(job_definition, dict) else None
    jobs = resources.get("jobs") if isinstance(resources, dict) else None
    if not isinstance(jobs, dict) or not jobs:
        raise ValueError(f"No jobs found in {input_job_spec_path}.")

    # replaces the first job only!
    first_job_key = next(iter(jobs))
    if not isinstance(jobs[first_job_key], dict):
        raise ValueError(f"Job {first_job_key!r} in {input_job_spec_path} is not a mapping, so it has no tasks.")

    if new_job_name and new_job_name != first_job_key:
        if new_job_name in jobs:
            raise ValueError(
                f"Cannot rename job {first_job_key!r} to {new_job_name!r} in {input_job_spec_path}: "
                f"a different job already uses that key."
            )
        jobs[new_job_name] = jobs.pop(first_job_key)
        first_job_key = new_job_name

    if new_job_name:
        jobs[first_job_key]["name"] = new_job_name
    return job_definition, jobs, first_job_key


def _dump(job_definition: dict) -> str:
    return yaml.dump(job_definition, sort_keys=False, width=1000)


def resolve_job_spec_destination(target_job_spec_path: str | Path) -> Path:
    """Validates a requested job spec target and returns its canonical destination."""
    requested_destination = Path(target_job_spec_path)
    if requested_destination.is_symlink() or (requested_destination.exists() and not requested_destination.is_file()):
        raise ValueError(f"Job spec target {requested_destination} must be a regular non-symlink file.")
    return requested_destination.resolve()


def prepare_job_spec(rendered: str, input_job_spec_path: str, destination: Path) -> JobSpecArtifact:
    """Encodes a rendered spec and resolves the mode its atomic replacement must use."""
    mode_source = destination if destination.exists() else Path(input_job_spec_path)
    mode = stat.S_IMODE(mode_source.stat().st_mode)
    return JobSpecArtifact(rendered.encode("utf-8"), destination, mode)


def write_job_spec(artifact: JobSpecArtifact) -> None:
    """Publishes a prepared job spec atomically."""
    atomic_write_bytes(artifact.destination, artifact.content, artifact.mode)
