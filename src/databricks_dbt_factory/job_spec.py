import os
import tempfile

import yaml


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
        KeyError: If no jobs are found in the provided YAML file.

    The target is written atomically (serialize fully, write to a temp file in the same
    directory, then `os.replace`), so a serialization error or interruption never leaves a
    truncated spec — important because the CLI supports updating a file in place
    (`input_job_spec_path == target_job_spec_path`).
    """
    rendered = render_job_spec(input_job_spec_path, new_tasks, new_job_name)
    write_job_spec(rendered, target_job_spec_path)


def render_job_spec(
    input_job_spec_path: str,
    new_tasks: list[dict],
    new_job_name: str | None = None,
) -> str:
    """Renders the updated job definition as YAML, without writing anything.

    Separated from `write_job_spec` so a caller with more than one artifact to produce can do all the
    fallible work first: the CLI also copies a runner notebook, and reading or serializing an invalid
    input spec after that copy left the notebook behind for a run that produced no spec.

    Args:
        input_job_spec_path (str): Path to the job definition YAML file.
        new_tasks (dict): New tasks to replace the existing tasks in the job definition file.
        new_job_name (str, optional): The name of the job to update. Defaults to None.

    Raises:
        ValueError: If the file is not valid YAML, contains no jobs, or holds a non-mapping at any level
            of `resources.jobs.<job>`. A `ValueError` rather than a `KeyError` so `main` can report it as
            a user-fixable problem without also catching the bare `KeyError`s that an unexpected manifest
            shape raises from the factory — those are bugs, and swallowing them turns a diagnosable
            traceback into `error: 'resource_type'`.
    """
    with open(input_job_spec_path, 'r', encoding="utf-8") as file:
        try:
            job_definition = yaml.safe_load(file)
        except yaml.YAMLError as error:
            raise ValueError(f"Could not parse {input_job_spec_path} as YAML: {error}") from error

    # *Every* level this function dereferences is checked here, before any of it is used — validating one
    # level at a time just leaves the next one exposed. The code below calls `.get`/`.pop`, indexes by key
    # and assigns into the job, so a non-mapping anywhere raises `AttributeError`/`TypeError`, which escapes
    # `main`'s `except (ValueError, FileNotFoundError)` and prints a traceback for a malformed *input file*
    # — the outcome this guard exists to prevent.
    resources = job_definition.get('resources') if isinstance(job_definition, dict) else None
    jobs = resources.get('jobs') if isinstance(resources, dict) else None
    if not isinstance(jobs, dict) or not jobs:
        raise ValueError(f"No jobs found in {input_job_spec_path}.")

    # replaces the first job only!
    first_job_key = next(iter(jobs))
    if not isinstance(jobs[first_job_key], dict):
        raise ValueError(f"Job {first_job_key!r} in {input_job_spec_path} is not a mapping, so it has no tasks.")

    if new_job_name:
        jobs[new_job_name] = jobs.pop(first_job_key)
        first_job_key = new_job_name

    first_job = jobs[first_job_key]
    if new_job_name:
        first_job['name'] = new_job_name
    first_job['tasks'] = new_tasks  # Replace tasks field

    return yaml.dump(job_definition, sort_keys=False, width=1000)


def write_job_spec(rendered: str, target_job_spec_path: str) -> None:
    """Writes an already-rendered job spec atomically.

    Writes to a temp file in the target's directory and `os.replace`s it into position, so an
    interruption never leaves a truncated spec — important because the CLI supports updating a file in
    place (`input_job_spec_path == target_job_spec_path`).
    """
    target_dir = os.path.dirname(os.path.abspath(target_job_spec_path))
    with tempfile.NamedTemporaryFile(
        'w', encoding="utf-8", dir=target_dir, prefix='.job_spec_', suffix='.tmp', delete=False
    ) as tmp:
        tmp.write(rendered)
        tmp_path = tmp.name
    os.replace(tmp_path, target_job_spec_path)
