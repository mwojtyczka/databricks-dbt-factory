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
    with open(input_job_spec_path, 'r', encoding="utf-8") as file:
        job_definition = yaml.safe_load(file)

    jobs = (job_definition.get('resources') or {}).get('jobs')

    if not jobs:
        raise KeyError("No jobs found in the provided YAML file.")

    # replaces the first job only!
    first_job_key = next(iter(jobs))
    if new_job_name:
        jobs[new_job_name] = jobs.pop(first_job_key)
        first_job_key = new_job_name

    first_job = jobs[first_job_key]
    if new_job_name:
        first_job['name'] = new_job_name
    first_job['tasks'] = new_tasks  # Replace tasks field

    # Serialize before touching the target so a dump failure leaves any existing file intact,
    # then swap the fully-written temp file into place atomically.
    rendered = yaml.dump(job_definition, sort_keys=False, width=1000)
    target_dir = os.path.dirname(os.path.abspath(target_job_spec_path))
    with tempfile.NamedTemporaryFile(
        'w', encoding="utf-8", dir=target_dir, prefix='.job_spec_', suffix='.tmp', delete=False
    ) as tmp:
        tmp.write(rendered)
        tmp_path = tmp.name
    os.replace(tmp_path, target_job_spec_path)
