import json
import yaml


class FileHandler:
    """Handles reading and writing files for dbt manifests and databricks job definitions."""

    @staticmethod
    def read_dbt_manifest(path: str) -> dict:
        """
        Reads a JSON manifest file and returns its content as a dictionary.

        Args:
            path (str): Path to the manifest file.

        Returns:
            dict: Parsed content of the manifest file.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file is not a valid manifest file.
        """
        try:
            with open(path, 'r', encoding="utf-8") as file:
                return json.load(file)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Manifest file not found: {path}. Details: {e}") from e
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing JSON from manifest file: {path}. Details: {e}") from e

    @staticmethod
    def replace_tasks_in_job_spec(
        input_job_spec_path: str, new_tasks: list[dict], target_job_spec_path: str | None = None
    ) -> None:
        """Replace the tasks field in a Databricks job definition YAML file.
,
        Args:
            input_job_spec_path (str): Path to the job definition YAML file.
            new_tasks (dict): New tasks to replace the existing tasks in the job definition file.
            target_job_spec_path (str, optional): Path to save the updated job definition file.
            If not provided, the input file will be updated in place. Defaults to None.

        Raises:
            KeyError: If no jobs are found in the provided YAML file.
        """
        with open(input_job_spec_path, 'r', encoding="utf-8") as file:
            job_definition = yaml.safe_load(file)

        jobs = job_definition.get('resources', {}).get('jobs', {})

        if jobs is None:
            raise KeyError("No jobs found in the provided YAML file.")

        jobs[next(iter(jobs))]['tasks'] = new_tasks  # Replace tasks field

        target_path = target_job_spec_path or input_job_spec_path
        with open(target_path, 'w', encoding="utf-8") as file:
            yaml.dump(job_definition, file, sort_keys=False, width=1000)
