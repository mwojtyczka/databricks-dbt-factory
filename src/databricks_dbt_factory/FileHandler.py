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
            json.JSONDecodeError: If the file is not valid JSON.
        """
        try:
            with open(path, 'r', encoding="utf-8") as file:
                return json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Manifest file not found: {path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing JSON from manifest file: {path}. Details: {e}")

    @staticmethod
    def write_job_definition(definition: dict, path: str):
        """
        Writes a job definition to a YAML file.

        Args:
            definition (dict): The job definition to write.
            path (str): Path to the output YAML file.

        Raises:
            IOError: If there is an issue writing to the file.
        """
        try:
            with open(path, "w", encoding="utf-8") as file:
                yaml.dump(definition, file, sort_keys=False, width=1000, allow_unicode=True)
        except IOError as e:
            raise IOError(f"Error writing job definition to file: {path}. Details: {e}")
