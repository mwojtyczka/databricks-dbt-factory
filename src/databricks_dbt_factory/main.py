import argparse
from databricks_dbt_factory.DatabricksDbtFactory import DatabricksDbtFactory
from databricks_dbt_factory.FileHandler import FileHandler
from databricks_dbt_factory.TaskFactory import ModelTaskFactory, SnapshotTaskFactory, SeedTaskFactory, TestTaskFactory


def main():
    parser = argparse.ArgumentParser(description="Generate Databricks job definition from dbt manifest.")
    parser.add_argument("--manifest-path", type=str, help="Path to the manifest file", required=True)
    parser.add_argument("--output-path", type=str, help="Path to the manifest file", required=True)
    parser.add_argument("--job-name", type=str, help="Path to the manifest file", required=True)
    args = parser.parse_args()

    file_handler = FileHandler()
    task_factories = {
        'model': ModelTaskFactory(),
        'snapshot': SnapshotTaskFactory(),
        'seed': SeedTaskFactory(),
        'test': TestTaskFactory(),
    }
    factory = DatabricksDbtFactory(file_handler, task_factories)
    factory.generate_job_definition_and_save(args.manifest_path, args.output_path, args.job_name)


if __name__ == "__main__":
    main()
