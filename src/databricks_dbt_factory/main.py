import argparse
from databricks_dbt_factory.DatabricksDbtFactory import DatabricksDbtFactory
from databricks_dbt_factory.FileHandler import FileHandler
from databricks_dbt_factory.Task import TaskOptions
from databricks_dbt_factory.TaskFactory import ModelTaskFactory, SnapshotTaskFactory, SeedTaskFactory, TestTaskFactory


def main():
    parser = argparse.ArgumentParser(description="Generate Databricks job definition from dbt manifest.")
    parser.add_argument("--manifest-path", type=str, help="Path to the manifest file", required=True)
    parser.add_argument("--output-path", type=str, help="Path to the manifest file", required=True)
    parser.add_argument("--job-name", type=str, help="Path to the manifest file", required=True)
    parser.add_argument("--target", type=str, help="dbt target to use", required=False, default="dev")
    parser.add_argument("--warehouse_id", type=str, help="SQL Warehouse to run dbt models on", required=True)
    parser.add_argument("--profiles-dir", type=str, help="dbt profile location", required=False, default=".")
    parser.add_argument("--extra-dbt-options", type=str, help="additional dbt options", required=False, default="")
    parser.add_argument("--run-tests", type=str, help="Run data tests", required=False, default=True)
    args = parser.parse_args()

    file_handler = FileHandler()
    dbt_options = f"--{args.target} --{args.profile_dir} {args.extra_dbt_options}"
    task_options = TaskOptions(warehouse_id=args.warehouse_id)
    task_factories = {
        'model': ModelTaskFactory(task_options, dbt_options),
        'snapshot': SnapshotTaskFactory(task_options, dbt_options),
        'seed': SeedTaskFactory(task_options, dbt_options),
    }
    if args.run_tests:
        task_factories['test'] = TestTaskFactory(task_options, dbt_options)
    factory = DatabricksDbtFactory(file_handler, task_factories)
    factory.create_job_definition_and_save(args.manifest_path, args.output_path, args.job_name)


if __name__ == "__main__":
    main()
