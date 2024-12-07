import argparse
from databricks_dbt_factory.DatabricksDbtFactory import DatabricksDbtFactory
from databricks_dbt_factory.FileHandler import FileHandler
from databricks_dbt_factory.Task import TaskOptions
from databricks_dbt_factory.TaskFactory import (
    ModelTaskFactory,
    SnapshotTaskFactory,
    SeedTaskFactory,
    TestTaskFactory,
    DbtDependencyResolver,
)


def main():
    parser = argparse.ArgumentParser(description="Generate Databricks job definition from dbt manifest.")
    parser.add_argument("--dbt-manifest-path", type=str, help="Path to the manifest file", required=True)
    parser.add_argument(
        "--job-definition-path", type=str, help="Path to the destination job definition file", required=True
    )
    parser.add_argument("--target", type=str, help="dbt target to use", required=False, default="dev")
    parser.add_argument(
        "--warehouse_id", type=str, help="SQL Warehouse to run dbt models on", required=False, default=None
    )
    parser.add_argument("--profiles-dir", type=str, help="dbt profile location", required=False, default=".")
    parser.add_argument("--extra-dbt-options", type=str, help="additional dbt options", required=False, default="")
    parser.add_argument(
        "--run-tests", type=bool, help="Whether to run data tests after the model", required=False, default=True
    )
    args = parser.parse_args()

    file_handler = FileHandler()
    resolver = DbtDependencyResolver()
    dbt_options = f"--{args.target} --{args.profile_dir} {args.extra_dbt_options}"
    task_options = TaskOptions(warehouse_id=args.warehouse_id)
    task_factories = {
        'model': ModelTaskFactory(resolver, task_options, dbt_options),
        'snapshot': SnapshotTaskFactory(resolver, task_options, dbt_options),
        'seed': SeedTaskFactory(resolver, task_options, dbt_options),
    }
    if args.run_tests:
        task_factories['test'] = TestTaskFactory(resolver, task_options, dbt_options)
    factory = DatabricksDbtFactory(file_handler, task_factories)
    factory.create_tasks_and_update_job_spec(args.dbt_manifest_path, args.job_definition_path)


if __name__ == "__main__":
    main()
