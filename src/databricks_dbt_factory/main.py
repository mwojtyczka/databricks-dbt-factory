import argparse
from databricks_dbt_factory.DbtFactory import DbtFactory
from databricks_dbt_factory.FileHandler import FileHandler
from databricks_dbt_factory.DbtTask import DbtTaskOptions
from databricks_dbt_factory.TaskFactory import (
    ModelTaskFactory,
    SnapshotTaskFactory,
    SeedTaskFactory,
    TestTaskFactory,
    DbtDependencyResolver,
)


def main():
    args = parse_args()

    file_handler = FileHandler()
    resolver = DbtDependencyResolver()
    dbt_options = f"--{args.target} {args.extra_dbt_command_options}"
    task_options = DbtTaskOptions(
        environment_key=args.environment_key,
        warehouse_id=args.warehouse_id,
        catalog=args.catalog,
        schema=args.schema,
        profiles_directory=args.profiles_dir,
        project_directory=args.project_dir,
        source=args.source,
    )
    task_factories = {
        'model': ModelTaskFactory(resolver, task_options, dbt_options),
        'snapshot': SnapshotTaskFactory(resolver, task_options, dbt_options),
        'seed': SeedTaskFactory(resolver, task_options, dbt_options),
    }

    if args.run_tests:
        task_factories['test'] = TestTaskFactory(resolver, task_options, dbt_options)

    factory = DbtFactory(file_handler, task_factories)
    factory.create_tasks_and_update_job_spec(
        args.dbt_manifest_path, args.input_job_spec_path, args.target_job_spec_path
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Generate Databricks job definition from dbt manifest.")
    parser.add_argument("--dbt-manifest-path", type=str, help="Path to the manifest file", required=True)
    parser.add_argument("--input-job-spec-path", type=str, help="Path to the input job spec file", required=True)
    parser.add_argument(
        "--target-job-spec-path",
        type=str,
        help="Path to the target job spec file. " "If not provided the input spec will be changed in place.",
        required=False,
    )
    parser.add_argument("--target", type=str, help="dbt target to use", required=True)
    parser.add_argument(
        "--source",
        type=str,
        help="Optional project source. If not provided WORKSPACE will be used.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--warehouse_id", type=str, help="Optional SQL Warehouse to run dbt models on", required=False, default=None
    )
    parser.add_argument("--schema", type=str, help="Optional schema to write to.", required=False, default=None)
    parser.add_argument("--catalog", type=str, help="Optional catalog to write to.", required=False, default=None)
    parser.add_argument(
        "--profiles-dir",
        type=str,
        help="Optional (relative) path to the profiles directory.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--project-dir",
        type=str,
        help="Optional (relative) path to the project directory.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--environment-key",
        type=str,
        help="Optional (relative) key of an environment.",
        required=False,
        default="Default",
    )
    parser.add_argument(
        "--extra-dbt-command-options",
        type=str,
        help="Optional additional dbt command options",
        required=False,
        default="",
    )
    parser.add_argument(
        "--run-tests",
        type=bool,
        help="Whether to run data tests after the model. Enabled by default.",
        required=False,
        default=True,
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
