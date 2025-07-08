import argparse
from databricks_dbt_factory.DbtFactory import DbtFactory
from databricks_dbt_factory.SpecsHandler import SpecsHandler
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

    file_handler = SpecsHandler()
    resolver = DbtDependencyResolver()

    dbt_options = build_dbt_options(args)
    dbt_tasks_deps = (
        [item.strip() for item in args.dbt_tasks_deps.split(",") if item.strip()] if args.dbt_tasks_deps else []
    )

    task_options = DbtTaskOptions(
        environment_key=args.environment_key,
        warehouse_id=args.warehouse_id,
        catalog=args.catalog,
        schema=args.schema,
        profiles_directory=args.profiles_directory,
        project_directory=args.project_directory,
        source=args.source,
        dbt_deps_enabled=args.enable_dbt_deps,
        dbt_tasks_deps=dbt_tasks_deps,
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
        args.dbt_manifest_path, args.input_job_spec_path, args.target_job_spec_path, args.new_job_name, args.dry_run
    )


def build_dbt_options(args):
    """Builds the dbt command options based on the provided arguments."""
    dbt_options = ""

    if args.target:
        dbt_options += f"--target {args.target}"

    if args.extra_dbt_command_options:
        dbt_options += f" {args.extra_dbt_command_options}"

    return dbt_options


def parse_args():
    parser = argparse.ArgumentParser(description="Generate Databricks job definition from dbt manifest.")
    parser.add_argument(
        "--new-job-name",
        type=str,
        help="Optional job name. If provided the existing job name in job spec is updated",
        required=False,
        default=None,
    )
    parser.add_argument("--dbt-manifest-path", type=str, help="Path to the manifest file", required=True)
    parser.add_argument("--input-job-spec-path", type=str, help="Path to the input job spec file", required=True)
    parser.add_argument(
        "--target-job-spec-path",
        type=str,
        help="Path to the target job spec file.",
        required=True,
    )
    parser.add_argument("--target", type=str, help="Optional dbt target to use.", required=False)
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
        "--profiles-directory",
        type=str,
        help="Optional (relative) path to the profiles directory.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--project-directory",
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
    parser.add_argument(
        "--enable-dbt-deps",
        type=bool,
        help="Optional flag to enable dbt deps to be run before each task.",
        required=False,
        default=False,
    )
    parser.add_argument(
        "--dbt-tasks-deps",
        type=str,
        help="Optional list of tasks that require dbt deps. Only in effect if `--enable-dbt-deps` is enabled.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--dry-run",
        type=bool,
        help="Print generated tasks without updating the job spec file. Disabled by default.",
        required=False,
        default=False,
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
