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
        environment_key=args.environment_key if args.environment_key is not None else "Default",
        warehouse_id=args.warehouse_id,
        catalog=args.catalog,
        schema=args.schema,
        profiles_directory=args.profiles_directory,
        project_directory=args.project_directory,
        source=args.source,
        dbt_deps_enabled=args.enable_dbt_deps,
        dbt_tasks_deps=dbt_tasks_deps,
        task_type=args.task_type,
        notebook_path=args.notebook_path,
        job_cluster_key=args.job_cluster_key,
    )
    task_factories = {
        'model': ModelTaskFactory(resolver, task_options, dbt_options),
        'snapshot': SnapshotTaskFactory(resolver, task_options, dbt_options),
        'seed': SeedTaskFactory(resolver, task_options, dbt_options),
    }

    if args.run_tests:
        task_factories['test'] = TestTaskFactory(resolver, task_options, dbt_options)

    factory = DbtFactory(file_handler, task_factories, bundle_tests=args.bundle_tests)
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
        help="Optional (relative) key of an environment. Defaults to 'Default' when unset.",
        required=False,
        default=None,
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
        action=argparse.BooleanOptionalAction,
        help="Run data tests after each model. Enabled by default; use --no-run-tests to disable.",
        required=False,
        default=True,
    )
    parser.add_argument(
        "--bundle-tests",
        action=argparse.BooleanOptionalAction,
        help=(
            "Bundle single-model tests for a given resource into one "
            "`dbt test --select <pkg>.<resource> --indirect-selection cautious` task (default: "
            "disabled — one task per test node). Cross-model tests (e.g. `relationships`) are "
            "detected from the manifest and emitted as their own tasks gated on every referenced "
            "resource, so no tests are silently dropped. Trade-off: fewer tasks and a smaller "
            "DAG, but per-test failures show up as a single red `<resource>_tests` task — drill "
            "into the logs to see which assertion failed."
        ),
        required=False,
        default=False,
    )
    parser.add_argument(
        "--enable-dbt-deps",
        action=argparse.BooleanOptionalAction,
        help="Run `dbt deps` before each task. Disabled by default; use --enable-dbt-deps to enable.",
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
        "--task-type",
        type=str,
        help="Task type to generate: 'dbt' for native dbt_task (default), 'notebook' for notebook_task wrapper.",
        required=False,
        default="dbt",
        choices=["dbt", "notebook"],
    )
    parser.add_argument(
        "--notebook-path",
        type=str,
        help="Path to the dbt runner notebook. Required when --task-type is 'notebook'.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--job-cluster-key",
        type=str,
        help="Job cluster key for running tasks on job compute instead of serverless. Mutually exclusive with --environment-key.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        help="Print generated tasks without updating the job spec file. Disabled by default.",
        required=False,
        default=False,
    )
    args = parser.parse_args()

    if args.task_type == "notebook" and not args.notebook_path:
        parser.error("--notebook-path is required when --task-type is 'notebook'")

    if args.job_cluster_key and args.environment_key is not None:
        parser.error("--job-cluster-key and --environment-key are mutually exclusive")

    if args.task_type == "notebook":
        conflicting = []
        for flag, value in (
            ("--warehouse_id", args.warehouse_id),
            ("--schema", args.schema),
            ("--catalog", args.catalog),
        ):
            if value:
                conflicting.append(flag)
        if conflicting:
            parser.error(
                f"{', '.join(conflicting)} cannot be used with --task-type notebook; "
                "notebook tasks connect via profiles.yml."
            )

    return args


if __name__ == "__main__":
    main()
