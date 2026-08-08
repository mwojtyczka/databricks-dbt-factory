import argparse
import hashlib
import os
import shlex
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

from databricks_dbt_factory.__about__ import __version__
from databricks_dbt_factory.DbtFactory import DbtFactory
from databricks_dbt_factory.file_io import atomic_write_bytes
from databricks_dbt_factory.job_spec import (
    JobSpecArtifact,
    prepare_job_spec,
    render_job_spec,
    resolve_job_spec_destination,
    write_job_spec,
)
from databricks_dbt_factory.Utils import read_dbt_manifest
from databricks_dbt_factory.DbtTask import DbtTaskOptions
from databricks_dbt_factory.TaskFactory import (
    ModelTaskFactory,
    SnapshotTaskFactory,
    SeedTaskFactory,
    TestTaskFactory,
    DbtDependencyResolver,
    validate_dbt_options,
    validate_extra_dbt_options,
)

_RUNNER_NOTEBOOK_FILENAME = "run_dbt_command.py"


@dataclass(frozen=True)
class _RunnerArtifact:
    """A content-addressed runner notebook ready for immutable publication."""

    content: bytes
    destination: Path
    notebook_path: str
    at_project_root: bool


@dataclass(frozen=True)
class _OutputPlan:
    """Resolved output settings shared by task generation and publication."""

    job_spec_destination: Path | None
    runner_artifact: _RunnerArtifact | None
    notebook_path: str | None
    project_directory: str | None
    source: str | None


def _prepare_runner_notebook(target_job_spec_destination: Path, project_directory: str | None) -> _RunnerArtifact:
    """Reads the packaged runner and resolves its content-addressed bundle path without writing."""
    source = resources.files("databricks_dbt_factory") / "notebook" / _RUNNER_NOTEBOOK_FILENAME
    content = source.read_bytes()
    digest = hashlib.sha256(content).hexdigest()

    spec_dir = target_job_spec_destination.parent
    if project_directory and not Path(project_directory).is_absolute():
        at_project_root = True
        destination_dir = (spec_dir / project_directory).resolve()
    else:
        at_project_root = False
        destination_dir = spec_dir

    destination = destination_dir / f"run_dbt_command_{digest}.py"
    relative = Path(os.path.relpath(destination, start=spec_dir)).as_posix()
    notebook_path = relative if relative.startswith("..") else f"./{relative}"
    return _RunnerArtifact(content, destination, notebook_path, at_project_root)


def _publish_runner_notebook(artifact: _RunnerArtifact) -> None:
    """Publishes a new immutable runner, or reuses an identical regular file."""
    _validate_runner_notebook(artifact)
    if artifact.destination.exists():
        return
    atomic_write_bytes(artifact.destination, artifact.content, 0o644)


def _validate_runner_notebook(artifact: _RunnerArtifact) -> None:
    """Rejects any existing runner target that cannot be safely reused as immutable content."""
    destination = artifact.destination
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise ValueError(f'Runner target {destination} must be a regular non-symlink file.')
    if destination.exists() and destination.read_bytes() != artifact.content:
        raise ValueError(f'Runner target {destination} does not match its SHA-256 filename.')


def _prepare_output_plan(args: argparse.Namespace) -> _OutputPlan:
    """Resolves CLI output locations and effective notebook task settings."""
    notebook_path = args.notebook_path
    auto_copy_runner = args.task_type == "notebook" and notebook_path is None
    if auto_copy_runner and args.source == "GIT":
        raise SystemExit(
            "error: --source GIT requires a caller-managed --notebook-path; "
            "auto-copied bundle runners use --source WORKSPACE."
        )

    job_spec_destination = None
    if not args.dry_run:
        try:
            job_spec_destination = resolve_job_spec_destination(args.target_job_spec_path)
        except ValueError as error:
            raise SystemExit(f"error: {error}") from error

    if not auto_copy_runner:
        return _OutputPlan(
            job_spec_destination,
            None,
            notebook_path,
            args.project_directory,
            args.source,
        )

    runner_target = job_spec_destination or Path(args.target_job_spec_path).resolve()
    runner_artifact = _prepare_runner_notebook(runner_target, args.project_directory)
    project_directory = "." if runner_artifact.at_project_root else args.project_directory
    return _OutputPlan(
        job_spec_destination,
        runner_artifact,
        runner_artifact.notebook_path,
        project_directory,
        "WORKSPACE",
    )


def _create_dbt_factory(args: argparse.Namespace, output_plan: _OutputPlan, dbt_options: str) -> DbtFactory:
    """Builds the configured resource factories for one CLI invocation."""
    resolver = DbtDependencyResolver()
    dbt_tasks_deps = (
        [item.strip() for item in args.dbt_tasks_deps.split(",") if item.strip()] if args.dbt_tasks_deps else []
    )

    task_options = DbtTaskOptions(
        environment_key=args.environment_key if args.environment_key is not None else "Default",
        warehouse_id=args.warehouse_id,
        catalog=args.catalog,
        schema=args.schema,
        profiles_directory=args.profiles_directory,
        project_directory=output_plan.project_directory,
        source=output_plan.source,
        dbt_deps_enabled=args.enable_dbt_deps,
        dbt_tasks_deps=dbt_tasks_deps,
        task_type=args.task_type,
        notebook_path=output_plan.notebook_path,
        job_cluster_key=args.job_cluster_key,
    )
    task_factories = {
        'model': ModelTaskFactory(resolver, task_options, dbt_options),
        'snapshot': SnapshotTaskFactory(resolver, task_options, dbt_options),
        'seed': SeedTaskFactory(resolver, task_options, dbt_options),
    }
    if args.run_tests:
        task_factories['test'] = TestTaskFactory(resolver, task_options, dbt_options)
    return DbtFactory(task_factories, bundle_tests=args.bundle_tests)


def _validate_artifact_destinations(
    runner_artifact: _RunnerArtifact | None, job_spec_artifact: JobSpecArtifact
) -> None:
    """Validates that prepared artifacts are safe and address distinct destinations."""
    if runner_artifact is None:
        return
    if runner_artifact.destination.resolve() == job_spec_artifact.destination.resolve():
        raise ValueError(f'runner and job spec destinations must be different: {runner_artifact.destination}')
    _validate_runner_notebook(runner_artifact)


def _prepare_generated_artifacts(
    args: argparse.Namespace,
    factory: DbtFactory,
    output_plan: _OutputPlan,
) -> tuple[list[dict], JobSpecArtifact | None]:
    """Generates tasks and prepares the job spec without publishing files."""
    manifest = read_dbt_manifest(args.dbt_manifest_path)
    tasks = factory.create_tasks(manifest)
    if args.dry_run:
        return tasks, None

    assert output_plan.job_spec_destination is not None
    rendered = render_job_spec(args.input_job_spec_path, tasks, args.new_job_name)
    job_spec_artifact = prepare_job_spec(
        rendered,
        args.input_job_spec_path,
        output_plan.job_spec_destination,
    )
    _validate_artifact_destinations(output_plan.runner_artifact, job_spec_artifact)
    return tasks, job_spec_artifact


def _publish_artifacts(runner_artifact: _RunnerArtifact | None, job_spec_artifact: JobSpecArtifact) -> None:
    """Publishes prepared artifacts after rechecking filesystem identity aliases."""
    if runner_artifact is not None:
        _publish_runner_notebook(runner_artifact)
        if job_spec_artifact.destination.exists() and runner_artifact.destination.samefile(
            job_spec_artifact.destination
        ):
            raise ValueError(f'runner and job spec destinations must be different: {runner_artifact.destination}')
    write_job_spec(job_spec_artifact)


def main():
    args = parse_args()

    try:
        dbt_options = build_dbt_options(args)
    except ValueError as error:
        raise SystemExit(f"error: {error}") from error

    output_plan = _prepare_output_plan(args)
    factory = _create_dbt_factory(args, output_plan, dbt_options)
    try:
        tasks, job_spec_artifact = _prepare_generated_artifacts(args, factory, output_plan)
    except (ValueError, FileNotFoundError) as error:
        raise SystemExit(f"error: {error}") from error

    if args.dry_run:
        print(tasks)
        return

    assert job_spec_artifact is not None
    try:
        _publish_artifacts(output_plan.runner_artifact, job_spec_artifact)
    except ValueError as error:
        raise SystemExit(f"error: {error}") from error


def build_dbt_options(args):
    """Builds the dbt command options based on the provided arguments."""
    validate_extra_dbt_options(args.extra_dbt_command_options)
    dbt_options = ""

    if args.target is not None:
        dbt_options += f"--target {shlex.quote(args.target)}"

    if args.extra_dbt_command_options:
        dbt_options += f" {args.extra_dbt_command_options}"

    validate_dbt_options(dbt_options)
    return dbt_options


def parse_args():
    parser = argparse.ArgumentParser(description="Generate Databricks job definition from dbt manifest.")
    parser.add_argument(
        "--version",
        action="version",
        version=f"databricks-dbt-factory {__version__}",
        help="Show the installed databricks-dbt-factory version and exit.",
    )
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
    parser.add_argument(
        "--target",
        type=str,
        help="Optional dbt target to use. Its parse context must match the supplied manifest.",
        required=False,
    )
    parser.add_argument(
        "--source",
        type=str,
        help=(
            "Optional project source. If omitted, Databricks infers it from the job's git_source; "
            "auto-copied runners explicitly use WORKSPACE."
        ),
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
        help="Optional (relative) runtime path to the profiles directory used for the supplied manifest.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--project-directory",
        type=str,
        help="Optional (relative) runtime path to the project represented by the supplied manifest.",
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
        help=(
            "Optional static dbt options that do not change selection or parse context. Selector options, "
            "--vars, --profile, --profiles-dir, --project-dir, --target/-t, and Databricks dynamic value "
            "references are refused. Use dedicated factory arguments where available; runtime parse context "
            "must match the supplied manifest. Allowed values that begin with a reserved short-option prefix "
            "must use the unambiguous --option=value form."
        ),
        required=False,
        default="",
    )
    parser.add_argument(
        "--no-run-tests",
        action="store_false",
        dest="run_tests",
        help="Skip generating dbt test tasks. Tests are included by default.",
    )
    parser.add_argument(
        "--bundle-tests",
        action="store_true",
        help=(
            "Bundle exact selectors for data tests with one testable parent (model, seed, snapshot, "
            "or source) and unit tests into one Databricks task per parent (default: one task per "
            "test node). Selectors are unioned by required indirect-selection mode, producing at "
            "most two `dbt test` commands. Data tests with zero or multiple testable parents remain "
            "standalone; multi-resource tests depend on every referenced model, seed, or snapshot "
            "task. Trade-off: fewer tasks and a smaller DAG, but per-test failures "
            "show up as a single red `<resource>_test` task — drill into the logs to see which "
            "assertion failed."
        ),
    )
    parser.add_argument(
        "--enable-dbt-deps",
        action="store_true",
        help="Run `dbt deps` before each task.",
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
        help="Task type to generate: 'notebook' for notebook_task wrapper (default), 'dbt' for native dbt_task.",
        required=False,
        default="notebook",
        choices=["dbt", "notebook"],
    )
    parser.add_argument(
        "--notebook-path",
        type=str,
        help=(
            "Path to the dbt runner notebook (used when --task-type is 'notebook'). If omitted, "
            "the factory publishes the packaged runner under its full content-addressed SHA-256 "
            "filename and references it relatively, so `databricks bundle deploy` uploads it. "
            "Pass an explicit path to pin the notebook elsewhere and manage it yourself."
        ),
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
        action="store_true",
        help="Print generated tasks without updating the job spec file.",
    )
    args = parser.parse_args()

    # Python 3.10 argparse represents an option value exactly equal to `--` as an empty list.
    if args.extra_dbt_command_options == []:
        args.extra_dbt_command_options = "--"

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
