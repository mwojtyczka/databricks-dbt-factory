# mypy: ignore-errors
#!/usr/bin/env python3
"""Measure model reuse on jaffle-shop: `bench.py run` or `bench.py clean`.

`run` fetches jaffle-shop at a pinned SHA, deploys a bundle with a serverless SQL warehouse, captures
reuse provenance inside serverless, generates the off and on arm jobs with the Factory CLI, and runs the
scenarios (cold, rerun, append, edit) for both arms. Results are appended to
`<catalog>.jaffle_bench_results.reuse_benchmark_runs`.
"""

from __future__ import annotations

import argparse
import io
import os
import json
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
BUILD = HERE / "build"
BUNDLE = BUILD / "bundle"
VARIANTS = BUILD / "variants"
ACCEPTANCE_RUNNER = REPO / "tests" / "databricks" / "acceptance_runner.py"

JAFFLE_URL = "https://github.com/dbt-labs/jaffle-shop.git"
# Last jaffle-shop commit whose dbt_project requires dbt >=1.12 (the HEAD targets dbt v2 and does not parse).
JAFFLE_SHA = "0f25768470271be01c2663f1c53f366b53d41823"
PROFILE = TARGET = "fevm-test-jg"
BUNDLE_NAME = "jaffle_bench"
CATALOG = "test_jg_catalog"
RAW, OFF, ON, RESULTS = "jaffle_bench_raw", "jaffle_bench_off", "jaffle_bench_on", "jaffle_bench_results"
ARTIFACTS = "artifacts"
RESULTS_TABLE = f"{CATALOG}.{RESULTS}.reuse_benchmark_runs"
ARM_TARGET = {"off": "arm_off", "on": "arm_on"}
ARM_SCHEMA = {"off": OFF, "on": ON}
# Jobs accept only a custom base environment path; `make bench-env` uploads it and registers it as the
# workspace base environment jaffle-bench-dbt.
BASE_ENVIRONMENT = "/Workspace/Users/${workspace.current_user.userName}/.jaffle_bench/dbt_base_environment.yml"
SCENARIOS = ("cold", "rerun", "append", "edit")
# Changes stg_supplies' compiled SQL (and so its view definition) without changing its rows.
CODE_EDIT = ("select * from renamed", "select * from renamed where supply_id is not null")
VOLUME_ROOT = f"/Volumes/{CATALOG}/{ON}/{ARTIFACTS}"
RESULT_COLUMNS = (
    "benchmark_run_id, recorded_at, rep, arm, scenario, variant, step_kind, job_run_id, task_key, task_state, "
    "task_seconds, run_seconds, dbt_run_invocations, dbt_test_invocations, reuse_reason, reused, "
    "warehouse_query_ms, dbus, factory_git_sha, factory_version, jaffle_shop_sha, scale_factor, warehouse_size"
)
RESULTS_DDL = f"""CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
  benchmark_run_id STRING, recorded_at TIMESTAMP, rep INT, arm STRING, scenario STRING, variant STRING,
  step_kind STRING, job_run_id BIGINT, task_key STRING, task_state STRING, task_seconds DOUBLE,
  run_seconds DOUBLE, dbt_run_invocations INT, dbt_test_invocations INT, reuse_reason STRING, reused BOOLEAN,
  warehouse_query_ms DOUBLE, dbus DOUBLE, factory_git_sha STRING, factory_version STRING,
  jaffle_shop_sha STRING, scale_factor DOUBLE, warehouse_size STRING) USING DELTA"""


@dataclass(frozen=True)
class Step:
    kind: str  # variant | reset | append | run
    arm: str = ""
    scenario: str = ""
    variant: str = ""
    rep: int = 0


def plan(reps: int, arms: tuple[str, ...] = ("off", "on"), scenarios: tuple[str, ...] = SCENARIOS) -> list[Step]:
    """Each rep runs both arms through every scenario; arm order alternates between reps.

    Every arm starts from an empty arm schema and the base project, then ends with the edit variant,
    so the base variant is redeployed at the start of the next arm. Scenarios can be narrowed for smoke runs;
    the append and edit setup steps are only emitted when their scenario is selected.
    """
    steps: list[Step] = []
    for rep in range(1, reps + 1):
        for arm in arms if rep % 2 else arms[::-1]:
            steps += [Step("variant", arm, variant="base", rep=rep), Step("reset", arm, rep=rep)]
            if "cold" in scenarios:
                steps.append(Step("run", arm, "cold", rep=rep))
            if "rerun" in scenarios:
                steps.append(Step("run", arm, "rerun", rep=rep))
            if "append" in scenarios:
                steps += [Step("append", arm, rep=rep), Step("run", arm, "append", rep=rep)]
            if "edit" in scenarios:
                steps += [Step("variant", arm, variant="edit", rep=rep), Step("run", arm, "edit", rep=rep)]
    return steps


def result_rows(
    meta: dict[str, Any],
    run: dict[str, Any],
    tasks: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
    query_ms: dict[str, float],
) -> list[dict[str, Any]]:
    """One row per job task. Metrics come from the instrumented runner; query_ms maps statement id to ms."""
    rows = []
    run_seconds = (run["end_ms"] - run["start_ms"]) / 1000 if run.get("end_ms") else None
    for task in tasks:
        metric = next((m for m in metrics if m.get("task_key") == task["task_key"]), {})
        statements = [e["query_id"] for e in metric.get("dbsql", []) if e.get("query_id")]
        known = [query_ms[s] for s in statements if s in query_ms]
        rows.append(
            {
                **meta,
                "job_run_id": run["run_id"],
                "step_kind": meta.get("step_kind", "run"),
                "task_key": task["task_key"],
                "task_state": task.get("state"),
                "task_seconds": (task["end_ms"] - task["start_ms"]) / 1000 if task.get("end_ms") else None,
                "run_seconds": run_seconds,
                "dbt_run_invocations": metric.get("invocations", {}).get("run"),
                "dbt_test_invocations": metric.get("invocations", {}).get("test"),
                "reuse_reason": metric.get("reason"),
                "reused": None if metric.get("reason") is None else metric["reason"] == "REUSED",
                "warehouse_query_ms": sum(known) if known else None,
            }
        )
    return rows


def lit(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def insert_statement(rows: list[dict[str, Any]]) -> str:
    """Builds the results INSERT; recorded_at is set by the warehouse at insert time."""
    columns = [c.strip() for c in RESULT_COLUMNS.split(",")]
    values = []
    for row in rows:
        cells = [lit(row.get(c)) if c != "recorded_at" else "current_timestamp()" for c in columns]
        values.append("(" + ", ".join(cells) + ")")
    return f"INSERT INTO {RESULTS_TABLE} ({', '.join(columns)}) VALUES " + ", ".join(values)


# ---- local build ------------------------------------------------------------------------------------------


def run_cmd(command: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True, check=False)  # noqa: S603
    if result.returncode:
        raise RuntimeError(f"{' '.join(command[:3])} failed:\n{result.stderr or result.stdout}")
    return result.stdout


def bundle_cmd(*args: str, extra: list[str] | None = None) -> str:
    return run_cmd(
        ["databricks", "bundle", *args, "--profile", PROFILE, "--target", TARGET, *(extra or [])], cwd=BUNDLE
    )


def fetch_jaffle_shop(project: Path) -> None:
    """Checks out the pinned SHA and applies the two benchmark edits: raw sources and project-wide reuse opt-in."""
    source = BUILD / "jaffle-shop"
    if not source.exists():
        run_cmd(["git", "clone", "--quiet", JAFFLE_URL, str(source)])
    run_cmd(["git", "-C", str(source), "checkout", "--quiet", "--force", JAFFLE_SHA])
    if project.exists():
        shutil.rmtree(project)
    shutil.copytree(source, project, ignore=shutil.ignore_patterns(".git", "target", "logs"))
    sources = project / "models" / "staging" / "__sources.yml"
    text = sources.read_text()
    assert text.count("schema: raw\n") == 1, "jaffle-shop sources changed; update the raw schema patch"
    sources.write_text(text.replace("schema: raw\n", f"schema: {RAW}\n"))
    config = project / "dbt_project.yml"
    text = config.read_text()
    marker = "models:\n  jaffle_shop:\n"
    assert text.count(marker) == 1, "jaffle-shop dbt_project.yml changed; update the reuse opt-in patch"
    opt_in = "    +meta:\n      dbt_factory:\n        reuse:\n          enabled: true\n"
    config.write_text(text.replace(marker, marker + opt_in))


def write_profiles(directory: Path, warehouse_id: str) -> None:
    """One statically named profile with one static target per arm; no secrets are written."""
    directory.mkdir(parents=True, exist_ok=True)
    outputs = {
        target: {
            "type": "databricks",
            "host": "fevm-test-jg.cloud.databricks.com",
            "http_path": f"/sql/1.0/warehouses/{warehouse_id}",
            "catalog": CATALOG,
            "schema": ARM_SCHEMA[arm],
            "threads": 4,
            "token": "{{ env_var('DBT_ACCESS_TOKEN') }}",
        }
        for arm, target in ARM_TARGET.items()
    }
    profile = {"default": {"target": ARM_TARGET["on"], "outputs": outputs}}
    (directory / "profiles.yml").write_text(yaml.safe_dump(profile, sort_keys=True))


def dbt_deps(project: Path) -> None:
    scratch = BUILD / "dbt-deps-profiles"
    write_profiles(scratch, "0000000000000000")
    dbt = Path(sys.executable).parent / "dbt"
    run_cmd([str(dbt), "deps", "--project-dir", str(project), "--profiles-dir", str(scratch)])


def prune_snapshot(project: Path) -> None:
    """Removes what the reuse snapshot must not contain: dbt output, logs, and profiles (credential-bearing files
    are refused). Package integration-test profiles are not used by dbt runs."""
    for name in ("target", "logs"):
        shutil.rmtree(project / name, ignore_errors=True)
    for profile in project.rglob("profiles.yml"):
        profile.unlink()


def make_variants() -> None:
    """Base: the pinned project. edit: base plus a one-line SQL change to stg_supplies (see CODE_EDIT)."""
    base = VARIANTS / "base" / "project"
    fetch_jaffle_shop(base)
    dbt_deps(base)
    prune_snapshot(base)
    edit = VARIANTS / "edit" / "project"
    if edit.exists():
        shutil.rmtree(edit)
    shutil.copytree(base, edit)
    edited = edit / "models" / "staging" / "stg_supplies.sql"
    assert CODE_EDIT[0] in edited.read_text(), "pinned stg_supplies.sql no longer contains the edited line"
    edited.write_text(edited.read_text().replace(*CODE_EDIT))


def build_partial_parse(variant: str) -> None:
    """Pre-builds target/partial_parse.msgpack with the pinned dbt in the variant's own project path.

    Only the msgpack is kept: it is inside the captured snapshot, so the project digest covers it, and the
    runtime loads it instead of parsing (the on arm verifies it against the captured manifest).
    """
    project = VARIANTS / variant / "project"
    dbt = Path(sys.executable).parent / "dbt"
    env = {**os.environ, "DBT_ACCESS_TOKEN": "parse-only"}  # parse never connects; the token only needs a value
    run_cmd([str(dbt), "parse", "--project-dir", str(project), "--profiles-dir",
             str(VARIANTS / variant / "profiles"), "--target", ARM_TARGET["on"]], env=env)  # fmt: skip
    msgpack = project / "target" / "partial_parse.msgpack"
    assert msgpack.is_file(), "dbt parse did not write partial_parse.msgpack"
    kept = BUILD / f"partial_parse_{variant}.msgpack"
    shutil.copy(msgpack, kept)
    shutil.rmtree(project / "target")
    (project / "target").mkdir()
    shutil.copy(kept, project / "target" / "partial_parse.msgpack")


def build_wheel() -> Path:
    dist = BUILD / "dist"
    run_cmd(["uv", "build", "--quiet", "--wheel", "--out-dir", str(dist), str(REPO)])
    return max(dist.glob("databricks_dbt_factory-*.whl"), key=lambda p: p.stat().st_mtime)


def stage_bundle(include: list[str]) -> None:
    """Copies the committed bundle files into build/bundle and limits `include` to the resources present."""
    BUNDLE.mkdir(parents=True, exist_ok=True)
    for source in (HERE / "bundle").iterdir():
        shutil.copy(source, BUNDLE / source.name)
    shutil.copy(ACCEPTANCE_RUNNER, BUNDLE / "acceptance_runner.py")
    config = yaml.safe_load((BUNDLE / "databricks.yml").read_text())
    config["include"] = include
    (BUNDLE / "databricks.yml").write_text(yaml.safe_dump(config, sort_keys=False))


def materialize(variant: str, jobs: bool) -> None:
    """Replaces the bundle's project, profiles and (optionally) generated jobs with a variant's files."""
    source = VARIANTS / variant
    names = ["project", "profiles"] + (["reuse_artifacts"] if jobs else [])
    for name in names:
        target = BUNDLE / name
        if target.exists():
            shutil.rmtree(target)
        # copy (not copy2): copy2 keeps the source mtime, which DAB's sync uses to skip unchanged files, so a
        # variant switch would silently leave the previous variant's edited files in the workspace.
        shutil.copytree(source / name, target, copy_function=shutil.copy)
    if jobs:
        for name in BUNDLE.glob("prepare_dbt_reuse_*.py"):
            name.unlink()
        for name in [*source.glob("prepare_dbt_reuse_*.py"), source / "off.yml", source / "on.yml"]:
            shutil.copy(name, BUNDLE / name.name)


def deploy(wheel_path: str) -> None:
    bundle_cmd("deploy", "--auto-approve", extra=["--var", f"wheel_path={wheel_path}"])


def bundle_summary() -> dict[str, Any]:
    return json.loads(bundle_cmd("summary", "--output", "json"))


def generated_job_files(
    variant: str,
    arm_target: str,
    arm: str,
    manifest: Path,
    template: Path,
    project_dir: str,
    profiles_dir: str,
    reuse: list[str],
) -> None:
    """Runs the Factory CLI for one arm and instruments the job for metrics collection."""
    factory = Path(sys.executable).parent / "databricks_dbt_factory"
    output = VARIANTS / variant / f"{arm}.yml"
    command = [
        str(factory), "--dbt-manifest-path", str(manifest), "--input-job-spec-path", str(template),
        "--target-job-spec-path", str(output), "--source", "WORKSPACE", "--target", arm_target,
        "--task-type", "notebook", "--notebook-path", "./acceptance_runner.py", "--bundle-tests",
        "--project-directory", project_dir, "--profiles-directory", profiles_dir, *reuse,
    ]  # fmt: skip
    run_cmd(command)
    document = yaml.safe_load(output.read_text())
    instrument(document, arm)
    output.write_text(yaml.safe_dump(document, sort_keys=False))


def instrument(document: dict[str, Any], arm: str) -> None:
    """Adds the runner's probe parameters, as the acceptance harness does, so each task writes a metrics file."""
    defaults = {"probe_stage": "", "report_dir": "", "probe_reuse_enabled": "true" if arm == "on" else "false"}
    for job in document["resources"]["jobs"].values():
        job.setdefault("parameters", []).extend({"name": k, "default": v} for k, v in defaults.items())
        for task in job["tasks"]:
            if "notebook_task" not in task:
                continue
            base = task["notebook_task"].setdefault("base_parameters", {})
            base.update({
                "probe_job_id": "{{job.id}}",
                "probe_run_id": "{{job.run_id}}",
                "probe_task_key": "{{task.name}}",
                "probe_attempt": "{{task.execution_count}}",
            })  # fmt: skip
            base.update({k: "{{job.parameters." + k + "}}" for k in defaults})


def job_template(arm: str, wheel_path: str) -> dict[str, Any]:
    return {
        "resources": {
            "jobs": {
                f"bench_{arm}": {
                    "name": f"jaffle_bench_{arm}",
                    "max_concurrent_runs": 1,
                    "timeout_seconds": 7200,
                    "tags": {"purpose": "jaffle-shop-reuse-benchmark"},
                    "environments": [
                        {
                            "environment_key": "Default",
                            # dbt pins come prebuilt from bundle/dbt_base_environment.yml (`make bench-env`).
                            "spec": {"base_environment": BASE_ENVIRONMENT, "dependencies": [wheel_path]},
                        }
                    ],
                }
            }
        }
    }


# ---- workspace --------------------------------------------------------------------------------------------


class Workspace:
    """Thin SDK wrapper: schemas, volume, job runs, SQL and results."""

    def __init__(self) -> None:
        from databricks.sdk import WorkspaceClient

        self.client = WorkspaceClient(profile=PROFILE)
        self.user = self.client.current_user.me().user_name
        self.file_path = f"/Workspace/Users/{self.user}/.bundle/{BUNDLE_NAME}/{TARGET}/files"
        self.warehouse_id = ""

    def ensure_schemas(self) -> None:
        from databricks.sdk.errors import NotFound

        for schema in (RAW, OFF, ON, RESULTS):
            try:
                self.client.schemas.get(f"{CATALOG}.{schema}")
            except NotFound:
                self.client.schemas.create(name=schema, catalog_name=CATALOG)
        try:
            self.client.volumes.read(f"{CATALOG}.{ON}.{ARTIFACTS}")
        except NotFound:
            from databricks.sdk.service.catalog import VolumeType

            self.client.volumes.create(catalog_name=CATALOG, schema_name=ON, name=ARTIFACTS,
                                       volume_type=VolumeType.MANAGED)  # fmt: skip

    def drop_schemas(self) -> None:
        from databricks.sdk.errors import NotFound

        for schema in (RAW, OFF, ON):
            try:
                self.client.schemas.delete(f"{CATALOG}.{schema}", force=True)
            except NotFound:
                pass

    def upload_wheel(self, wheel: Path) -> str:
        import hashlib

        content = wheel.read_bytes()
        digest = hashlib.sha256(content).hexdigest()[:12]
        path = f"{VOLUME_ROOT}/wheels/{digest}/{wheel.name}"
        self.client.files.create_directory(f"{VOLUME_ROOT}/wheels/{digest}")
        self.client.files.upload(path, io.BytesIO(content), overwrite=True)
        return path

    def run_job(self, job_id: int, parameters: dict[str, str], *, cancel_on_exit: list[int]) -> Any:
        from databricks.sdk.service.jobs import RunLifeCycleState

        run_id = self.client.jobs.run_now(job_id, job_parameters=parameters).run_id
        cancel_on_exit.append(run_id)
        terminal = {RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR}
        deadline = time.monotonic() + 2400
        while True:
            run = self.client.jobs.get_run(run_id)
            if run.state.life_cycle_state in terminal:
                break
            if time.monotonic() > deadline:
                raise TimeoutError(f"run {run_id} exceeded 40 minutes")
            time.sleep(15)
        cancel_on_exit.remove(run_id)
        if run.state.result_state.value != "SUCCESS":
            raise RuntimeError(f"run {run_id} finished {run.state.result_state}: {run.state.state_message}")
        return run

    def control(self, job_id: int, parameters: dict[str, str], cancel_on_exit: list[int]) -> Any:
        run = self.run_job(job_id, parameters, cancel_on_exit=cancel_on_exit)
        task = run.tasks[0]
        output = self.client.jobs.get_run_output(task.run_id)
        return json.loads(output.notebook_output.result)

    def download(self, path: str) -> bytes:
        with self.client.files.download(path).contents as stream:
            return stream.read()

    def sql(self, statement: str) -> list[list[Any]]:
        from databricks.sdk.service.sql import StatementState

        result = self.client.statement_execution.execute_statement(
            statement=statement, warehouse_id=self.warehouse_id, wait_timeout="50s"
        )
        while result.status.state in {StatementState.PENDING, StatementState.RUNNING}:
            time.sleep(2)
            result = self.client.statement_execution.get_statement(result.statement_id)
        if result.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f"SQL failed: {result.status.error}")
        return list(result.result.data_array or []) if result.result else []

    def metrics(self, report_dir: str, run_id: int) -> list[dict[str, Any]]:
        found = []
        try:
            entries = list(self.client.files.list_directory_contents(report_dir))
        except Exception:  # noqa: BLE001 - a run that wrote no metrics is reported as such in the rows
            return found
        for entry in entries:
            if entry.path.endswith(".json") and not entry.is_directory:
                metric = json.loads(self.download(entry.path))
                if "invocations" in metric and str(metric.get("run_id")) == str(run_id):
                    found.append(metric)
        return found

    def query_ms(self, statements: list[str]) -> dict[str, float]:
        if not statements:
            return {}
        ids = ", ".join(lit(s) for s in sorted(set(statements)))
        rows = self.sql(
            f"SELECT statement_id, execution_duration_ms FROM system.query.history WHERE statement_id IN ({ids})"
        )
        return {row[0]: float(row[1]) for row in rows if row[1] is not None}


# ---- orchestration ----------------------------------------------------------------------------------------


class Benchmark:
    def __init__(self, sf: float, reps: int, run_id: str, arms: tuple[str, ...], scenarios: tuple[str, ...]) -> None:
        self.sf, self.reps, self.run_id = sf, reps, run_id
        self.arms, self.scenarios = arms, scenarios
        self.ws = Workspace()
        self.factory_sha = run_cmd(["git", "-C", str(REPO), "rev-parse", "HEAD"]).strip()
        self.factory_version = run_cmd(["git", "-C", str(REPO), "describe", "--tags", "--always"]).strip()
        self.job_ids: dict[str, int] = {}
        self.cancel_on_exit: list[int] = []
        self.variant = "base"
        self.rows: list[dict[str, Any]] = []
        self.reuse_config = {
            "format_version": 1,
            "project_id": "jaffle_shop_bench",
            "environment": "bench",
            "owner_key": "jaffle_bench",
            "state_catalog": CATALOG,
            "state_schema": ON,
            "artifact_volume": {"catalog": CATALOG, "schema": ON, "volume": ARTIFACTS},
        }

    def setup(self) -> None:
        self.ws.ensure_schemas()
        wheel = self.ws.upload_wheel(build_wheel())
        self.wheel_path = wheel
        make_variants()
        # Phase A: the warehouse and control job only, so the profile can name the warehouse.
        stage_bundle(["warehouse.yml", "control.yml"])
        deploy(wheel)
        summary = bundle_summary()
        self.ws.file_path = summary["workspace"]["file_path"]
        self.ws.warehouse_id = summary["resources"]["sql_warehouses"]["bench"]["id"]
        self.control_job = int(summary["resources"]["jobs"]["bench_control"]["id"])
        for variant in ("base", "edit"):
            write_profiles(VARIANTS / variant / "profiles", self.ws.warehouse_id)
            build_partial_parse(variant)
        for variant in ("base", "edit"):
            self.capture_variant(variant)
        self.generate_variant("base")
        self.generate_variant("edit")
        self.materialize_and_deploy("base", all_jobs=True)
        self.job_ids = {arm: int(bundle_summary()["resources"]["jobs"][f"bench_{arm}"]["id"]) for arm in ARM_TARGET}

    def materialize_and_deploy(self, variant: str, all_jobs: bool = False) -> None:
        includes = ["warehouse.yml", "control.yml"] + (["off.yml", "on.yml"] if all_jobs else [])
        stage_bundle(includes)
        materialize(variant, jobs=all_jobs)
        deploy(self.wheel_path)
        self.variant = variant

    def capture_variant(self, variant: str) -> None:
        self.materialize_and_deploy(variant)
        ref = f"{self.run_id}-{variant}"
        result = self.ws.control(
            self.control_job,
            {
                "action": "capture", "variant": variant, "target": ARM_TARGET["on"], "deployment_ref": ref,
                "volume_root": VOLUME_ROOT, "arm": "on", "arm_schema": ON, "sf": str(self.sf),
                "reuse_config": json.dumps(self.reuse_config),
            },
            self.cancel_on_exit,
        )  # fmt: skip
        out = VARIANTS / variant
        out.mkdir(parents=True, exist_ok=True)
        (out / "manifest.json").write_bytes(self.ws.download(result["manifest"]))
        (out / "provenance.json").write_bytes(self.ws.download(result["provenance"]))

    def generate_variant(self, variant: str) -> None:
        from databricks_dbt_factory.reuse_runtime import publish_artifacts, read_descriptor

        out = VARIANTS / variant
        project = f"{self.ws.file_path}/project"
        profiles = f"{self.ws.file_path}/profiles"
        config_path = out / "reuse.json"
        config_path.write_text(json.dumps(self.reuse_config))
        manifest, provenance = out / "manifest.json", out / "provenance.json"
        generated_job_files(variant, ARM_TARGET["off"], "off", manifest, self._template("off"), project, profiles, [])
        reuse = ["--reuse-config", str(config_path), "--reuse-provenance", str(provenance)]
        # Generation keeps earlier deployments in reuse_artifacts/; publish only the one this job references.
        shutil.rmtree(out / "reuse_artifacts", ignore_errors=True)
        generated_job_files(variant, ARM_TARGET["on"], "on", manifest, self._template("on"), project, profiles, reuse)
        descriptors = list((out / "reuse_artifacts").glob("descriptor_*.json"))
        assert len(descriptors) == 1, f"expected one generated reuse descriptor, found {len(descriptors)}"
        descriptor_path = descriptors[0]
        descriptor = read_descriptor(descriptor_path.read_bytes())
        local = tuple(descriptor_path.parent / name for name in (
            descriptor["manifest"], descriptor["provenance"], descriptor_path.name,
        ))  # fmt: skip
        publish_artifacts(self.ws.client, local, VOLUME_ROOT)

    def _template(self, arm: str) -> Path:
        path = BUILD / f"template_{arm}.yml"
        path.write_text(yaml.safe_dump(job_template(arm, self.wheel_path), sort_keys=False))
        return path

    def step(self, item: Step) -> None:
        if item.kind == "variant":
            if item.variant != self.variant:
                self.materialize_and_deploy(item.variant, all_jobs=True)
        elif item.kind in {"reset", "append"}:
            params = {"action": item.kind, "sf": str(self.sf), "arm": item.arm, "arm_schema": ARM_SCHEMA[item.arm],
                      "reuse_config": json.dumps(self.reuse_config)}  # fmt: skip
            started = time.time()
            output = self.ws.control(self.control_job, params, self.cancel_on_exit)
            self.rows.append(self._control_row(item, output, started))
        elif item.kind == "run":
            self.run_arm(item)
        self.flush()

    def _control_row(self, item: Step, output: dict[str, Any], started: float) -> dict[str, Any]:
        return {
            "benchmark_run_id": self.run_id, "rep": item.rep, "arm": item.arm, "scenario": item.scenario or item.kind,
            "variant": self.variant, "step_kind": item.kind, "task_key": "control", "task_state": "SUCCESS",
            "task_seconds": time.time() - started, "run_seconds": None, "reused": None,
            "dbt_run_invocations": None, "dbt_test_invocations": None, "reuse_reason": None,
            "warehouse_query_ms": None, "dbus": None, "job_run_id": None,
            "factory_git_sha": self.factory_sha, "factory_version": self.factory_version,
            "jaffle_shop_sha": JAFFLE_SHA, "scale_factor": self.sf, "warehouse_size": "2X-Small",
        }  # fmt: skip

    def run_arm(self, item: Step) -> None:
        report_dir = f"{VOLUME_ROOT}/metrics/{self.run_id}/{item.rep}-{item.arm}-{item.scenario}"
        self.ws.client.files.create_directory(report_dir)
        params = {"probe_stage": f"{item.rep}-{item.arm}-{item.scenario}", "report_dir": report_dir}
        started = time.time()
        run = self.ws.run_job(self.job_ids[item.arm], params, cancel_on_exit=self.cancel_on_exit)
        self._record_run(item, run, report_dir, started)

    def _record_run(self, item: Step, run: Any, report_dir: str, started: float) -> None:
        run_id = run.run_id
        tasks = [
            {
                "task_key": t.task_key, "state": t.state.result_state.value if t.state and t.state.result_state else None,
                "start_ms": t.start_time, "end_ms": t.end_time,
            }
            for t in run.tasks or []
        ]  # fmt: skip
        metrics = self.ws.metrics(report_dir, run_id)
        statements = [e["query_id"] for m in metrics for e in m.get("dbsql", []) if e.get("query_id")]
        run_info = {"run_id": run_id, "start_ms": run.start_time, "end_ms": run.end_time}
        meta = {
            "benchmark_run_id": self.run_id, "rep": item.rep, "arm": item.arm, "scenario": item.scenario,
            "variant": self.variant, "step_kind": "run", "factory_git_sha": self.factory_sha,
            "factory_version": self.factory_version, "jaffle_shop_sha": JAFFLE_SHA, "scale_factor": self.sf,
            "warehouse_size": "2X-Small", "dbus": None,
        }  # fmt: skip
        rows = result_rows(meta, run_info, tasks, metrics, self.ws.query_ms(statements))
        if not metrics:
            rows.append({**meta, "job_run_id": run_id, "task_key": "no-metrics", "task_state": "UNKNOWN"})
        self.rows.extend(rows)

    def flush(self) -> None:
        if not self.rows:
            return
        self.ws.sql(RESULTS_DDL)
        self.ws.sql(insert_statement(self.rows))
        self.rows = []

    def run(self) -> None:
        self.setup()
        for item in plan(self.reps, self.arms, self.scenarios):
            self.step(item)
        print(f"benchmark {self.run_id} finished; rows appended to {RESULTS_TABLE}")


def run_command(args: argparse.Namespace) -> None:
    arms = tuple(a for a in args.arms.split(",") if a)
    scenarios = tuple(s for s in args.scenarios.split(",") if s)
    if not set(arms) <= {"off", "on"} or not set(scenarios) <= set(SCENARIOS):
        raise SystemExit("--arms must be off/on and --scenarios must be among " + ",".join(SCENARIOS))
    bench = Benchmark(args.sf, args.reps, uuid.uuid4().hex[:12], arms, scenarios)
    try:
        bench.run()
    except BaseException:
        for run_id in bench.cancel_on_exit:
            bench.ws.client.jobs.cancel_run(run_id)
        if bench.ws.warehouse_id and bench.rows:
            bench.flush()
        if not args.keep:
            clean_command(args)
        raise


def clean_command(_args: argparse.Namespace) -> None:
    if (BUNDLE / "databricks.yml").exists():
        bundle_cmd("destroy", "--auto-approve")
    Workspace().drop_schemas()
    print("bundle destroyed and raw/off/on schemas dropped; results schema kept")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    run_parser = sub.add_parser("run", help="deploy, run every scenario for both arms, and append results")
    run_parser.add_argument("--sf", type=float, default=0.001, help="raw data scale factor")
    run_parser.add_argument("--reps", type=int, default=1, help="repetitions with alternating arm order")
    run_parser.add_argument("--arms", default="off,on", help="comma list of arms to run, e.g. on")
    run_parser.add_argument("--scenarios", default=",".join(SCENARIOS), help="comma list of scenarios to run")
    run_parser.add_argument("--keep", action="store_true", help="keep the bundle and schemas after the run")
    run_parser.set_defaults(func=run_command)
    clean_parser = sub.add_parser("clean", help="destroy the bundle and drop the raw/off/on schemas")
    clean_parser.set_defaults(func=clean_command)
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
