# Databricks notebook source
"""Run real dbt model/test attempts through the shipped runner on serverless."""
# pylint: skip-file
# mypy: ignore-errors

import builtins
import io
import json
import os
import platform
import runpy
import tempfile
import time
from importlib import resources
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils, spark
from dbt.cli.main import dbtRunner

from databricks_dbt_factory import reuse_runtime as runtime
from databricks_dbt_factory.__version__ import __version__
from databricks_dbt_factory.dbt_factory import DbtFactory
from databricks_dbt_factory.reuse import Relation, ReuseConfig
from databricks_dbt_factory.reuse_prepare import capture_parse

catalog, schema, volume, warehouse, job_id, run_id = (
    dbutils.widgets.get(key) for key in ("catalog", "schema", "volume", "warehouse_id", "job_id", "run_id")
)
assert dbutils.widgets.get("mode") == "serverless"
w = WorkspaceClient()
config_value = {
    "format_version": 1,
    "project_id": "factory_runtime_probe",
    "environment": "scratch",
    "owner_key": "probe",
    "state_catalog": catalog,
    "state_schema": schema,
    "artifact_volume": {"catalog": catalog, "schema": schema, "volume": volume},
}
config = ReuseConfig.from_dict(config_value)
report = {
    "compute": "serverless",
    "factory_version": __version__,
    "python": platform.python_version(),
    "spark": spark.version,
    "scope": "real dbt and shipped notebook; repaired attempts simulated within one serverless task; not jobs.repair_run",
    "checks": {},
    "invocations": {"run": 0, "test": 0, "parse": 0},
}


def table(name):
    return runtime.quote_relation(Relation(catalog, schema, name))


def authenticate():
    os.environ["DBT_ACCESS_TOKEN"] = w.config.authenticate()["Authorization"].removeprefix("Bearer ").strip()


def run_checks(root):
    project, profiles = root / "project", root / "profiles"
    for directory in (project / "models", project / "tests", profiles):
        directory.mkdir(parents=True)
    (project / "dbt_project.yml").write_text(
        "name: factory_runtime_probe\nversion: '1.0'\nconfig-version: 2\nprofile: probe\n"
    )
    (project / "models" / "orders.sql").write_text(
        "{{ config(materialized='table', meta={'dbt_factory': {'reuse': {'enabled': true}}}) }}\n"
        "select * from {{ source('raw', 'input_table') }}"
    )
    (project / "models" / "sources.yml").write_text(
        json.dumps(
            {
                "version": 2,
                "sources": [
                    {
                        "name": "raw",
                        "database": catalog,
                        "schema": schema,
                        "tables": [{"name": "input_table"}, {"name": "test_control"}],
                    }
                ],
            }
        )
    )
    (project / "tests" / "required_gate.sql").write_text(
        "select o.* from {{ ref('orders') }} o cross join {{ source('raw', 'test_control') }} c where c.fail"
    )
    profile_path = profiles / "profiles.yml"
    profile_path.write_text(
        json.dumps(
            {
                "probe": {
                    "target": "probe",
                    "outputs": {
                        "probe": {
                            "type": "databricks",
                            "catalog": catalog,
                            "schema": schema,
                            "threads": 1,
                            "host": urlparse(w.config.host).netloc,
                            "http_path": f"/sql/1.0/warehouses/{warehouse}",
                            "token": "{{ env_var('DBT_ACCESS_TOKEN') }}",
                        }
                    },
                }
            }
        )
    )
    runtime.initialize_state(spark, config)
    spark.sql(f"CREATE TABLE {table('input_table')} USING DELTA AS SELECT 1 AS id").collect()
    spark.sql(f"CREATE TABLE {table('test_control')} USING DELTA AS SELECT true AS fail").collect()
    identity = {
        "project_id": config.project_id,
        "environment": config.environment,
        "workspace_id": str(w.get_workspace_id()),
        "metastore_id": w.metastores.summary().metastore_id,
        "principal": w.current_user.me().user_name,
    }
    authenticate()
    manifest, provenance = capture_parse(project, profiles, identity=identity, target="probe", deployment_ref=run_id)
    os.chdir(project)
    parsed = json.loads(manifest)
    local = runtime.write_bundle(runtime.prepare_bundle(manifest, provenance, config_value), root / "artifacts")
    refs = runtime.publish_artifacts(w, local, f"/Volumes/{catalog}/{schema}/{volume}")
    descriptor = next(ref for name, ref in refs.items() if name.startswith("descriptor_"))
    uid = "model.factory_runtime_probe.orders"
    peers = parsed["nodes"] | parsed["sources"] | parsed.get("unit_tests", {})
    select = DbtFactory._node_select(peers[uid], peers=peers)
    test_uid = next(key for key in parsed["nodes"] if key.startswith("test."))
    test_select = DbtFactory._node_select(peers[test_uid], peers=peers)
    values = {
        "project_directory": "",
        "profiles_directory": str(profiles),
        "factory_reuse_config": json.dumps(config_value),
        "factory_reuse_deployment": json.dumps(descriptor),
        "factory_reuse_model": uid,
        "factory_job_id": job_id,
        "factory_run_id": run_id,
        "factory_task_key": "orders",
        "factory_attempt_number": "0",
        "factory_secret_versions": "{}",
        "factory_dbt_vars": "{}",
    }
    notebook = resources.files("databricks_dbt_factory") / "notebook" / "run_dbt_command.py"
    original_invoke = dbtRunner.invoke

    def counted(runner, args):
        if args[0] in report["invocations"]:
            report["invocations"][args[0]] += 1
        return original_invoke(runner, args)

    def execute(*, test=False, reject_import=False):
        current = dict(values)
        current["dbt_commands"] = json.dumps(
            [f"dbt test --select {test_select} --target probe" if test else f"dbt run --select {select} --target probe"]
        )
        if test:
            current["factory_reuse_config"] = ""
        widgets = SimpleNamespace(text=current.setdefault, get=current.__getitem__)
        original_import = builtins.__import__

        def guard(name, *args, **kwargs):
            if name == "dbt" or name.startswith("dbt."):
                raise AssertionError("reused model imported dbt")
            return original_import(name, *args, **kwargs)

        try:
            if reject_import:
                builtins.__import__ = guard
            result = runpy.run_path(
                str(notebook), init_globals={"spark": spark, "dbutils": SimpleNamespace(widgets=widgets)}
            )
        finally:
            builtins.__import__ = original_import
        assert "DBT_ACCESS_TOKEN" not in os.environ
        return result["model_run"]

    def prepare(attempt):
        authenticate()
        prepared = runtime.prepare_run(
            spark,
            w,
            config,
            descriptor,
            project,
            profiles,
            runtime.Attempt(job_id, run_id, "prepare", attempt),
            model_ids=[uid],
        )
        values["factory_reuse_preparation"] = json.dumps(runtime.publish_preparation(w, config, prepared))
        return prepared

    dbtRunner.invoke = counted
    try:
        first = execute()
        assert first.publication == "RECORDED", (first.reason, first.publication)
        prepare(0)
        for attempt, refreshed in ((1, False), (2, True)):
            try:
                execute(test=True)
            except RuntimeError as error:
                assert "dbt command failed" in str(error)
            else:
                raise AssertionError("required test unexpectedly passed")
            spark.sql(f"UPDATE {table('test_control')} SET fail = false").collect()
            values["factory_attempt_number"] = str(attempt)
            if refreshed:
                prepare(attempt)
            assert execute(reject_import=True).reason == "REUSED"
            execute(test=True)
            spark.sql(f"UPDATE {table('test_control')} SET fail = true").collect()
        assert report["invocations"]["run"] == 1
        assert report["invocations"]["test"] == 4
        report["checks"]["failed_test_then_repair_and_test_rerun"] = {
            "with_preparation": True,
            "without_preparation": True,
        }
        spark.sql(f"INSERT INTO {table('input_table')} VALUES (2)").collect()
        values["factory_attempt_number"] = "3"
        changed = execute()
        assert changed.reason == "INPUT_CHANGED" and changed.publication == "RECORDED", (
            changed.reason,
            changed.publication,
        )
        values["factory_attempt_number"] = "4"
        stale = execute()
        assert stale.reason == "PREPARATION_STALE" and stale.publication == "RECORDED", (
            stale.reason,
            stale.publication,
        )
        prepare(3)
        values["factory_attempt_number"] = "5"
        assert execute(reject_import=True).reason == "REUSED"
        report["checks"]["changed_input_and_stale_preparation"] = True
        profile = profile_path.read_text()
        profile_path.write_text(profile.replace(warehouse, "changed"))
        try:
            execute(reject_import=True)
        except ValueError as error:
            assert "profiles digest mismatch" in str(error)
        else:
            raise AssertionError("profile edit authorized reuse")
        finally:
            profile_path.write_text(profile)
        report["checks"]["profile_edit_refused_before_dbt_import"] = True
        builds = table("factory_reuse_builds")
        saved = table("saved_builds")
        count = spark.sql(f"SELECT count(*) FROM {builds}").first()[0]
        spark.sql(f"ALTER TABLE {builds} RENAME TO {saved}").collect()
        try:
            values.update(factory_attempt_number="6", factory_force_rebuild="true")
            outage = execute()
            assert outage.publication == "STATE_UNAVAILABLE", (outage.reason, outage.publication)
            assert spark.sql(f"SELECT count(*) FROM {saved}").first()[0] == count
        finally:
            spark.sql(f"ALTER TABLE {saved} RENAME TO {builds}").collect()
        report["checks"]["state_write_outage_preserves_real_model_success"] = True
        assert report["invocations"]["run"] == 4, report["invocations"]
    finally:
        dbtRunner.invoke = original_invoke


started, previous_cwd, previous_gate = time.monotonic(), Path.cwd(), runtime.REUSE_ENABLED
try:
    runtime.REUSE_ENABLED = True  # Probe process only; release gate remains disabled in the wheel.
    with tempfile.TemporaryDirectory(prefix="factory-runtime-probe-") as directory:
        root = Path(directory)
        os.chdir(root)
        run_checks(root)
    report["passed"] = True
except Exception as error:
    report.update(passed=False, error=str(error), error_type=type(error).__name__)
finally:
    runtime.REUSE_ENABLED = previous_gate
    os.chdir(previous_cwd)
    os.environ.pop("DBT_ACCESS_TOKEN", None)
report["seconds"] = time.monotonic() - started
payload = json.dumps(report)
w.files.upload(f"/Volumes/{catalog}/{schema}/{volume}/report.json", io.BytesIO(payload.encode()), overwrite=False)
if not report["passed"]:
    raise AssertionError(f"Runtime probe failed: {report['error']}; details in Volume report.json")
dbutils.notebook.exit(payload)
