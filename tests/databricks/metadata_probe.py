# Databricks notebook source
"""Empirical primitives for reuse; only writes inside the harness-owned schema."""
# pylint: skip-file
# mypy: ignore-errors

import hashlib
import importlib.metadata
import io
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists

from databricks.sdk.runtime import dbutils, spark

catalog, schema, volume, warehouse_id, mode = (
    dbutils.widgets.get(key) for key in ("catalog", "schema", "volume", "warehouse_id", "mode")
)
assert mode == "serverless"
w = WorkspaceClient()
report = {
    "compute": mode,
    "python": platform.python_version(),
    "spark": spark.version,
    "versions": {name: importlib.metadata.version(name) for name in ("dbt-core", "dbt-databricks", "databricks-sdk")},
    "checks": {},
}


def relation(name):
    return ".".join("`" + part.replace("`", "``") + "`" for part in (catalog, schema, name))


def probe(name, function, required=True):
    start = time.monotonic()
    try:
        result = {"status": "passed", "evidence": function()}
    except Exception as exc:
        result = {"status": "failed" if required else "unavailable", "error": str(exc), "type": type(exc).__name__}
    result["seconds"] = time.monotonic() - start
    report["checks"][name] = result


def identities():
    principals = {
        "spark": spark.sql("SELECT current_user()").first()[0],
        "sdk": w.current_user.me().user_name,
    }
    assert principals["spark"] == principals["sdk"], principals
    return principals


def observe(name):
    start = time.monotonic()
    full_name = f"{catalog}.{schema}.{name}"
    uc_before = w.tables.get(full_name).table_id
    before = spark.sql(f"DESCRIBE DETAIL {relation(name)}").first().asDict()
    version = spark.sql(f"DESCRIBE HISTORY {relation(name)} LIMIT 1").first()["version"]
    after = spark.sql(f"DESCRIBE DETAIL {relation(name)}").first().asDict()
    uc_after = w.tables.get(full_name).table_id
    assert before["format"] == "delta" and before["id"]
    assert uc_before and uc_before == uc_after and before["id"] == after["id"]
    assert isinstance(version, int)
    return {"uc_id": uc_before, "delta_id": before["id"], "version": version, "seconds": time.monotonic() - start}


def delta_metadata():
    spark.sql(f"CREATE TABLE {relation('input_table')} USING DELTA AS SELECT 1 AS id")
    cold = observe("input_table")
    warm = [observe("input_table") for _ in range(3)]
    assert all(
        (item["uc_id"], item["delta_id"], item["version"]) == (cold["uc_id"], cold["delta_id"], cold["version"])
        for item in warm
    )
    spark.sql(f"INSERT INTO {relation('input_table')} VALUES (2)")
    appended = observe("input_table")
    assert appended["version"] > cold["version"]
    assert appended["delta_id"] == cold["delta_id"] and appended["uc_id"] == cold["uc_id"]
    spark.sql(f"DROP TABLE {relation('input_table')}")
    spark.sql(f"CREATE TABLE {relation('input_table')} USING DELTA AS SELECT 1 AS id")
    recreated = observe("input_table")
    assert recreated["version"] == cold["version"]
    assert recreated["delta_id"] != cold["delta_id"] and recreated["uc_id"] != cold["uc_id"]
    return {"first": cold, "repeated": warm, "appended": appended, "recreated": recreated}


def view_metadata():
    view_name = f"{catalog}.{schema}.input_view"
    spark.sql(f"CREATE VIEW {relation('input_view')} AS SELECT id FROM {relation('input_table')}")
    first = w.tables.get(view_name)
    before = observe("input_table")
    spark.sql(f"ALTER VIEW {relation('input_view')} AS SELECT id + 1 AS id FROM {relation('input_table')}")
    changed = w.tables.get(view_name)
    after = observe("input_table")
    assert first.table_type.value == "VIEW" and first.view_definition
    assert first.view_definition != changed.view_definition
    assert (before["delta_id"], before["version"]) == (after["delta_id"], after["version"])
    table = w.tables.get(f"{catalog}.{schema}.input_table")
    return {
        "table_type": table.table_type.value,
        "view_type": first.table_type.value,
        "first_definition": first.view_definition,
        "changed_definition": changed.view_definition,
    }


def state_writes():
    spark.sql(f"CREATE TABLE {relation('builds')} (attempt STRING, evidence STRING) USING DELTA")
    statement = f"""MERGE INTO {relation("builds")} AS target
        USING (SELECT :attempt AS attempt, :evidence AS evidence) AS source
        ON target.attempt = source.attempt
        WHEN NOT MATCHED THEN INSERT *"""
    values = {"attempt": "probe-attempt", "evidence": json.dumps({"quoted": "a'b"})}
    for _ in range(2):
        spark.sql(statement, args=values)
    rows = spark.sql(f"SELECT * FROM {relation('builds')}").collect()
    assert len(rows) == 1 and rows[0]["evidence"] == values["evidence"]
    return {"attempts_after_two_merges": len(rows)}


def volume_files():
    payload = b'{"probe":"immutable artifact"}\n'
    path = f"/Volumes/{catalog}/{schema}/{volume}/artifact.json"
    w.files.upload(path, io.BytesIO(payload), overwrite=False)
    with w.files.download(path).contents as stream:
        actual = stream.read()
    assert hashlib.sha256(actual).digest() == hashlib.sha256(payload).digest()
    try:
        w.files.upload(path, io.BytesIO(b"different"), overwrite=False)
    except AlreadyExists:
        pass
    else:
        raise AssertionError("Duplicate upload unexpectedly overwrote immutable artifact")
    with w.files.download(path).contents as stream:
        assert stream.read() == payload
    return {"sha256": hashlib.sha256(actual).hexdigest(), "duplicate_rejected": True}


def dbt_execution():
    with tempfile.TemporaryDirectory(prefix="factory-dbt-probe-") as directory:
        root = Path(directory)
        (root / "models").mkdir()
        (root / "dbt_project.yml").write_text(
            json.dumps(
                {
                    "name": "factory_probe",
                    "version": "1.0",
                    "config-version": 2,
                    "profile": "probe",
                    "models": {"factory_probe": {"+materialized": "table"}},
                }
            )
        )
        (root / "models" / "identity.sql").write_text(
            f"select current_user() as principal, id from {relation('input_table')}"
        )
        # The short-lived runtime credential never enters artifacts, arguments or reported logs.
        (root / "profiles.yml").write_text(
            json.dumps(
                {
                    "probe": {
                        "target": "probe",
                        "outputs": {
                            "probe": {
                                "type": "databricks",
                                "catalog": catalog,
                                "schema": schema,
                                "host": w.config.host,
                                "http_path": f"/sql/1.0/warehouses/{warehouse_id}",
                                "threads": 1,
                                "token": "{{ env_var('DBT_ENV_SECRET_PROBE_TOKEN') }}",
                            }
                        },
                    }
                }
            )
        )
        env = os.environ.copy()
        env["DBT_ENV_SECRET_PROBE_TOKEN"] = w.config.authenticate()["Authorization"].split(" ", 1)[1]
        env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
        start = time.monotonic()
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from dbt.cli.main import cli; cli()",
                    "run",
                    "--project-dir",
                    str(root),
                    "--profiles-dir",
                    str(root),
                    "--log-path",
                    str(root / "logs"),
                ],
                env=env,
                cwd=root,
                capture_output=True,
                timeout=600,
                check=False,
            )
        finally:
            env.pop("DBT_ENV_SECRET_PROBE_TOKEN", None)
        assert result.returncode == 0, (
            f"dbt failed with exit code {result.returncode}; logs withheld to protect credentials"
        )
        artifacts = json.loads((root / "target" / "run_results.json").read_text())
        assert len(artifacts["results"]) == 1 and artifacts["results"][0]["status"] == "success"
        principal = spark.sql(f"SELECT DISTINCT principal FROM {relation('identity')}").first()[0]
        assert principal == report["checks"]["principals"]["evidence"]["spark"]
        return {
            "principal": principal,
            "seconds": time.monotonic() - start,
            "status": artifacts["results"][0]["status"],
            "adapter_response": artifacts["results"][0]["adapter_response"],
        }


def lineage():
    rows = spark.sql(
        """SELECT source_table_full_name, target_table_full_name, event_time
        FROM system.access.table_lineage
        WHERE event_date >= current_date() - 1 AND target_table_full_name = :target
        ORDER BY event_time DESC LIMIT 20""",
        args={"target": f"{catalog}.{schema}.identity"},
    ).collect()
    return {"access": "accessible", "rows": [row.asDict() for row in rows], "complete": False}


probe("principals", identities)
probe("delta_metadata", delta_metadata)
probe("view_metadata", view_metadata)
probe("state_writes", state_writes)
probe("volume_files", volume_files)
probe("dbt_execution", dbt_execution)
probe("lineage", lineage, required=False)
report["passed"] = all(item["status"] != "failed" for item in report["checks"].values())
payload = json.dumps(report, default=str)
w.files.upload(f"/Volumes/{catalog}/{schema}/{volume}/report.json", io.BytesIO(payload.encode()), overwrite=False)
if not report["passed"]:
    raise AssertionError("Required metadata probe failed; details in the Volume report.json")
dbutils.notebook.exit(payload)
