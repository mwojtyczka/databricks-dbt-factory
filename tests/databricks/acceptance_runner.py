# Databricks notebook source
"""Measure the shipped runner without importing dbt on a reused model path."""
# pylint: skip-file
# mypy: ignore-errors

import builtins
import io
import hashlib
import json
import os
import re
import runpy
import sys
import time
from importlib import resources


def sql_event(statement):
    text = str(statement)
    sql = re.sub(r"\A(?:\s+|/\*.*?\*/|--[^\n]*(?:\n|$))*", "", text, flags=re.S)
    return {
        "kind": sql.split(None, 1)[0].upper() if sql else "",
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "elapsed_seconds": 0.0,
    }


class ObservedFrame:
    def __init__(self, frame, event):
        self.frame, self.event = frame, event

    def __getattr__(self, name):
        return getattr(self.frame, name)

    def _action(self, name, *args, **kwargs):
        started = time.monotonic()
        try:
            return getattr(self.frame, name)(*args, **kwargs)
        finally:
            self.event["elapsed_seconds"] += time.monotonic() - started

    def collect(self, *args, **kwargs):
        return self._action("collect", *args, **kwargs)

    def first(self, *args, **kwargs):
        return self._action("first", *args, **kwargs)


class ObservedSpark:
    def __init__(self, spark, events):
        self.spark, self.events = spark, events

    def __getattr__(self, name):
        return getattr(self.spark, name)

    def sql(self, statement, *args, **kwargs):
        event = sql_event(statement)
        self.events.append(event)
        started = time.monotonic()
        try:
            return ObservedFrame(self.spark.sql(statement, *args, **kwargs), event)
        finally:
            event["elapsed_seconds"] += time.monotonic() - started


def execute(dbutils, spark, *, notebook=None, client=None):
    from databricks_dbt_factory import reuse_runtime as runtime

    for key, default in {
        "report_dir": "",
        "probe_stage": "",
        "probe_reuse_enabled": "true",
        "probe_environment": "1",
        "probe_job_id": "",
        "probe_run_id": "",
        "probe_task_key": "",
        "probe_attempt": "",
    }.items():
        dbutils.widgets.text(key, default)
    enabled = dbutils.widgets.get("probe_reuse_enabled")
    if enabled not in {"true", "false"}:
        raise ValueError("probe_reuse_enabled must be true or false")
    report_dir = dbutils.widgets.get("report_dir")
    if report_dir and (not report_dir.startswith("/Volumes/") or ".." in report_dir.split("/")):
        raise ValueError("report_dir must be an absolute Volume directory")
    report = {
        "stage": dbutils.widgets.get("probe_stage"),
        "job_id": dbutils.widgets.get("probe_job_id"),
        "run_id": dbutils.widgets.get("probe_run_id"),
        "task_key": dbutils.widgets.get("probe_task_key"),
        "attempt": dbutils.widgets.get("probe_attempt"),
        "invocations": {"run": 0, "test": 0, "parse": 0},
        "imported_dbt": False,
        "dbt_loaded_before": any(name == "dbt" or name.startswith("dbt.") for name in sys.modules),
        "reason": None,
        "publication": None,
        "success": False,
        "dbsql": [],
        "spark_sql": [],
    }
    original_import = builtins.__import__
    original_begin = runtime.begin_model
    original_enabled = runtime.REUSE_ENABLED
    previous_environment = os.environ.get("FACTORY_ACCEPTANCE_CONTEXT")
    wrapped = {}
    observed = []
    cursor_methods = {}

    def observe_cursor(cursor):
        if cursor is None or cursor in cursor_methods:
            return
        original_execute = cursor.execute
        cursor_methods[cursor] = original_execute

        def execute_sql(instance, statement, *args, **kwargs):
            event = sql_event(statement)
            report["dbsql"].append(event)
            report_dir = dbutils.widgets.get("report_dir")
            if report_dir and event["kind"] in {"CREATE", "INSERT", "MERGE"}:
                try:
                    from databricks.sdk import WorkspaceClient

                    marker = {
                        "kind": event["kind"],
                        "task_key": report["task_key"],
                        "stage": report["stage"],
                        "phase": "cursor_execute",
                        "sha256": event["sha256"],
                    }
                    (client or WorkspaceClient()).files.upload(
                        report_dir.rstrip("/") + "/executing.json",
                        io.BytesIO(json.dumps(marker).encode()),
                        overwrite=True,
                    )
                except Exception as error:
                    print("[acceptance] executing marker failed: " + type(error).__name__)
            started = time.monotonic()
            try:
                return original_execute(instance, statement, *args, **kwargs)
            finally:
                event["elapsed_seconds"] = time.monotonic() - started
                try:
                    event["query_id"] = instance.query_id
                except Exception:
                    event["query_id"] = None

        cursor.execute = execute_sql

    def begin(*args, **kwargs):
        model_run = original_begin(*args, **kwargs)
        observed.append(model_run)
        return model_run

    def observe_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "dbt" or name.startswith("dbt."):
            report["imported_dbt"] = True
        module = original_import(name, globals, locals, fromlist, level)
        # Patch only once dbt has loaded itself. Calling original_import above preserves
        # normal circular-import behavior and does not import dbt on the skip path.
        main = sys.modules.get("dbt.cli.main")
        runner = getattr(main, "dbtRunner", None)
        if runner is not None and runner not in wrapped:
            original_invoke = runner.invoke
            wrapped[runner] = original_invoke

            def invoke(instance, args, *extra, **kwargs):
                for argument in args:
                    if argument in report["invocations"]:
                        report["invocations"][argument] += 1
                        break
                return original_invoke(instance, args, *extra, **kwargs)

            runner.invoke = invoke
        connector = sys.modules.get("databricks.sql.client")
        observe_cursor(getattr(connector, "Cursor", None))
        return module

    started = time.monotonic()
    try:
        runtime.REUSE_ENABLED = enabled == "true"
        os.environ["FACTORY_ACCEPTANCE_CONTEXT"] = dbutils.widgets.get("probe_environment")
        runtime.begin_model = begin
        builtins.__import__ = observe_import
        path = notebook or resources.files("databricks_dbt_factory") / "notebook" / "run_dbt_command.py"
        result = runpy.run_path(
            str(path), init_globals={"dbutils": dbutils, "spark": ObservedSpark(spark, report["spark_sql"])}
        )
        report["success"] = True
        if not observed and result.get("model_run") is not None:
            observed.append(result["model_run"])
    finally:
        builtins.__import__ = original_import
        runtime.begin_model = original_begin
        runtime.REUSE_ENABLED = original_enabled
        if previous_environment is None:
            os.environ.pop("FACTORY_ACCEPTANCE_CONTEXT", None)
        else:
            os.environ["FACTORY_ACCEPTANCE_CONTEXT"] = previous_environment
        for cursor, method in cursor_methods.items():
            cursor.execute = method
        for runner, invoke in wrapped.items():
            runner.invoke = invoke
        if observed:
            report["reason"] = observed[-1].reason
            report["publication"] = observed[-1].publication
        report["elapsed_seconds"] = time.monotonic() - started
        report_dir = dbutils.widgets.get("report_dir")
        if report_dir:
            from databricks.sdk import WorkspaceClient

            filename = (
                "_".join(
                    re.sub(r"[^A-Za-z0-9.-]", "-", str(report[key])) or "unknown"
                    for key in ("stage", "job_id", "run_id", "task_key", "attempt")
                )
                + ".json"
            )
            report["report_path"] = report_dir.rstrip("/") + "/" + filename
            # Metrics are auxiliary: an upload failure must not replace a dbt/test failure.
            try:
                (client or WorkspaceClient()).files.upload(
                    report["report_path"], io.BytesIO(json.dumps(report).encode()), overwrite=True
                )
            except Exception as error:
                print("[acceptance] report upload failed: " + type(error).__name__)
        print("[acceptance] " + json.dumps(report))
    return report


if __name__ == "__main__":
    from databricks.sdk.runtime import dbutils, spark

    dbutils.notebook.exit(json.dumps(execute(dbutils, spark)))
