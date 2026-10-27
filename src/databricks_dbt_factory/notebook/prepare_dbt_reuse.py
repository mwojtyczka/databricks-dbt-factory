# Databricks notebook source
"""Prepare native code comparisons. Model tasks must still recheck live evidence."""

import json
import os
from pathlib import Path
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient

from databricks_dbt_factory.reuse import ReuseConfig, canonical_json
from databricks_dbt_factory.reuse_runtime import Attempt, EvidenceUnavailable, prepare_run, publish_preparation

# COMMAND ----------

for name in (
    "factory_reuse_config",
    "factory_reuse_deployment",
    "project_directory",
    "profiles_directory",
    "factory_job_id",
    "factory_run_id",
    "factory_task_key",
    "factory_attempt_number",
    "factory_secret_versions",
):
    dbutils.widgets.text(name, "")

config = ReuseConfig.from_dict(json.loads(dbutils.widgets.get("factory_reuse_config")))
deployment = json.loads(dbutils.widgets.get("factory_reuse_deployment"))
attempt = Attempt(
    dbutils.widgets.get("factory_job_id"),
    dbutils.widgets.get("factory_run_id"),
    dbutils.widgets.get("factory_task_key"),
    int(dbutils.widgets.get("factory_attempt_number")),
)
project_directory = dbutils.widgets.get("project_directory")
profiles_directory = dbutils.widgets.get("profiles_directory")
if not project_directory or not profiles_directory:
    raise ValueError("preparation requires explicit project_directory and profiles_directory")
project = Path(project_directory)
if not project.is_absolute():
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    project = (Path("/Workspace" + ctx.notebookPath().get()).parent / project).resolve()
profiles = Path(profiles_directory)
if not profiles.is_absolute():
    profiles = (project / profiles).resolve()

# COMMAND ----------

ws = WorkspaceClient()
try:
    os.environ["DBT_ACCESS_TOKEN"] = ws.config.authenticate()["Authorization"].removeprefix("Bearer ").strip()
    parsed = urlparse(ws.config.host)
    os.environ["DBT_HOST"] = parsed.netloc or parsed.path.strip("/")
    result = prepare_run(
        spark,
        ws,
        config,
        deployment,
        project,
        profiles,
        attempt,
        secret_versions=json.loads(dbutils.widgets.get("factory_secret_versions") or "{}"),
    )
    pointer = {}
    try:
        pointer = publish_preparation(ws, config, result)
    except EvidenceUnavailable as error:
        result["reason"] = str(error)
    print("[dbt-factory] preparation: " + canonical_json({"reason": result["reason"], "models": result["models"]}))
    dbutils.jobs.taskValues.set(key="factory_reuse_preparation", value=canonical_json(pointer))
finally:
    os.environ.pop("DBT_ACCESS_TOKEN", None)
    os.environ.pop("DBT_HOST", None)

dbutils.notebook.exit(canonical_json({"reason": result["reason"], "preparation": pointer}))
