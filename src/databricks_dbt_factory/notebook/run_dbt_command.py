# Databricks notebook source

import json
import os
import shlex

from dbt.cli.main import dbtRunner

# COMMAND ----------

dbutils.widgets.text("dbt_commands", "")
dbutils.widgets.text("project_directory", "")
dbutils.widgets.text("profiles_directory", "")

dbt_commands = dbutils.widgets.get("dbt_commands")
project_directory = dbutils.widgets.get("project_directory")
profiles_directory = dbutils.widgets.get("profiles_directory")

if not dbt_commands:
    raise ValueError("dbt_commands parameter is required")

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
os.environ["DATABRICKS_TOKEN"] = ctx.apiToken().get()
os.environ["DATABRICKS_HOST"] = ctx.apiUrl().get()

# COMMAND ----------

runner = dbtRunner()

for command_str in json.loads(dbt_commands):
    command_str = command_str.strip()
    if not command_str:
        continue

    if command_str.startswith("dbt "):
        command_str = command_str[4:]

    args = shlex.split(command_str)

    if project_directory:
        args.extend(["--project-dir", project_directory])

    if profiles_directory:
        args.extend(["--profiles-dir", profiles_directory])

    print(f"Running: dbt {' '.join(args)}")
    print("-" * 60)

    result = runner.invoke(args)

    if not result.success:
        raise Exception(f"dbt command failed: dbt {' '.join(args)}\n{result.exception}")

    print(f"Completed successfully: dbt {' '.join(args)}")
