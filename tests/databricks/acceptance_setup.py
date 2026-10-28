# Databricks notebook source
"""Capture a real serverless deployment and emit its generated Lakeflow bundle."""
# pylint: skip-file
# mypy: ignore-errors

import io
import json
import os
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import yaml


def reuse_config(catalog, schema, volume, variant):
    return {
        "format_version": 1,
        "project_id": "factory_acceptance_split" if variant == "split" else "factory_acceptance",
        "environment": "scratch",
        "owner_key": "acceptance",
        "state_catalog": catalog,
        "state_schema": schema,
        "artifact_volume": {"catalog": catalog, "schema": schema, "volume": volume},
    }


def write_project(root, host, catalog, schema, warehouse, variant, rows=1000000, volume="artifacts"):
    if variant not in {
        "base",
        "unrelated",
        "macro",
        "vars",
        "split",
        "expensive",
        "native",
        "environment",
        "engine_previous",
        "partial",
    }:
        raise ValueError("unsupported acceptance variant")
    if not isinstance(rows, int) or rows <= 0:
        raise ValueError("rows must be a positive integer")
    root = Path(root)
    project, profiles = root / "project", root / "profiles"
    for directory in (project / "models", project / "macros", project / "tests", profiles):
        directory.mkdir(parents=True)
    (project / "dbt_project.yml").write_text(
        "name: factory_acceptance\nversion: '1.0'\nconfig-version: 2\nprofile: probe\n"
    )
    suffix = "_split" if variant == "split" else "_native" if variant == "native" else ""
    config = "{{ config(materialized='table', tags=['%s'], alias='%s', meta={'dbt_factory': {'reuse': {'enabled': true}}}) }}\n"
    sql = "select id * {{ outer() }} * {{ var('factor', 1) }} as id from {{ source('raw', 'input_table') }}"
    if variant == "expensive":
        sql = (
            "select i.id * {{ outer() }} * {{ var('factor', 1) }} as id, x.work "
            "from {{ source('raw', 'input_table') }} i cross join "
            f"(select sum(cast(conv(substr(sha2(cast(id as string), 256), 1, 7), 16, 10) as bigint)) as work from range({rows})) x"
        )
    (project / "models" / "orders.sql").write_text(config % ("bronze", "orders" + suffix) + sql)
    (project / "models" / "report.sql").write_text(
        config % ("silver", "report" + suffix)
        + "select o.* from {{ ref('orders') }} o cross join {{ source('raw', 'test_control') }} c"
        + (" where o.id >= 0" if variant == "partial" else "")
    )
    (project / "macros" / "inner.sql").write_text(
        "{% macro inner() %}" + ("2" if variant in {"macro", "vars", "environment"} else "1") + "{% endmacro %}"
    )
    (project / "macros" / "outer.sql").write_text(
        "{% macro outer() %}{{ inner() }} * {{ env_var('FACTORY_ACCEPTANCE_CONTEXT', '1') }}{% endmacro %}"
    )
    if variant == "unrelated":
        (project / "macros" / "unused.sql").write_text("{% macro unused() %}99{% endmacro %}")
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
    (profiles / "profiles.yml").write_text(
        json.dumps(
            {
                "probe": {
                    "target": "probe",
                    "outputs": {
                        "probe": {
                            "type": "databricks",
                            "host": host,
                            "http_path": f"/sql/1.0/warehouses/{warehouse}",
                            "catalog": catalog,
                            "schema": schema,
                            "threads": 1,
                            "token": "{{ env_var('DBT_ACCESS_TOKEN') }}",
                        }
                    },
                }
            }
        )
    )
    return reuse_config(catalog, schema, volume, variant), {"factor": 2} if variant in {"vars", "environment"} else {}


def render_config(root, manifest, context, config, descriptor, workspace_root, wheel_path, variant, warehouse=None):
    from databricks_dbt_factory.dbt_factory import DbtFactory
    from databricks_dbt_factory.dbt_task import DbtTaskOptions, TaskType
    from databricks_dbt_factory.job_spec import render_job_spec, render_partitioned_job_spec
    from databricks_dbt_factory.reuse import ReuseDeployment
    from databricks_dbt_factory.task_factory import DbtDependencyResolver, ModelTaskFactory, TestTaskFactory

    if not workspace_root.startswith("/Workspace/") or ".." in Path(workspace_root).parts:
        raise ValueError("workspace_root must be an absolute /Workspace project files path")
    if variant == "native":
        options = DbtTaskOptions(
            task_type=TaskType.DBT,
            project_directory=workspace_root + "/project",
            warehouse_id=warehouse or "warehouse",
            catalog=config["state_catalog"],
            schema=config["state_schema"],
            source="WORKSPACE",
        )
        deployment = None
    else:
        options = DbtTaskOptions(
            notebook_path="./acceptance_runner.py",
            project_directory=workspace_root + "/project",
            profiles_directory=workspace_root + "/profiles",
        )
        deployment = ReuseDeployment(config, context, descriptor, "./prepare_dbt_reuse.py")
    resolver = DbtDependencyResolver()
    dbt_options = "" if variant == "native" else "--target probe"
    factory = DbtFactory(
        {
            "model": ModelTaskFactory(resolver, options, dbt_options),
            "test": TestTaskFactory(resolver, options, dbt_options),
        },
        reuse=deployment,
    )
    template = Path(root) / "template.yml"
    template.write_text(
        yaml.safe_dump(
            {
                "bundle": {"name": config["state_schema"]},
                "targets": {"probe": {"default": True}},
                "resources": {
                    "jobs": {
                        "acceptance": {
                            "name": config["state_schema"],
                            "max_concurrent_runs": 1,
                            "timeout_seconds": 2400,
                            "tags": {"aidevkit_project": "dbt-factory", "purpose": "reuse-acceptance"},
                            "environments": [
                                {
                                    "environment_key": "Default",
                                    "spec": {
                                        "environment_version": "4",
                                        "dependencies": [
                                            "dbt-core==1.12.4" if variant == "engine_previous" else "dbt-core==1.12.5",
                                            "dbt-databricks==1.12.7",
                                            wheel_path,
                                        ],
                                    },
                                }
                            ],
                        }
                    }
                },
            }
        )
    )
    rendered = (
        render_partitioned_job_spec(str(template), factory.create_partitions(manifest, ["bronze", "silver"]))
        if variant == "split"
        else render_job_spec(str(template), factory.create_tasks(manifest))
    )
    result = yaml.safe_load(rendered)
    if variant not in {"split", "native"}:
        baseline = DbtFactory(
            {
                "model": ModelTaskFactory(resolver, options, "--target probe"),
                "test": TestTaskFactory(resolver, options, "--target probe"),
            }
        )
        baseline_tasks = baseline.create_tasks(manifest)
        baseline._apply_reuse_variables(baseline_tasks, json.loads(context.canonical_json)["vars"])
        baseline_rendered = yaml.safe_load(render_job_spec(str(template), baseline_tasks, "baseline"))
        result["resources"]["jobs"]["baseline"] = baseline_rendered["resources"]["jobs"]["baseline"]
    return result


def main(dbutils, spark):
    from databricks.sdk import WorkspaceClient
    from databricks_dbt_factory import reuse_runtime as runtime
    from databricks_dbt_factory.reuse import Relation, ReuseConfig
    from databricks_dbt_factory.reuse_prepare import capture_parse, read_provenance

    for key, default in {
        "catalog": "",
        "schema": "",
        "volume": "",
        "warehouse_id": "",
        "mode": "serverless",
        "job_id": "",
        "run_id": "",
        "action": "setup",
        "variant": "base",
        "workspace_root": "",
        "wheel_path": "",
        "rows": "1000000",
    }.items():
        dbutils.widgets.text(key, default)
    value = {
        key: dbutils.widgets.get(key)
        for key in (
            "catalog",
            "schema",
            "volume",
            "warehouse_id",
            "mode",
            "job_id",
            "run_id",
            "action",
            "variant",
            "workspace_root",
            "wheel_path",
            "rows",
        )
    }
    if value["mode"] != "serverless":
        raise ValueError("acceptance requires serverless")
    catalog, schema, volume = (value[key] for key in ("catalog", "schema", "volume"))
    config = reuse_config(catalog, schema, volume, value["variant"])
    ws = WorkspaceClient()
    if value["action"] == "setup":
        runtime.initialize_state(spark, ReuseConfig.from_dict(config))
        for name, select in [("input_table", "1 AS id"), ("test_control", "false AS fail")]:
            table = runtime.quote_relation(Relation(catalog, schema, name))
            spark.sql(f"CREATE TABLE IF NOT EXISTS {table} USING DELTA AS SELECT {select}").collect()
        dbutils.notebook.exit(json.dumps({"action": "setup", "config": config}))
        return
    if value["action"] != "capture":
        raise ValueError("action must be setup or capture")
    with tempfile.TemporaryDirectory(prefix="acceptance-capture-") as directory:
        root = Path(directory)
        config, variables = write_project(
            root,
            urlparse(ws.config.host).netloc,
            catalog,
            schema,
            value["warehouse_id"],
            value["variant"],
            int(value["rows"]),
            volume,
        )
        identity = {
            "project_id": config["project_id"],
            "environment": config["environment"],
            "workspace_id": str(ws.get_workspace_id()),
            "metastore_id": ws.metastores.summary().metastore_id,
            "principal": ws.current_user.me().user_name,
        }
        os.environ["DBT_ACCESS_TOKEN"] = ws.config.authenticate()["Authorization"].removeprefix("Bearer ").strip()
        previous_environment = os.environ.get("FACTORY_ACCEPTANCE_CONTEXT")
        os.environ["FACTORY_ACCEPTANCE_CONTEXT"] = "2" if value["variant"] == "environment" else "1"
        try:
            manifest, provenance = capture_parse(
                root / "project",
                root / "profiles",
                identity=identity,
                target="probe",
                deployment_ref=value["run_id"] + "-" + value["variant"],
                variables=variables,
                environment_inputs=["FACTORY_ACCEPTANCE_CONTEXT"],
            )
        finally:
            os.environ.pop("DBT_ACCESS_TOKEN", None)
            if previous_environment is None:
                os.environ.pop("FACTORY_ACCEPTANCE_CONTEXT", None)
            else:
                os.environ["FACTORY_ACCEPTANCE_CONTEXT"] = previous_environment
        bundle = runtime.write_bundle(runtime.prepare_bundle(manifest, provenance, config), root / "artifacts")
        volume_root = f"/Volumes/{catalog}/{schema}/{volume}"
        references = runtime.publish_artifacts(ws, bundle, volume_root)
        descriptor = next(ref for name, ref in references.items() if name.startswith("descriptor_"))
        parsed = json.loads(manifest)
        generated = render_config(
            root,
            parsed,
            read_provenance(provenance, manifest).context,
            config,
            descriptor,
            value["workspace_root"],
            value["wheel_path"],
            value["variant"],
            value["warehouse_id"],
        )
        (root / "manifest.json").write_bytes(manifest)
        (root / "provenance.json").write_bytes(provenance)
        (root / "reuse.json").write_text(json.dumps(config))
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(root.rglob("*")):
                if path.is_file() and path.relative_to(root).parts[0] in {
                    "project",
                    "profiles",
                    "manifest.json",
                    "provenance.json",
                    "reuse.json",
                }:
                    archive.write(path, path.relative_to(root).as_posix())
        generated_path, project_zip_path = volume_root + "/generated.json", volume_root + "/project.zip"
        ws.files.upload(generated_path, io.BytesIO(json.dumps(generated).encode()), overwrite=True)
        ws.files.upload(project_zip_path, io.BytesIO(payload.getvalue()), overwrite=True)
        dbutils.notebook.exit(
            json.dumps(
                {
                    "action": "capture",
                    "variant": value["variant"],
                    "descriptor": descriptor,
                    "config": config,
                    "model_ids": sorted(
                        uid for uid, node in parsed["nodes"].items() if node["resource_type"] == "model"
                    ),
                    "generated_path": generated_path,
                    "project_zip_path": project_zip_path,
                    "task_keys": {
                        key: [task["task_key"] for task in job["tasks"]]
                        for key, job in generated["resources"]["jobs"].items()
                    },
                }
            )
        )


if __name__ == "__main__":
    from databricks.sdk.runtime import dbutils, spark

    main(dbutils, spark)
