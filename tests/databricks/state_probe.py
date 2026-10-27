# Databricks notebook source
"""Exercise the shipped state store against harness-owned Delta tables and immutable artifacts."""
# pylint: skip-file
# mypy: ignore-errors

import io
import json
import platform
import tempfile
import time
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils, spark

from databricks_dbt_factory.__version__ import __version__
from databricks_dbt_factory.reuse import Relation, ReuseConfig, describe_models
from databricks_dbt_factory.reuse_prepare import capture_parse, read_provenance
from databricks_dbt_factory.reuse_runtime import (
    Attempt,
    BuildEvidence,
    EvidenceUnavailable,
    claim_owner,
    initialize_state,
    prepare_bundle,
    prepare_lineage,
    publish_artifacts,
    quote_relation,
    read_build,
    record_build,
    record_decision,
    snapshot_relation,
    write_bundle,
)

catalog, schema, volume, job_id, run_id = (
    dbutils.widgets.get(key) for key in ("catalog", "schema", "volume", "job_id", "run_id")
)
assert dbutils.widgets.get("mode") == "serverless"
w = WorkspaceClient()
config_value = {
    "format_version": 1,
    "project_id": "factory_state_probe",
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
    "checks": {},
    "scope": "storage only; Spark SQL builds fixture tables and real dbt parse supplies mappings",
}


def relation(name):
    return Relation(catalog, schema, name)


def sql_table(name):
    return quote_relation(relation(name))


def expect_unavailable(function):
    try:
        function()
    except EvidenceUnavailable as error:
        return str(error)
    raise AssertionError("expected unavailable evidence")


def run_lineage_checks(context, model):
    # Match the native lower() filter without merging distinct Unicode names.
    for name in ("straße", "STRASSE"):
        spark.sql(f"CREATE VIEW {sql_table(name)} AS SELECT 1 AS id").collect()
    assert w.tables.get(f"{catalog}.{schema}.straße").table_id != w.tables.get(f"{catalog}.{schema}.STRASSE").table_id
    # Historical metadata avoids relying on asynchronous ingestion of fresh CTAS events.
    # Only scratch state is written; the observed relations are never claimed or modified.
    sample = spark.sql(
        "SELECT target_table_catalog AS catalog, target_table_schema AS schema, target_table_name AS identifier "
        "FROM system.access.table_lineage WHERE event_date >= date_sub(current_date(), 365) "
        "AND target_table_catalog = :catalog AND target_table_schema <> :schema "
        "AND source_table_catalog IS NOT NULL AND source_table_schema IS NOT NULL "
        "AND source_table_name IS NOT NULL AND target_table_name IS NOT NULL LIMIT 1",
        args={"catalog": catalog, "schema": schema},
    ).first()
    assert sample is not None, "live lineage check requires an existing table-to-table observation in the catalog"
    model = replace(model, output=Relation.from_dict(sample.asDict(), "lineage target"), inputs=(), views=())
    observed = prepare_lineage(spark, config, context, model, job_id=job_id)
    assert observed["reason"] == "UNDECLARED_INPUT" and observed["blockers"], observed

    def without_events(statement, args=None):
        if "`system`.`access`.`table_lineage`" in statement:
            return spark.sql("SELECT 1 WHERE false")
        return spark.sql(statement, args=args)

    retained = prepare_lineage(SimpleNamespace(sql=without_events), config, context, model, job_id=job_id)
    assert retained == observed, "retained blockers must survive an empty system-table result"
    assert all(value["path"] is None for value in retained["blockers"]), "path input cannot be declared in v1"
    reviewed = replace(
        model,
        inputs=tuple(
            Relation(value["catalog"], value["schema"], value["identifier"]) for value in retained["blockers"]
        ),
    )
    assert prepare_lineage(spark, config, context, reviewed, job_id=job_id)["reason"] is None
    assert prepare_lineage(spark, config, context, model, job_id=job_id)["reason"] == "UNDECLARED_INPUT"
    report["checks"]["lineage"] = {
        "scope": "existing UC observations, real scratch Delta persistence; empty event boundary controlled",
        "target": asdict(model.output),
        "blockers": observed["blockers"],
        "explicit_declaration_satisfies_blocker": True,
        "removing_declaration_blocks_again": True,
    }


def run_checks():
    initialize_state(spark, config)
    initialize_state(spark, config)
    spark.sql(f"CREATE TABLE {sql_table('input_table')} USING DELTA AS SELECT 1 AS id").collect()
    spark.sql(f"CREATE TABLE {sql_table('orders')} USING DELTA AS SELECT * FROM {sql_table('input_table')}").collect()
    source = snapshot_relation(spark, w, relation("input_table"))
    output = snapshot_relation(spark, w, relation("orders"))
    identity = {
        "project_id": config.project_id,
        "environment": config.environment,
        "workspace_id": str(w.get_workspace_id()),
        "metastore_id": w.metastores.summary().metastore_id,
        "principal": w.current_user.me().user_name,
    }
    assert spark.sql("SELECT current_user()").first()[0] == identity["principal"]
    with tempfile.TemporaryDirectory(prefix="factory-state-probe-") as directory:
        root = Path(directory)
        project, profiles = root / "project", root / "profiles"
        (project / "models").mkdir(parents=True)
        profiles.mkdir()
        (project / "dbt_project.yml").write_text(
            "name: factory_state_probe\nversion: '1.0'\nconfig-version: 2\nprofile: probe\n"
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
                        {"name": "raw", "database": catalog, "schema": schema, "tables": [{"name": "input_table"}]}
                    ],
                }
            )
        )
        (profiles / "profiles.yml").write_text(
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
                                "http_path": "/sql/1.0/warehouses/unused",
                                "token": "unused-parse-only",
                                "threads": 1,
                            }
                        },
                    }
                }
            )
        )
        manifest, provenance = capture_parse(
            project, profiles, identity=identity, target="probe", deployment_ref=run_id
        )
        context = read_provenance(provenance, manifest).context
        local = write_bundle(prepare_bundle(manifest, provenance, config_value), root / "artifacts")
        refs = publish_artifacts(w, local, f"/Volumes/{catalog}/{schema}/{volume}")
    descriptor = next(ref for name, ref in refs.items() if name.startswith("descriptor_"))
    build = BuildEvidence(
        "model.factory_state_probe.orders",
        Attempt(job_id, run_id, "serverless", 0),
        datetime.now(timezone.utc).isoformat(),
        descriptor,
        context.digest,
        (source,),
        (source,),
        output,
    )
    claim_owner(spark, config, context, output.relation, job_id=job_id)
    for _ in range(2):
        assert record_build(spark, w, config, context, build, successful=True) == "RECORDED"
    baseline = read_build(spark, w, config, context, build.model_id, output, job_id=job_id)
    assert baseline["build"]["completed_at"] == build.completed_at
    assert baseline["build"]["inputs_before"] == [asdict(source)]
    assert spark.sql(f"SELECT count(*) FROM {sql_table('factory_reuse_builds')}").first()[0] == 1
    report["checks"]["idempotent_build"] = {"key": baseline["key"], "sha256": baseline["sha256"]}
    try:
        record_build(
            spark, w, config, context, replace(build, completed_at="2030-01-01T00:00:00+00:00"), successful=True
        )
    except ValueError:
        report["checks"]["conflicting_attempt"] = True
    else:
        raise AssertionError("conflicting attempt was overwritten")
    other = replace(config, owner_key="other")
    try:
        claim_owner(spark, other, context, output.relation, job_id=job_id)
    except ValueError:
        report["checks"]["conflicting_owner"] = True
    else:
        raise AssertionError("conflicting owner was accepted")
    observed_at = datetime.now(timezone.utc).isoformat()
    for _ in range(2):
        assert (
            record_decision(
                spark,
                config,
                context,
                build.model_id,
                build.attempt,
                reason="STORAGE_PROBE",
                observed_at=observed_at,
                evidence={"quoted": "a'b"},
                durations={},
            )
            == "RECORDED"
        )
    assert spark.sql(f"SELECT count(*) FROM {sql_table('factory_reuse_decisions')}").first()[0] == 1
    assert (
        read_build(spark, w, config, context, build.model_id, output, job_id=job_id)["build"]["completed_at"]
        == build.completed_at
    )
    report["checks"]["decision_separate_from_build"] = True
    spark.sql(f"INSERT INTO {sql_table('input_table')} VALUES (2)").collect()
    changed = snapshot_relation(spark, w, source.relation)
    assert changed.version > source.version and changed.uc_id == source.uc_id
    assert (
        record_build(spark, w, config, context, replace(build, inputs_after=(changed,)), successful=True)
        == "INPUT_CHANGED_DURING_BUILD"
    )
    assert record_build(spark, w, config, context, build, successful=False) == "BUILD_FAILED"
    report["checks"]["unstable_inputs_withheld"] = True
    spark.sql(f"INSERT INTO {sql_table('orders')} VALUES (2)").collect()
    changed_output = snapshot_relation(spark, w, output.relation)
    assert read_build(spark, w, config, context, build.model_id, changed_output, job_id=job_id) is None
    report["checks"]["output_mutation_invalidates"] = True
    spark.sql(f"DROP TABLE {sql_table('orders')}").collect()
    spark.sql(f"CREATE TABLE {sql_table('orders')} USING DELTA AS SELECT 1 AS id").collect()
    recreated = snapshot_relation(spark, w, output.relation)
    assert (
        recreated.version == output.version
        and recreated.uc_id != output.uc_id
        and recreated.delta_id != output.delta_id
    )
    assert read_build(spark, w, config, context, build.model_id, recreated, job_id=job_id) is None
    report["checks"]["output_recreation_invalidates"] = True
    spark.sql(f"ALTER TABLE {sql_table('orders')} RENAME TO {sql_table('renamed_orders')}").collect()
    report["checks"]["renamed_output"] = expect_unavailable(lambda: snapshot_relation(spark, w, output.relation))
    spark.sql(f"ALTER TABLE {sql_table('renamed_orders')} RENAME TO {sql_table('orders')}").collect()
    spark.sql(f"CREATE VIEW {sql_table('input_view')} AS SELECT * FROM {sql_table('input_table')}").collect()
    report["checks"]["non_delta_input"] = expect_unavailable(
        lambda: snapshot_relation(spark, w, relation("input_view"))
    )
    spark.sql(f"ALTER TABLE {sql_table('factory_reuse_builds')} RENAME TO {sql_table('saved_builds')}").collect()
    try:
        unavailable_build = replace(
            build,
            attempt=replace(build.attempt, attempt_number=1),
            inputs_before=(changed,),
            inputs_after=(changed,),
            output=snapshot_relation(spark, w, output.relation),
        )
        assert record_build(spark, w, config, context, unavailable_build, successful=True) == "STATE_UNAVAILABLE"
        report["checks"]["state_write_outage_after_success"] = True
        report["checks"]["state_read_outage"] = expect_unavailable(
            lambda: read_build(spark, w, config, context, build.model_id, output, job_id=job_id)
        )
    finally:
        spark.sql(f"ALTER TABLE {sql_table('saved_builds')} RENAME TO {sql_table('factory_reuse_builds')}").collect()
    assert spark.sql(f"SELECT count(*) FROM {sql_table('factory_reuse_builds')}").first()[0] == 1
    model = next(model for model in describe_models(json.loads(manifest), context) if model.unique_id == build.model_id)
    run_lineage_checks(context, model)


started = time.monotonic()
try:
    run_checks()
    report["passed"] = True
except Exception as error:
    report.update(passed=False, error=str(error), error_type=type(error).__name__)
report["seconds"] = time.monotonic() - started
payload = json.dumps(report)
w.files.upload(f"/Volumes/{catalog}/{schema}/{volume}/report.json", io.BytesIO(payload.encode()), overwrite=False)
if not report["passed"]:
    raise AssertionError("State probe failed; details in Volume report.json")
dbutils.notebook.exit(payload)
