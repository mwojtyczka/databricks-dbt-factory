"""Guard live acceptance against unsafe output paths and misleading green statuses."""

import json
import hashlib
from types import SimpleNamespace

import pytest

from tests.databricks.lakeflow_acceptance import (
    check_stage,
    prepare_acceptance_bundle,
    completed_repair,
    executing_query,
)


def test_generated_acceptance_bootstrap_is_serverless(tmp_path, bundle_validator):
    wheel = tmp_path / "factory.whl"
    wheel.write_bytes(b"wheel")
    directory = tmp_path / "bundle"
    prepare_acceptance_bundle(
        directory, "test_jg_catalog", "factory_acceptance_scratch", "artifacts", "warehouse", wheel, "scratch"
    )
    config = json.loads((directory / "databricks.yml").read_text())
    assert not list(bundle_validator.iter_errors(config))
    assert config["targets"] == {"scratch": {"default": True}}
    job = config["resources"]["jobs"]["bootstrap"]
    assert not job.get("job_clusters")
    assert all(not any("cluster" in key for key in task) for task in job["tasks"])
    assert job["tasks"][0]["notebook_task"]["base_parameters"]["workspace_root"] == "${workspace.file_path}"
    assert (directory / "prepare_dbt_reuse.py").is_file()
    with pytest.raises(FileExistsError):
        prepare_acceptance_bundle(directory, "catalog", "schema", "artifacts", "warehouse", wheel, "scratch")


def test_acceptance_rejects_invalid_schema_before_creating_bundle(tmp_path):
    with pytest.raises(ValueError, match="identifier"):
        prepare_acceptance_bundle(
            tmp_path / "output", "catalog", "scratch;drop schema x", "artifacts", "warehouse", "missing.whl", "scratch"
        )
    assert not (tmp_path / "output").exists()


def metrics():
    return [
        {
            "task_key": "orders_model",
            "reason": "REUSED",
            "imported_dbt": False,
            "success": True,
            "invocations": {"run": 0, "test": 0, "parse": 0},
        },
        {
            "task_key": "required_gate_test",
            "reason": None,
            "imported_dbt": True,
            "success": True,
            "invocations": {"run": 0, "test": 1, "parse": 0},
        },
        {
            "task_key": "report_model",
            "reason": "REUSED",
            "imported_dbt": False,
            "success": True,
            "invocations": {"run": 0, "test": 0, "parse": 0},
        },
    ]


def test_green_model_tasks_cannot_hide_dbt_initialization_or_missing_test_rerun():
    report = metrics()
    check_stage(report, orders_reused=True, expected_models=0)
    report[0]["imported_dbt"] = True
    with pytest.raises(AssertionError, match="import"):
        check_stage(report, orders_reused=True, expected_models=0)
    report = metrics()
    report[1]["invocations"]["test"] = 0
    with pytest.raises(AssertionError, match="test"):
        check_stage(report, orders_reused=True, expected_models=0)


def test_repair_must_wait_for_the_requested_attempt():
    old = {"state": {"life_cycle_state": "TERMINATED"}, "repair_history": []}
    assert not completed_repair(old, 12)
    old["repair_history"] = [{"id": 12, "end_time": 0}]
    assert not completed_repair(old, 12)
    old["repair_history"][0]["end_time"] = 123
    assert completed_repair(old, 12)


def test_unchanged_requires_metrics_for_every_executed_model():
    with pytest.raises(AssertionError, match="missing"):
        check_stage(metrics()[:-1], orders_reused=True, expected_models=0)


def test_interruption_requires_the_exact_running_warehouse_query():
    statement = "CREATE OR REPLACE TABLE scratch.orders AS SELECT * FROM range(100)"
    marker = {"sha256": hashlib.sha256(statement.encode()).hexdigest()}
    query = SimpleNamespace(query_text=statement, status=SimpleNamespace(value="RUNNING"), query_id="owned")
    assert executing_query([query], marker) == "owned"
    query.status.value = "FINISHED"
    assert executing_query([query], marker) is None
    query.status.value = "RUNNING"
    query.query_text += " -- another query"
    assert executing_query([query], marker) is None
