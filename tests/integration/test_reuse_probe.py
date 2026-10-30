"""Keep the live metadata probe scoped to fresh scratch resources."""

import json

import pytest

from tests.databricks.reuse_acceptance import prepare_bundle


def test_probe_bundle_is_valid_and_uses_only_serverless(tmp_path, bundle_validator):
    directory = tmp_path / "probe"
    prepare_bundle(
        directory,
        catalog="test_jg_catalog",
        schema="factory_reuse_probe_123456789abc",
        volume="artifacts",
        warehouse_id="warehouse",
    )
    config = json.loads((directory / "databricks.yml").read_text())
    errors = list(bundle_validator.iter_errors(config))
    assert not errors, [error.message for error in errors]
    job = config["resources"]["jobs"]["probe"]
    assert job["max_concurrent_runs"] == 1
    assert [task["task_key"] for task in job["tasks"]] == ["serverless"]
    for task in job["tasks"]:
        parameters = task["notebook_task"]["base_parameters"]
        assert parameters["catalog"] == "test_jg_catalog"
        assert parameters["schema"] == "factory_reuse_probe_123456789abc"
        assert parameters["mode"] == task["task_key"]
        assert "environment_key" in task
        assert "new_cluster" not in task
        assert "existing_cluster_id" not in task
        assert "job_cluster_key" not in task
    assert not job.get("job_clusters")
    assert (directory / "metadata_probe.py").read_text().startswith("# Databricks notebook source")


def test_probe_refuses_to_overwrite_an_existing_bundle_directory(tmp_path):
    with pytest.raises(FileExistsError):
        prepare_bundle(tmp_path, "catalog", "scratch", "artifacts", "warehouse")


def test_state_probe_uses_matching_wheel_and_serverless_only(tmp_path, bundle_validator):
    wheel = tmp_path / "databricks_dbt_factory-0.4.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel fixture")
    directory = tmp_path / "state-probe"
    prepare_bundle(
        directory, "test_jg_catalog", "scratch", "artifacts", "warehouse", scenario="state", factory_wheel=wheel
    )
    config = json.loads((directory / "databricks.yml").read_text())
    assert not list(bundle_validator.iter_errors(config))
    job = config["resources"]["jobs"]["probe"]
    task = job["tasks"][0]
    assert task["notebook_task"]["notebook_path"] == "./state_probe.py"
    assert task["notebook_task"]["base_parameters"]["job_id"] == "{{job.id}}"
    assert task["notebook_task"]["base_parameters"]["run_id"] == "{{job.run_id}}"
    assert (
        job["environments"][0]["spec"]["dependencies"][-1] == f"/Volumes/test_jg_catalog/scratch/artifacts/{wheel.name}"
    )
    assert "job_clusters" not in job
    assert not any("cluster" in key for key in task)
    assert (directory / "state_probe.py").read_text().startswith("# Databricks notebook source")


def test_state_probe_requires_a_built_wheel_before_creating_output(tmp_path):
    with pytest.raises(ValueError, match="wheel"):
        prepare_bundle(tmp_path / "output", "catalog", "scratch", "artifacts", "warehouse", scenario="state")
    assert not (tmp_path / "output").exists()


def test_runtime_probe_requires_matching_wheel_and_serverless(tmp_path, bundle_validator):
    wheel = tmp_path / "databricks_dbt_factory-0.4.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel fixture")
    directory = tmp_path / "runtime"
    prepare_bundle(
        directory, "test_jg_catalog", "scratch", "artifacts", "warehouse", scenario="runtime", factory_wheel=wheel
    )
    config = json.loads((directory / "databricks.yml").read_text())
    assert not list(bundle_validator.iter_errors(config))
    job = config["resources"]["jobs"]["probe"]
    assert job["tasks"][0]["notebook_task"]["notebook_path"] == "./runtime_probe.py"
    assert job["tasks"][0]["notebook_task"]["base_parameters"]["job_id"] == "{{job.id}}"
    assert job["environments"][0]["spec"]["dependencies"][-1].endswith(wheel.name)
    assert not job.get("job_clusters")
    assert not any("cluster" in key for key in job["tasks"][0])
    with pytest.raises(ValueError, match="wheel"):
        prepare_bundle(tmp_path / "missing", "test_jg_catalog", "scratch", "artifacts", "warehouse", scenario="runtime")
    assert not (tmp_path / "missing").exists()
