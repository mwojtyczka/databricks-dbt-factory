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
