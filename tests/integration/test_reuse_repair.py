"""Repairs and evidence outages through the shipped notebook, with native dbt parsing."""

import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from databricks_dbt_factory import reuse_runtime
from tests.integration import test_notebook_reuse as notebook_tests

captured = notebook_tests.captured
local_bundle = notebook_tests.local_bundle
evidence = notebook_tests.evidence
notebook_runtime = notebook_tests.notebook_runtime
matching_preparation = notebook_tests.matching_preparation
model_invocations = notebook_tests.model_invocations
execute_notebook = notebook_tests.execute_notebook


def prepare_again(runtime, deployment, monkeypatch):
    store, client, config, _context, build, _snapshots, values = runtime
    monkeypatch.setenv("DBT_ACCESS_TOKEN", "dummy-parse-only")
    prepared = reuse_runtime.prepare_run(
        store,
        client,
        config,
        build.descriptor,
        *deployment[:2],
        reuse_runtime.Attempt("job", "next", "prepare", 1),
        model_ids=[build.model_id],
    )
    values["factory_reuse_preparation"] = json.dumps(reuse_runtime.publish_preparation(client, config, prepared))


@pytest.mark.parametrize("rerun_preparation", [False, True])
@pytest.mark.parametrize("change", [None, "input", "output"])
def test_repair_rechecks_current_data(
    matching_preparation, captured, model_invocations, monkeypatch, rerun_preparation, change
):
    store, _client, _config, _context, build, snapshots, values = matching_preparation
    monkeypatch.setattr(reuse_runtime, "REUSE_ENABLED", True)
    first = execute_notebook(values, store)
    assert first["model_run"].reason == "REUSED"
    assert not model_invocations
    if change:
        previous = build.inputs_before[0] if change == "input" else build.output
        snapshots[previous.relation] = replace(previous, version=previous.version + 1)
    values["factory_attempt_number"] = "1"
    if rerun_preparation:
        prepare_again(matching_preparation, captured, monkeypatch)
    repaired = execute_notebook(values, store)["model_run"]
    if change:
        assert repaired.reason != "REUSED"
        assert repaired.publication == "RECORDED"
        assert len(model_invocations) == 1
    else:
        assert repaired.reason == "REUSED"
        assert repaired.publication is None
        assert not model_invocations


@pytest.mark.parametrize("rerun_preparation", [False, True])
def test_repair_binds_new_successful_baseline(
    matching_preparation, captured, model_invocations, monkeypatch, rerun_preparation
):
    store, client, config, context, build, _snapshots, values = matching_preparation
    monkeypatch.setattr(reuse_runtime, "REUSE_ENABLED", True)
    values["factory_force_rebuild"] = "true"
    assert execute_notebook(values, store)["model_run"].publication == "RECORDED"
    successful = reuse_runtime.read_build(store, client, config, context, build.model_id, build.output, job_id="job")
    values.update(factory_force_rebuild="false", factory_attempt_number="1")
    if rerun_preparation:
        prepare_again(matching_preparation, captured, monkeypatch)
    repaired = execute_notebook(values, store)["model_run"]
    assert repaired.reason == ("REUSED" if rerun_preparation else "PREPARATION_STALE")
    assert len(model_invocations) == (1 if rerun_preparation else 2)
    if rerun_preparation:
        assert (
            reuse_runtime.read_build(store, client, config, context, build.model_id, build.output, job_id="job")
            == successful
        )


@pytest.fixture
def normal_invocations(monkeypatch):
    """Replace warehouse execution only; native ls parses and resolves the real command."""
    from dbt.cli.main import dbtRunner  # pylint: disable=import-outside-toplevel

    original, calls = dbtRunner.invoke, []

    def invoke(runner, args):
        if args[0] != "run":
            return original(runner, args)
        calls.append(args)
        selected = original(
            runner, ["ls", *args[1:], "--resource-type", "model", "--output", "json", "--output-keys", "unique_id"]
        )
        assert selected.success, selected.exception
        assert [json.loads(item)["unique_id"] for item in selected.result] == ["model.probe.orders"]
        (Path(os.environ["DBT_TARGET_PATH"]) / "run_results.json").write_text(
            json.dumps({"results": [{"unique_id": "model.probe.orders", "status": "success"}]})
        )
        return SimpleNamespace(success=True)

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    return calls


@pytest.mark.parametrize("outage", ["state-read", "artifact-read", "build-write", "decision-write"])
def test_evidence_outage_preserves_model_execution(
    matching_preparation, normal_invocations, monkeypatch, outage, capsys
):
    store, client, config, context, build, _snapshots, values = matching_preparation
    monkeypatch.setattr(reuse_runtime, "REUSE_ENABLED", True)
    values["factory_force_rebuild"] = "true"
    original_sql = store.sql
    saved_artifact = None
    if outage == "artifact-read":
        saved_artifact = client.files.objects.pop(build.descriptor["path"])
    elif outage == "state-read":
        store.unavailable = True
    else:
        table = "factory_reuse_builds" if outage == "build-write" else "factory_reuse_decisions"

        def sql(statement, args=None):
            if statement.startswith("MERGE") and table in statement:
                raise OSError("write unavailable")
            return original_sql(statement, args=args)

        monkeypatch.setattr(store, "sql", sql)
    result = execute_notebook(values, store)
    assert len(normal_invocations) == 1
    run = result["model_run"]
    if outage == "decision-write":
        assert run.publication == "RECORDED"
        assert "decision publication: STATE_UNAVAILABLE" in capsys.readouterr().out
    else:
        assert run.reason == ("ARTIFACT_UNAVAILABLE" if outage == "artifact-read" else "STATE_UNAVAILABLE")
        assert run.publication in {None, "STATE_UNAVAILABLE"}
    store.unavailable = False
    monkeypatch.setattr(store, "sql", original_sql)
    if saved_artifact:
        client.files.objects[build.descriptor["path"]] = saved_artifact
    baseline = reuse_runtime.read_build(store, client, config, context, build.model_id, build.output, job_id="job")
    assert baseline["build"]["attempt"]["run_id"] == ("next" if outage == "decision-write" else "run")
    assert "DBT_ACCESS_TOKEN" not in os.environ
    assert "DBT_HOST" not in os.environ
    assert not Path(result["local_dir"]).exists()


def test_profile_edit_after_preparation_is_checked_before_import(matching_preparation, monkeypatch):
    import builtins  # pylint: disable=import-outside-toplevel

    store, _client, _config, _context, _build, _snapshots, values = matching_preparation
    monkeypatch.setattr(reuse_runtime, "REUSE_ENABLED", True)
    path = Path(values["profiles_directory"]) / "profiles.yml"
    original_profile, original_import = path.read_text(), builtins.__import__

    def no_dbt(name, *args, **kwargs):
        if name == "dbt" or name.startswith("dbt."):
            raise AssertionError("profile validation imported dbt")
        return original_import(name, *args, **kwargs)

    path.write_text(original_profile.replace("/warehouses/x", "/warehouses/changed"))
    monkeypatch.setattr(builtins, "__import__", no_dbt)
    try:
        with pytest.raises(ValueError, match="profiles digest mismatch"):
            execute_notebook(values, store)
    finally:
        path.write_text(original_profile)
    assert "DBT_ACCESS_TOKEN" not in os.environ


def test_source_profile_edit_after_verification_cannot_change_invocation(
    matching_preparation, model_invocations, monkeypatch
):
    import builtins  # pylint: disable=import-outside-toplevel

    store, _client, _config, _context, _build, _snapshots, values = matching_preparation
    values["factory_force_rebuild"] = "true"
    path = Path(values["profiles_directory"]) / "profiles.yml"
    original_profile, original_import = path.read_text(), builtins.__import__

    def change_on_import(name, *args, **kwargs):
        if name == "dbt.cli.main":
            path.write_text(original_profile.replace("schema: default", "schema: changed"))
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", change_on_import)
    try:
        result = execute_notebook(values, store)
        assert result["model_run"].publication == "RECORDED"
        assert len(model_invocations) == 1
        args = model_invocations[0]
        assert args[args.index("--profiles-dir") + 1] != values["profiles_directory"]
        assert not Path(args[args.index("--profiles-dir") + 1]).exists()
    finally:
        path.write_text(original_profile)


@pytest.mark.parametrize("change", ["sdk-host", "runtime-version", "secret-version", "profile-read"])
def test_unbound_runtime_context_runs_without_publication(
    matching_preparation, normal_invocations, monkeypatch, change
):
    store, client, _config, _context, _build, _snapshots, values = matching_preparation
    monkeypatch.setattr(reuse_runtime, "REUSE_ENABLED", True)
    if change == "sdk-host":
        client.config.host = "https://different.databricks.com"
    elif change == "secret-version":
        values["factory_secret_versions"] = '{"routing-secret": "changed"}'
    elif change == "runtime-version":
        original = reuse_runtime.runtime_versions
        monkeypatch.setattr(reuse_runtime, "runtime_versions", lambda: original() | {"dbt-core": "changed"})
    else:

        def unavailable(*_args):
            raise PermissionError("profile evidence denied")

        monkeypatch.setattr(reuse_runtime, "verify_profiles", unavailable)
    result = execute_notebook(values, store)["model_run"]
    assert result.reason == ("PROFILE_UNAVAILABLE" if change == "profile-read" else "CONTEXT_CHANGED")
    assert result.publication is None
    assert len(normal_invocations) == 1
