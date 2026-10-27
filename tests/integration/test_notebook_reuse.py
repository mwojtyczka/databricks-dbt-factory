"""Run the shipped notebook with real parse artifacts and controlled platform boundaries."""

import builtins
import copy
import json
import os
import runpy
from dataclasses import asdict, replace
from importlib import resources
from types import SimpleNamespace
from pathlib import Path

import databricks.sdk
import pytest

from databricks_dbt_factory import reuse, reuse_runtime
from tests import test_reuse_runtime as state_tests
from tests.test_reuse import CONFIG

captured = state_tests.captured
local_bundle = state_tests.local_bundle
evidence = state_tests.evidence


def data_evidence(build):
    return {
        "complete": True,
        "inputs": [asdict(item) for item in build.inputs_before],
        "output": asdict(build.output),
        "views": [asdict(item) for item in build.views_before],
    }


@pytest.mark.parametrize(
    "change, reason",
    [
        (None, "REUSED"),
        ("no-build", "NO_BASELINE"),
        ("force", "FORCED"),
        ("code", "CODE_CHANGED"),
        ("context", "CONTEXT_CHANGED"),
        ("incomplete", "EVIDENCE_INCOMPLETE"),
        ("missing-output", "EVIDENCE_INCOMPLETE"),
        ("partial", "EVIDENCE_INCOMPLETE"),
        ("boolean-version", "EVIDENCE_INCOMPLETE"),
    ],
)
def test_pure_reuse_requires_complete_matching_evidence(evidence, change, reason):
    build = data_evidence(evidence[4])
    current = copy.deepcopy(build)
    if change == "no-build":
        build = None
    elif change == "incomplete":
        current["complete"] = False
    elif change == "missing-output":
        current["output"] = None
    elif change == "partial":
        del current["inputs"][0]["uc_id"]
    elif change == "boolean-version":
        current["inputs"][0]["version"] = True
    assert reuse.reuse_reason(build, current, change != "code", change != "context", change == "force") == reason


@pytest.mark.parametrize("side", ["inputs", "output"])
@pytest.mark.parametrize(
    "field, value",
    [
        ("uc_id", "new"),
        ("delta_id", "new"),
        ("version", 1),
        ("relation", {"catalog": "other", "schema": "schema", "identifier": "name"}),
    ],
)
def test_each_input_output_identity_and_version_can_invalidate(evidence, side, field, value):
    build = data_evidence(evidence[4])
    current = copy.deepcopy(build)
    (current[side][0] if side == "inputs" else current[side])[field] = value
    assert reuse.reuse_reason(build, current, True, True, False) == (
        "INPUT_CHANGED" if side == "inputs" else "OUTPUT_CHANGED"
    )


@pytest.fixture
def notebook_runtime(evidence, captured, monkeypatch):
    store, client, config, context, build = evidence
    payload = json.loads(context.canonical_json)
    snapshots = {item.relation: item for item in (*build.inputs_before, build.output)}
    sql = store.sql

    def execute(statement, args=None):
        if statement == "SELECT current_user()":
            return SimpleNamespace(first=lambda: (payload["principal"],))
        if statement.startswith("DESCRIBE"):
            item = next(
                value for relation, value in snapshots.items() if reuse_runtime.quote_relation(relation) in statement
            )
            return state_tests.Rows(
                [{"version": item.version} if "HISTORY" in statement else {"format": "delta", "id": item.delta_id}]
            )
        return sql(statement, args=args)

    def table(name):
        item = next(value for relation, value in snapshots.items() if ".".join(asdict(relation).values()) == name)
        return SimpleNamespace(
            table_id=item.uc_id,
            catalog_name=item.relation.catalog,
            schema_name=item.relation.schema,
            name=item.relation.identifier,
        )

    monkeypatch.setattr(store, "sql", execute)
    client.get_workspace_id = lambda: payload["workspace_id"]
    client.metastores = SimpleNamespace(summary=lambda: SimpleNamespace(metastore_id=payload["metastore_id"]))
    client.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name=payload["principal"]))
    client.tables = SimpleNamespace(get=table)
    client.config = SimpleNamespace(
        host="https://example.databricks.com/", authenticate=lambda: {"Authorization": "Bearer private-token"}
    )
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda: client)
    monkeypatch.chdir(captured[0])
    values = {
        "dbt_commands": json.dumps(
            ["dbt run --select fqn:probe.orders,package:probe,file:orders.sql,resource_type:model --target dev"]
        ),
        "project_directory": "",
        "profiles_directory": str(captured[1]),
        "factory_reuse_config": json.dumps(CONFIG),
        "factory_reuse_deployment": json.dumps(build.descriptor),
        "factory_reuse_model": build.model_id,
        "factory_job_id": "job",
        "factory_run_id": "next",
        "factory_task_key": "orders",
        "factory_attempt_number": "0",
        "factory_secret_versions": "{}",
    }
    return store, client, config, context, build, snapshots, values


@pytest.fixture
def matching_preparation(notebook_runtime, captured):
    store, client, config, context, build, _snapshots, values = notebook_runtime
    reuse_runtime.claim_owner(store, config, context, build.output.relation, job_id="job")
    assert reuse_runtime.record_build(store, client, config, context, build, successful=True) == "RECORDED"
    prepared = reuse_runtime.prepare_run(
        store,
        client,
        config,
        build.descriptor,
        captured[0],
        captured[1],
        reuse_runtime.Attempt("job", "next", "prepare", 0),
        model_ids=[build.model_id],
    )
    assert prepared["models"][build.model_id]["reason"] == "CODE_UNCHANGED"
    values["factory_reuse_preparation"] = json.dumps(reuse_runtime.publish_preparation(client, config, prepared))
    return notebook_runtime


def execute_notebook(values, store):
    widgets = SimpleNamespace(text=values.setdefault, get=values.__getitem__)
    notebook = resources.files("databricks_dbt_factory") / "notebook" / "run_dbt_command.py"
    return runpy.run_path(str(notebook), init_globals={"dbutils": SimpleNamespace(widgets=widgets), "spark": store})


def test_matching_preparation_finishes_without_importing_dbt(matching_preparation, monkeypatch, capsys):
    store, client, config, context, build, _snapshots, values = matching_preparation
    monkeypatch.setattr(reuse_runtime, "REUSE_ENABLED", True)
    original_import = builtins.__import__

    def reject_dbt_import(name, *args, **kwargs):
        if name == "dbt" or name.startswith("dbt."):
            raise AssertionError("reuse imported dbt")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_dbt_import)
    result = execute_notebook(values, store)
    assert result["model_run"].reason == "REUSED"
    assert "REUSED" in capsys.readouterr().out
    assert result["local_dir"] is None
    baseline = reuse_runtime.read_build(store, client, config, context, build.model_id, build.output, job_id="job")
    assert baseline["build"]["attempt"]["run_id"] == "run"
    assert "DBT_ACCESS_TOKEN" not in os.environ
    assert "DBT_HOST" not in os.environ


@pytest.fixture
def model_invocations(monkeypatch):
    """Keep real dbt parsing; replace only the warehouse model invocation."""
    from dbt.cli.main import dbtRunner  # pylint: disable=import-outside-toplevel

    original = dbtRunner.invoke
    calls = []

    def invoke(runner, args):
        if args[0] != "run":
            return original(runner, args)

        calls.append(args)
        assert runner.manifest is not None
        selected = original(
            runner,
            [
                "ls",
                *[arg for arg in args[1:] if arg not in {"--full-refresh", "-f"}],
                "--resource-type",
                "model",
                "--output",
                "json",
                "--output-keys",
                "unique_id",
            ],
        )
        assert selected.success, selected.exception
        assert [json.loads(item)["unique_id"] for item in selected.result] == ["model.probe.orders"]
        directory = Path(os.environ["DBT_TARGET_PATH"])
        (directory / "run_results.json").write_text(
            json.dumps({"results": [{"unique_id": "model.probe.orders", "status": "success"}]})
        )
        return SimpleNamespace(success=True)

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    return calls


@pytest.mark.parametrize("control", ["miss", "force", "full-refresh", "disabled"])
def test_model_execution_publishes_fresh_evidence(matching_preparation, model_invocations, control):
    store, client, config, context, build, snapshots, values = matching_preparation
    if control == "miss":
        snapshots[build.inputs_before[0].relation] = replace(build.inputs_before[0], version=1)
    elif control == "force":
        values["factory_force_rebuild"] = "true"
    elif control == "full-refresh":
        values["dbt_commands"] = json.dumps([json.loads(values["dbt_commands"])[0] + " --full-refresh"])
    result = execute_notebook(values, store)
    run = result["model_run"]
    assert (
        run.reason
        == {"miss": "INPUT_CHANGED", "force": "FORCED", "full-refresh": "FORCED", "disabled": "OPTIMIZATION_DISABLED"}[
            control
        ]
    )
    assert run.publication == "RECORDED"
    assert len(model_invocations) == 1
    fresh = reuse_runtime.read_build(store, client, config, context, build.model_id, build.output, job_id="job")
    assert fresh["build"]["attempt"]["run_id"] == "next"
    assert fresh["build"]["inputs_before"][0]["version"] == snapshots[build.inputs_before[0].relation].version
    assert "DBT_ACCESS_TOKEN" not in os.environ


@pytest.mark.parametrize("failure", ["changed-input", "failed", "cancelled", "wrong-model"])
def test_unstable_or_unsuccessful_execution_cannot_publish(
    matching_preparation, monkeypatch, model_invocations, failure
):
    from dbt.cli.main import dbtRunner  # pylint: disable=import-outside-toplevel

    store, client, config, context, build, snapshots, values = matching_preparation
    original = dbtRunner.invoke
    values["factory_force_rebuild"] = "true"

    def invoke(runner, args):
        result = original(runner, args)
        if args[0] == "run":
            if failure == "failed":
                return SimpleNamespace(success=False, exception="warehouse failed")
            if failure == "cancelled":
                raise KeyboardInterrupt("cancelled")
            if failure == "changed-input":
                snapshots[build.inputs_before[0].relation] = replace(build.inputs_before[0], version=1)
            if failure == "wrong-model":
                (Path(os.environ["DBT_TARGET_PATH"]) / "run_results.json").write_text(
                    json.dumps({"results": [{"unique_id": "model.probe.other", "status": "success"}]})
                )
        return result

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    if failure == "changed-input":
        result = execute_notebook(values, store)
        assert result["model_run"].reason == "INPUT_CHANGED_DURING_BUILD"
    else:
        with pytest.raises(
            {"failed": RuntimeError, "cancelled": KeyboardInterrupt, "wrong-model": ValueError}[failure]
        ):
            execute_notebook(values, store)
    baseline = reuse_runtime.read_build(store, client, config, context, build.model_id, build.output, job_id="job")
    assert baseline["build"]["attempt"]["run_id"] == "run"
    assert "DBT_ACCESS_TOKEN" not in os.environ
    assert "DBT_TARGET_PATH" not in os.environ


def test_changed_project_cannot_reuse(matching_preparation, monkeypatch):
    store, _client, _config, _context, _build, _snapshots, values = matching_preparation
    monkeypatch.setattr(reuse_runtime, "REUSE_ENABLED", True)
    path = Path("models/orders.sql")
    original = path.read_text(encoding="utf-8")
    try:
        path.write_text(original + "\n-- changed after preparation\n", encoding="utf-8")
        with pytest.raises(ValueError, match="snapshot digest mismatch"):
            execute_notebook(values, store)
    finally:
        path.write_text(original, encoding="utf-8")
    assert "DBT_ACCESS_TOKEN" not in os.environ


@pytest.mark.parametrize("preparation", ["missing", "stale-run", "stale-baseline", "unavailable"])
def test_unusable_preparation_executes_normally(matching_preparation, model_invocations, monkeypatch, preparation):
    store, client, config, _context, _build, _snapshots, values = matching_preparation
    monkeypatch.setattr(reuse_runtime, "REUSE_ENABLED", True)
    if preparation == "missing":
        values.pop("factory_reuse_preparation")
    elif preparation == "unavailable":
        pointer = json.loads(values["factory_reuse_preparation"])
        client.files.objects.pop(pointer["path"])
    else:
        prepared = json.loads(
            reuse_runtime.download_artifact(client, **json.loads(values["factory_reuse_preparation"]))
        )
        if preparation == "stale-run":
            prepared["attempt"]["run_id"] = "old-run"
        else:
            prepared["models"]["model.probe.orders"]["baseline"]["key"] = "stale"
        values["factory_reuse_preparation"] = json.dumps(reuse_runtime.publish_preparation(client, config, prepared))
    result = execute_notebook(values, store)
    assert len(model_invocations) == 1
    assert result["model_run"].reason == (
        "PREPARATION_UNAVAILABLE" if preparation in {"missing", "unavailable"} else "PREPARATION_STALE"
    )
    assert result["model_run"].publication == "RECORDED"


@pytest.mark.parametrize("command", [" --empty", " --defer", " --select other", "wrong-selector"])
def test_unbound_commands_cannot_skip_or_publish(matching_preparation, monkeypatch, command):
    from dbt.cli.main import dbtRunner  # pylint: disable=import-outside-toplevel

    store, _client, _config, _context, _build, _snapshots, values = matching_preparation
    monkeypatch.setattr(reuse_runtime, "REUSE_ENABLED", True)
    commands = json.loads(values["dbt_commands"])
    commands[0] = (
        commands[0].replace("fqn:probe.orders,package:probe,file:orders.sql,resource_type:model", "other")
        if command == "wrong-selector"
        else commands[0] + command
    )
    values["dbt_commands"] = json.dumps(commands)
    calls = []

    def invoke(_runner, args):
        calls.append(args)
        return SimpleNamespace(success=True)

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    result = execute_notebook(values, store)
    assert len(calls) == 1
    assert result["model_run"].reason == "UNSUPPORTED_COMMAND"
    assert result["model_run"].publication is None


def test_profile_change_cannot_publish_captured_graph(matching_preparation, model_invocations):
    store, _client, _config, _context, _build, _snapshots, values = matching_preparation
    values["factory_force_rebuild"] = "true"
    path = Path(values["profiles_directory"]) / "profiles.yml"
    original = path.read_text(encoding="utf-8")
    try:
        path.write_text(original.replace("schema: default", "schema: other"), encoding="utf-8")
        assert path.read_text(encoding="utf-8") != original
        with pytest.raises(ValueError, match="fresh parse differs"):
            execute_notebook(values, store)
    finally:
        path.write_text(original, encoding="utf-8")
    assert not model_invocations
    assert "DBT_ACCESS_TOKEN" not in os.environ


@pytest.mark.parametrize(
    "field, value, reason",
    [
        ("uc_id", "other", "DEFINITION_CHANGED"),
        ("definition_sha256", "b" * 64, "DEFINITION_CHANGED"),
        ("definition_sha256", "missing", "EVIDENCE_INCOMPLETE"),
        ("definition_sha256", "+" + "a" * 63, "EVIDENCE_INCOMPLETE"),
    ],
)
def test_view_observations_are_required(evidence, field, value, reason):
    build = data_evidence(evidence[4])
    build["views"] = [
        {
            "relation": {"catalog": "raw", "schema": "sales", "identifier": "view"},
            "uc_id": "view-uc",
            "definition_sha256": "a" * 64,
        }
    ]
    current = copy.deepcopy(build)
    current["views"][0][field] = value
    assert reuse.reuse_reason(build, current, True, True, False) == reason


@pytest.mark.parametrize("location", ["reason", "model"])
def test_preparation_cannot_itself_authorize_reuse(matching_preparation, model_invocations, location):
    store, client, config, _context, _build, _snapshots, values = matching_preparation
    prepared = json.loads(reuse_runtime.download_artifact(client, **json.loads(values["factory_reuse_preparation"])))
    if location == "reason":
        prepared["reason"] = "REUSED"
    else:
        prepared["models"]["model.probe.orders"]["reason"] = "REUSED"
    values["factory_reuse_preparation"] = json.dumps(reuse_runtime.publish_preparation(client, config, prepared))
    result = execute_notebook(values, store)
    assert len(model_invocations) == 1
    assert result["model_run"].reason != "REUSED"


@pytest.fixture
def captured_variables(notebook_runtime, captured, tmp_path):
    from databricks_dbt_factory.reuse_prepare import capture_parse  # pylint: disable=import-outside-toplevel

    _store, client, _config, context, _build, _snapshots, values = notebook_runtime
    identity = {
        key: json.loads(context.canonical_json)[key]
        for key in ("project_id", "environment", "workspace_id", "metastore_id", "principal")
    }
    manifest, provenance = capture_parse(
        captured[0],
        captured[1],
        identity=identity,
        target="dev",
        deployment_ref="vars-release",
        variables={"batch": 42},
    )
    paths = reuse_runtime.write_bundle(reuse_runtime.prepare_bundle(manifest, provenance, CONFIG), tmp_path / "bundle")
    references = reuse_runtime.publish_artifacts(client, paths, "/Volumes/state/factory/artifacts")
    values["factory_reuse_deployment"] = json.dumps(
        next(ref for name, ref in references.items() if name.startswith("descriptor_"))
    )
    values["factory_dbt_vars"] = json.dumps({"batch": 42})
    return notebook_runtime


@pytest.mark.parametrize("unavailable", [False, True])
def test_captured_vars_reach_model_invocation(captured_variables, model_invocations, monkeypatch, unavailable):
    store, _client, _config, _context, build, _snapshots, values = captured_variables
    if unavailable:

        def missing_artifacts(*_args):
            raise reuse_runtime.EvidenceUnavailable("ARTIFACT_UNAVAILABLE")

        monkeypatch.setattr(reuse_runtime, "load_deployment", missing_artifacts)
        from dbt.cli.main import dbtRunner  # pylint: disable=import-outside-toplevel

        monkeypatch.setattr(
            dbtRunner, "invoke", lambda _runner, args: model_invocations.append(args) or SimpleNamespace(success=True)
        )
    result = execute_notebook(values, store)
    if unavailable:
        assert result["model_run"].reason == "ARTIFACT_UNAVAILABLE"
        assert result["model_run"].publication is None
        args = model_invocations[0]
        assert json.loads(args[args.index("--vars") + 1]) == {"batch": 42}
        return
    assert result["model_run"].reason == "NO_BASELINE"
    assert result["model_run"].publication == "RECORDED"
    args = model_invocations[0]
    assert json.loads(args[args.index("--vars") + 1]) == {"batch": 42}
    assert not Path(result["local_dir"]).exists()
    assert result["model_run"].model_id == build.model_id


@pytest.mark.parametrize("field, value", [("model_ids", None), ("models", []), ("attempt", None)])
def test_malformed_preparation_is_explicit_failure(matching_preparation, field, value):
    store, client, config, _context, _build, _snapshots, values = matching_preparation
    prepared = json.loads(reuse_runtime.download_artifact(client, **json.loads(values["factory_reuse_preparation"])))
    prepared[field] = value
    content = reuse.canonical_json(prepared).encode()
    digest = reuse.digest_bytes(content)
    path = "/Volumes/" + "/".join(config.artifact_volume) + "/preparation_" + digest + ".json"
    client.files.objects[path] = content
    values["factory_reuse_preparation"] = json.dumps({"path": path, "sha256": digest})
    with pytest.raises(ValueError, match="reuse preparation"):
        execute_notebook(values, store)
    assert "DBT_ACCESS_TOKEN" not in os.environ


def test_failed_data_test_retains_successful_model_evidence(notebook_runtime, model_invocations, monkeypatch):
    from dbt.cli.main import dbtRunner  # pylint: disable=import-outside-toplevel

    store, client, config, context, build, _snapshots, values = notebook_runtime
    model = execute_notebook(values, store)
    assert model["model_run"].publication == "RECORDED"
    assert len(model_invocations) == 1
    successful = reuse_runtime.read_build(store, client, config, context, build.model_id, build.output, job_id="job")
    values["factory_reuse_config"] = ""
    values["dbt_commands"] = json.dumps(["dbt test --select orders --target dev"])

    def failing_test(_runner, args):
        assert args[0] == "test"
        return SimpleNamespace(success=False, exception="data test failed")

    monkeypatch.setattr(dbtRunner, "invoke", failing_test)
    with pytest.raises(RuntimeError, match="data test failed"):
        execute_notebook(values, store)
    assert (
        reuse_runtime.read_build(store, client, config, context, build.model_id, build.output, job_id="job")
        == successful
    )
    assert "DBT_ACCESS_TOKEN" not in os.environ


@pytest.mark.parametrize("forced", [False, True])
def test_retained_lineage_blocks_stale_preparation_and_new_build_evidence(matching_preparation, monkeypatch, forced):
    from dbt.cli.main import dbtRunner  # pylint: disable=import-outside-toplevel

    store, client, config, context, build, _snapshots, values = matching_preparation
    monkeypatch.setattr(reuse_runtime, "REUSE_ENABLED", True)
    _descriptor, manifest, _provenance = reuse_runtime.load_deployment(client, config, build.descriptor)
    model = next(
        model for model in reuse.describe_models(json.loads(manifest), context) if model.unique_id == build.model_id
    )
    original = store.sql

    def observed(statement, args=None):
        if "`system`.`access`.`table_lineage`" in statement:
            return state_tests.Rows([{"catalog": "raw", "schema": "sales", "identifier": "hidden", "path": None}])
        return original(statement, args=args)

    monkeypatch.setattr(store, "sql", observed)
    assert reuse_runtime.prepare_lineage(store, config, context, model, job_id="job")["reason"] == "UNDECLARED_INPUT"
    monkeypatch.setattr(store, "sql", original)
    values["factory_force_rebuild"] = "true" if forced else "false"
    calls = []
    monkeypatch.setattr(dbtRunner, "invoke", lambda _runner, args: calls.append(args) or SimpleNamespace(success=True))
    result = execute_notebook(values, store)
    assert len(calls) == 1
    assert result["model_run"].reason == "UNDECLARED_INPUT"
    assert result["model_run"].publication is None
