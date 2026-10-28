"""Reuse policy against real dbt-produced models; no workspace access."""

import copy
import json
import os
from unittest.mock import patch as patch_environment
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from databricks_dbt_factory.reuse import (
    ExecutionContext,
    Relation,
    ReuseConfig,
    describe_models,
    eligibility_reason,
    lineage_reason,
)
from databricks_dbt_factory.reuse_prepare import capture_parse, read_provenance, verify_snapshot
from tests.integration.dbt_helpers import REUSE_PROFILES as PROFILES


IDENTITY = {
    "project_id": "probe",
    "environment": "prod",
    "workspace_id": "123",
    "metastore_id": "meta",
    "principal": "runner@example.com",
}
CONFIG = {
    "format_version": 1,
    "project_id": "probe",
    "environment": "prod",
    "owner_key": "orders-job",
    "state_catalog": "state",
    "state_schema": "factory",
    "artifact_volume": {"catalog": "state", "schema": "factory", "volume": "artifacts"},
}


@pytest.fixture(autouse=True)
def reuse_credentials(monkeypatch):
    monkeypatch.setenv("DBT_ACCESS_TOKEN", "dummy-parse-only")


@pytest.fixture(scope="module")
def captured(tmp_path_factory):
    root = tmp_path_factory.mktemp("reuse-project")
    project, profiles = root / "project", root / "profiles"
    (project / "models").mkdir(parents=True)
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(PROFILES)
    (project / "dbt_project.yml").write_text('name: probe\nversion: "1.0"\nconfig-version: 2\nprofile: probe\n')
    models = {
        "orders": "{{ config(materialized='table', meta={'dbt_factory': {'reuse': {'enabled': true}}}) }}\n"
        "select * from {{ source('raw', 'orders') }}",
        "plain": "{{ config(materialized='table') }} select 1 as id",
        "incremental": "{{ config(materialized='incremental', meta={'dbt_factory': {'reuse': {'enabled': true}}}) }} select 1 as id",
        "view": "{{ config(materialized='view', meta={'dbt_factory': {'reuse': {'enabled': true}}}) }} select 1 as id",
        "hooked": "{{ config(materialized='table', pre_hook='select 1', meta={'dbt_factory': {'reuse': {'enabled': true}}}) }} select 1 as id",
        "consumer": "{{ config(materialized='table', meta={'dbt_factory': {'reuse': {'enabled': true}}}) }} select * from {{ ref('view') }}",
        "extra": "{{ config(materialized='table', meta={'dbt_factory': {'reuse': {'enabled': true, 'extra_inputs': "
        "[{'catalog': 'raw', 'schema': 'sales', 'identifier': 'exchange.rates'}]}}}) }} select 1 as id",
    }
    for name, sql in models.items():
        (project / "models" / f"{name}.sql").write_text(sql)
    (project / "models" / "custom.sql").write_text(
        "{{ config(materialized='custom', meta={'dbt_factory': {'reuse': {'enabled': true}}}) }} select 1 as id"
    )
    (project / "models" / "python_model.py").write_text(
        "def model(dbt, session):\n"
        "    dbt.config(materialized='table', meta={'dbt_factory': {'reuse': {'enabled': True}}})\n"
        "    return session.sql('select 1 as id')\n"
    )
    (project / "models" / "sources.yml").write_text(
        "version: 2\nsources:\n  - name: raw\n    database: raw\n    schema: sales\n    tables:\n      - name: orders\n"
    )
    with patch_environment.dict(os.environ, {"DBT_ACCESS_TOKEN": "dummy-parse-only"}):
        manifest, provenance = capture_parse(
            project, profiles, identity=IDENTITY, target="dev", deployment_ref="release-1"
        )
    return project, profiles, manifest, provenance


def test_config_is_validated_and_immutable():
    config = ReuseConfig.from_dict(CONFIG)
    assert config.artifact_volume == ("state", "factory", "artifacts")
    with pytest.raises(FrozenInstanceError):
        setattr(config, "owner_key", "other")


@pytest.mark.parametrize(
    "patch",
    [
        {"format_version": True},
        {"format_version": 2},
        {"format_version": 1.0},
        {"owner_key": ""},
        {"typo": "value"},
        {"artifact_volume": "state.factory.artifacts"},
        {"state_catalog": "{{job.id}}"},
    ],
)
def test_malformed_config_is_refused(patch):
    with pytest.raises(ValueError, match="reuse config"):
        ReuseConfig.from_dict(CONFIG | patch)


def test_real_models_have_conservative_eligibility(captured):
    _, _, manifest_bytes, provenance = captured
    manifest = json.loads(manifest_bytes)
    context = read_provenance(provenance, manifest_bytes).context
    assert eligibility_reason(manifest["nodes"]["model.probe.orders"], context) is None
    assert eligibility_reason(manifest["nodes"]["model.probe.plain"], context) == "NOT_OPTED_IN"
    assert eligibility_reason(manifest["nodes"]["model.probe.orders"], None) == "CONTEXT_UNKNOWN"
    descriptors = {item.unique_id: item for item in describe_models(manifest, context)}
    assert descriptors["model.probe.incremental"].reason == "UNSUPPORTED_MATERIALIZATION"
    assert descriptors["model.probe.custom"].reason == "UNSUPPORTED_MATERIALIZATION"
    assert descriptors["model.probe.python_model"].reason == "UNSUPPORTED_LANGUAGE"
    assert descriptors["model.probe.hooked"].reason == "HOOKS_PRESENT"
    assert descriptors["model.probe.consumer"].reason is None
    assert descriptors["model.probe.consumer"].definitions == ("model.probe.view",)
    assert descriptors["model.probe.consumer"].inputs == ()
    assert descriptors["model.probe.consumer"].views == (Relation("hive_metastore", "default", "view"),)
    assert lineage_reason(descriptors["model.probe.consumer"], descriptors["model.probe.consumer"].views) is None
    orders = descriptors["model.probe.orders"]
    assert orders.inputs == (Relation("raw", "sales", "orders"),)
    assert orders.output == Relation("hive_metastore", "default", "orders")
    extra = descriptors["model.probe.extra"]
    assert extra.inputs == (Relation("raw", "sales", "exchange.rates"),)
    assert lineage_reason(extra, (Relation("raw", "sales", "unknown"),)) == "UNDECLARED_INPUT"
    assert lineage_reason(extra, extra.inputs) is None
    assert lineage_reason(extra, ()) is None  # Empty lineage supplies no additional evidence.


@pytest.mark.parametrize(
    "reuse", [True, {"enabled": "yes"}, {"enabld": True}, {"enabled": True, "extra_inputs": ["raw.sales.orders"]}]
)
def test_malformed_model_policy_names_the_model(captured, reuse):
    node = copy.deepcopy(json.loads(captured[2])["nodes"]["model.probe.orders"])
    node["config"]["meta"]["dbt_factory"]["reuse"] = reuse
    with pytest.raises(ValueError, match="model.probe.orders"):
        eligibility_reason(node, None)


def test_unknown_relations_fall_back_but_ambiguous_relations_and_broken_graph_fail(captured):
    manifest = json.loads(captured[2])
    context = read_provenance(captured[3], captured[2]).context
    manifest["nodes"]["model.probe.orders"]["database"] = None
    assert (
        next(item for item in describe_models(manifest, context) if item.unique_id == "model.probe.orders").reason
        == "RELATION_UNKNOWN"
    )
    manifest = json.loads(captured[2])
    manifest["nodes"]["model.probe.plain"]["alias"] = "orders"
    with pytest.raises(ValueError, match="ambiguous.*model.probe.orders"):
        describe_models(manifest, context)
    manifest = json.loads(captured[2])
    del manifest["sources"]["source.probe.raw.orders"]
    with pytest.raises(ValueError, match="model.probe.orders.*source.probe.raw.orders"):
        describe_models(manifest, context)


def test_context_is_canonical_and_separate_from_artifact_identity(captured):
    project, _, manifest, provenance = captured
    record = read_provenance(provenance, manifest)
    payload = json.loads(record.context.canonical_json)
    assert ExecutionContext.from_dict(dict(reversed(list(payload.items())))).digest == record.context.digest
    assert ExecutionContext.from_dict(payload | {"target": "other"}).digest != record.context.digest
    assert record.manifest_sha256 not in record.context.canonical_json
    assert record.snapshot_sha256 not in record.context.canonical_json
    assert "dummy" not in provenance.decode()  # Profile credential never enters the snapshot/context.
    assert verify_snapshot(record, project) == record.snapshot_sha256


def test_provenance_binds_manifest_and_rejects_bad_records(captured):
    manifest, provenance = captured[2:]
    with pytest.raises(ValueError, match="manifest.*mismatch"):
        read_provenance(provenance, manifest + b"\n")
    for key in ("manifest_sha256", "snapshot_sha256", "context", "deployment_ref"):
        payload = json.loads(provenance)
        del payload[key]
        with pytest.raises(ValueError, match="provenance"):
            read_provenance(json.dumps(payload).encode(), manifest)


def test_capture_tracks_source_and_installed_packages_without_broad_context_invalidation(captured, tmp_path):
    import shutil  # pylint: disable=import-outside-toplevel

    original, profiles, _, old_provenance = captured
    project = tmp_path / "project"
    shutil.copytree(original, project)
    package = project / "dbt_packages" / "other"
    (project / "packages.yml").write_text("packages:\n  - local: dbt_packages/other\n")
    (package / "models").mkdir(parents=True)
    (package / "dbt_project.yml").write_text('name: other\nversion: "1.0"\nconfig-version: 2\n')
    model = package / "models" / "packaged.sql"
    model.write_text("select 1 as id")
    manifest, provenance = capture_parse(project, profiles, identity=IDENTITY, target="dev", deployment_ref="release-2")
    assert "model.other.packaged" in json.loads(manifest)["nodes"]
    assert json.loads(provenance)["context"] == json.loads(old_provenance)["context"]
    assert json.loads(provenance)["snapshot_sha256"] != json.loads(old_provenance)["snapshot_sha256"]
    model.write_text("select 2 as id")
    with pytest.raises(ValueError, match="snapshot.*mismatch"):
        verify_snapshot(read_provenance(provenance, manifest), project)


def test_capture_refuses_secret_context_profiles_in_snapshot_and_symlinks(captured, tmp_path):
    project, profiles = captured[:2]
    with pytest.raises(ValueError, match="secret"):
        capture_parse(
            project,
            profiles,
            identity=IDENTITY,
            target="dev",
            deployment_ref="release-1",
            environment_inputs=("DBT_ENV_SECRET_TOKEN",),
        )
    with pytest.raises(ValueError, match="profiles.*outside"):
        capture_parse(project, project, identity=IDENTITY, target="dev", deployment_ref="release-1")
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    (snapshot / "models").symlink_to(project / "models")
    with pytest.raises(ValueError, match="symlink"):
        verify_snapshot(read_provenance(captured[3], captured[2]), snapshot)


def test_cli_import_does_not_load_dbt_or_sdk(tmp_path):
    import subprocess  # pylint: disable=import-outside-toplevel
    import sys  # pylint: disable=import-outside-toplevel

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import databricks_dbt_factory.main; "
            "assert not any(m == 'dbt' or m.startswith(('dbt.', 'databricks.sdk')) for m in sys.modules)",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_cli_checks_provenance_and_target_before_publication(captured, monkeypatch, capsys, tmp_path):
    from databricks_dbt_factory.main import main  # pylint: disable=import-outside-toplevel

    manifest_path, provenance_path, config_path = (
        tmp_path / name for name in ("manifest.json", "provenance.json", "reuse.json")
    )
    manifest_path.write_bytes(captured[2])
    provenance_path.write_bytes(captured[3])
    config_path.write_text(json.dumps(CONFIG))
    target = tmp_path / "output" / "job.yml"
    arguments = [
        "main.py",
        "--dbt-manifest-path",
        str(manifest_path),
        "--input-job-spec-path",
        str(Path(__file__).parent / "test_data" / "job_definition_template.yaml"),
        "--target-job-spec-path",
        str(target),
        "--reuse-config",
        str(config_path),
        "--reuse-provenance",
        str(provenance_path),
        "--project-directory",
        str(captured[0]),
        "--profiles-directory",
        str(captured[1]),
    ]
    monkeypatch.setattr("sys.argv", [*arguments, "--target", "other"])
    with pytest.raises(SystemExit, match="target mismatch"):
        main()
    assert not target.parent.exists()
    manifest_path.write_bytes(captured[2] + b"\n")
    monkeypatch.setattr("sys.argv", [*arguments, "--target", "dev"])
    with pytest.raises(SystemExit, match="manifest digest mismatch"):
        main()
    manifest_path.write_bytes(captured[2])
    main()
    assert target.exists()
    diagnostics = capsys.readouterr().err
    assert "CONTEXT_UNKNOWN" not in diagnostics
    assert "Runtime reuse is not enabled" in diagnostics
    monkeypatch.setattr("sys.argv", [*arguments, "--target", "dev", "--enable-dbt-deps"])
    main()
    assert "CONTEXT_UNKNOWN" in capsys.readouterr().err


def test_capture_uses_the_recorded_environment_for_the_actual_parse(captured, monkeypatch, tmp_path):
    import shutil  # pylint: disable=import-outside-toplevel
    import subprocess  # pylint: disable=import-outside-toplevel

    project = tmp_path / "project"
    shutil.copytree(captured[0], project)
    with (project / "dbt_project.yml").open("a") as stream:
        stream.write("models:\n  probe:\n    +schema: \"{{ env_var('REUSE_CONTEXT') }}\"\n")
    monkeypatch.setenv("REUSE_CONTEXT", "before")
    original_run = subprocess.run

    def run_with_changed_parent_environment(*args, **kwargs):
        monkeypatch.setenv("REUSE_CONTEXT", "after")
        return original_run(*args, **kwargs)  # pylint: disable=subprocess-run-check

    monkeypatch.setattr(subprocess, "run", run_with_changed_parent_environment)
    manifest, provenance = capture_parse(
        project,
        captured[1],
        identity=IDENTITY,
        target="dev",
        deployment_ref="release-1",
        environment_inputs=("REUSE_CONTEXT",),
    )
    assert json.loads(provenance)["context"]["environment_inputs"] == {"REUSE_CONTEXT": "before"}
    assert json.loads(manifest)["nodes"]["model.probe.orders"]["schema"] == "default_before"


def test_project_hooks_disable_candidates(captured, tmp_path):
    import shutil  # pylint: disable=import-outside-toplevel

    project = tmp_path / "project"
    shutil.copytree(captured[0], project)
    with (project / "dbt_project.yml").open("a") as stream:
        stream.write("on-run-start:\n  - select 1\n")
    manifest, provenance = capture_parse(
        project, captured[1], identity=IDENTITY, target="dev", deployment_ref="release-1"
    )
    context = read_provenance(provenance, manifest).context
    descriptor = next(
        item for item in describe_models(json.loads(manifest), context) if item.unique_id == "model.probe.orders"
    )
    assert descriptor.reason == "HOOKS_PRESENT"


def test_extra_inputs_bind_to_producers_and_reject_cycles(captured):
    manifest = json.loads(captured[2])
    context = read_provenance(captured[3], captured[2]).context
    orders = manifest["nodes"]["model.probe.orders"]
    extra = manifest["nodes"]["model.probe.extra"]
    extra["config"]["meta"]["dbt_factory"]["reuse"]["extra_inputs"] = [
        {"catalog": orders["database"].upper(), "schema": orders["schema"], "identifier": "ORDERS"}
    ]
    descriptor = next(item for item in describe_models(manifest, context) if item.unique_id == "model.probe.extra")
    assert descriptor.dependencies == ("model.probe.orders",)
    orders["config"]["meta"]["dbt_factory"]["reuse"]["extra_inputs"] = [
        {"catalog": extra["database"], "schema": extra["schema"], "identifier": "extra"}
    ]
    with pytest.raises(ValueError, match="cycle.*model.probe"):
        describe_models(manifest, context)


def test_definition_closure_uses_dbt_graph_and_rejects_unresolved_dependencies(captured, tmp_path):
    import shutil  # pylint: disable=import-outside-toplevel

    project = tmp_path / "project"
    shutil.copytree(captured[0], project)
    (project / "models" / "view.sql").write_text("select * from {{ ref('inline') }}")
    (project / "models" / "inline.sql").write_text(
        "{{ config(materialized='ephemeral') }} select * from {{ source('raw', 'orders') }}"
    )
    manifest_bytes, provenance = capture_parse(
        project, captured[1], identity=IDENTITY, target="dev", deployment_ref="closure"
    )
    manifest = json.loads(manifest_bytes)
    context = read_provenance(provenance, manifest_bytes).context
    consumer = next(model for model in describe_models(manifest, context) if model.unique_id == "model.probe.consumer")
    assert consumer.reason is None
    assert consumer.inputs == (Relation("raw", "sales", "orders"),)
    assert consumer.definitions == ("model.probe.inline", "model.probe.view")
    del manifest["nodes"]["model.probe.inline"]
    with pytest.raises(ValueError, match="consumer.*missing declared dependency"):
        describe_models(manifest, context)


def test_dependency_fallback_reasons_are_stable_across_processes(captured, tmp_path):
    import shutil  # pylint: disable=import-outside-toplevel
    import subprocess  # pylint: disable=import-outside-toplevel
    import sys  # pylint: disable=import-outside-toplevel

    project = tmp_path / "project"
    shutil.copytree(captured[0], project)
    (project / "models" / "consumer.sql").write_text(
        "{{ config(materialized='table', meta={'dbt_factory': {'reuse': {'enabled': true}}}) }} "
        "select * from {{ ref('custom') }} union all select * from {{ source('raw', 'orders') }}"
    )
    manifest, provenance = capture_parse(project, captured[1], identity=IDENTITY, target="dev", deployment_ref="stable")
    value = json.loads(manifest)
    value["sources"]["source.probe.raw.orders"]["database"] = None  # Unavailable relation metadata.
    artifact = tmp_path / "manifest.json"
    artifact.write_text(json.dumps(value))
    context = json.loads(provenance)["context"]
    script = (
        "import json,sys; from pathlib import Path; from dataclasses import asdict; "
        "from databricks_dbt_factory.reuse import describe_models, ExecutionContext, canonical_json; "
        "print(canonical_json([asdict(m) for m in describe_models(json.loads(Path(sys.argv[1]).read_bytes()), "
        "ExecutionContext.from_dict(json.loads(sys.argv[2])))]))"
    )
    results = {
        subprocess.run(
            [sys.executable, "-c", script, str(artifact), json.dumps(context)],
            env=os.environ | {"PYTHONHASHSEED": str(seed)},
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for seed in range(8)
    }
    assert len(results) == 1  # Immutable descriptors cannot depend on the process hash seed.
