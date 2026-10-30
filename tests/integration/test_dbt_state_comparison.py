"""Selective comparison against fresh artifacts from the installed dbt engine."""

import json
import os
from unittest.mock import patch
import shutil

import pytest

from databricks_dbt_factory import reuse_prepare
from databricks_dbt_factory.reuse import ExecutionContext
from tests import test_reuse
from tests.test_reuse import IDENTITY

from tests.integration.dbt_helpers import REUSE_PROFILES as PROFILES

reuse_credentials = test_reuse.reuse_credentials


@pytest.fixture(scope="module")
def baseline(tmp_path_factory):
    root = tmp_path_factory.mktemp("native-state")
    project, profiles = root / "project", root / "profiles"
    for directory in (project / "models", project / "macros", profiles):
        directory.mkdir(parents=True)
    (profiles / "profiles.yml").write_text(PROFILES)
    (project / "dbt_project.yml").write_text('name: probe\nversion: "1.0"\nconfig-version: 2\nprofile: probe\n')
    config = "{{ config(materialized='table', meta={'dbt_factory': {'reuse': {'enabled': true}}}) }} "
    for name, sql in {
        "orders": "select {{ outer() }} as id",
        "other": "select 1 as id",
        "consumer": "select * from {{ ref('definition') }}",
    }.items():
        (project / "models" / f"{name}.sql").write_text(config + sql)
    (project / "models" / "definition.sql").write_text(
        "{{ config(materialized='ephemeral') }} select * from {{ ref('deployed') }}"
    )
    (project / "models" / "deployed.sql").write_text(
        "{{ config(materialized='view') }} select 1 as id from {{ source('raw', 'orders') }}"
    )
    (project / "models" / "sources.yml").write_text(
        "version: 2\nsources:\n  - name: raw\n    database: raw\n    schema: sales\n    tables:\n      - name: orders\n"
    )
    for name, body in {"outer": "{{ inner() }}", "inner": "1", "unused": "1"}.items():
        (project / "macros" / f"{name}.sql").write_text("{% macro " + name + "() %}" + body + "{% endmacro %}")
    with patch.dict(os.environ, {"DBT_ACCESS_TOKEN": "dummy-parse-only"}):
        manifest, provenance = reuse_prepare.capture_parse(
            project, profiles, identity=IDENTITY, target="dev", deployment_ref="baseline"
        )
    return project, profiles, manifest, provenance


@pytest.mark.parametrize(
    "filename,old,new,changed",
    [
        ("models/orders.sql", "as id", "as changed", {"orders"}),
        ("macros/inner.sql", "%}1", "%}2", {"orders"}),
        ("macros/unused.sql", "%}1", "%}2", set()),
        ("models/other.sql", "select 1", "select 2", {"other"}),
        ("models/definition.sql", "select *", "select id", {"consumer"}),
        ("models/deployed.sql", "select 1", "select 2", {"consumer"}),
        ("models/consumer.sql", "ref('definition')", "ref('other')", {"consumer"}),
        ("models/orders.sql", "'enabled': true", "'enabled': false", {"orders"}),
        ("models/sources.yml", "schema: sales", "schema: other", {"consumer"}),
    ],
)
def test_native_changes_are_selective(baseline, tmp_path, monkeypatch, filename, old, new, changed):
    project = tmp_path / "current"
    shutil.copytree(baseline[0], project)
    path = project / filename
    path.write_text(path.read_text().replace(old, new))
    manifest, provenance = reuse_prepare.capture_parse(
        project, baseline[1], identity=IDENTITY, target="dev", deployment_ref="current"
    )
    calls = []
    original = reuse_prepare.subprocess.run

    def count(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)  # pylint: disable=subprocess-run-check

    monkeypatch.setattr(reuse_prepare.subprocess, "run", count)
    result = reuse_prepare.compare_baselines(
        project,
        baseline[1],
        manifest,
        provenance,
        {f"model.probe.{name}": (baseline[2], baseline[3]) for name in ("orders", "other", "consumer")},
    )
    assert {uid.rsplit(".", 1)[1] for uid, reason in result["models"].items() if reason != "CODE_UNCHANGED"} == changed
    assert len(calls) == 1
    command = calls[0]
    assert command[command.index("--state") + 1] != command[command.index("--target-path") + 1]
    assert result["comparisons"][0]["diagnostics"]


def test_context_changes_and_missing_baselines_do_not_invoke_dbt(baseline, monkeypatch):
    def unexpected(*_args, **_kwargs):
        raise AssertionError("cross-context dbt comparison")

    monkeypatch.setattr(reuse_prepare.subprocess, "run", unexpected)
    for field, value in (
        ("versions", {"dbt-core": "0.0.0"}),
        ("versions", {"databricks-sql-connector": "0.0.0"}),
        ("vars", {"date": "tomorrow"}),
        ("environment_inputs", {"REGION": "other"}),
    ):
        payload = json.loads(baseline[3])
        context = json.loads(json.dumps(payload["context"]))
        context[field].update(value)
        payload.update(context=context, context_sha256=ExecutionContext.from_dict(context).digest)
        result = reuse_prepare.compare_baselines(
            baseline[0],
            baseline[1],
            baseline[2],
            baseline[3],
            {"model.probe.orders": (baseline[2], json.dumps(payload).encode())},
        )
        assert result["models"]["model.probe.orders"] == "CONTEXT_CHANGED"
        assert result["models"]["model.probe.other"] == "NO_BASELINE"


def test_partial_success_uses_each_models_own_baseline(baseline, tmp_path, monkeypatch):
    project = tmp_path / "current"
    shutil.copytree(baseline[0], project)
    path = project / "models" / "orders.sql"
    path.write_text(path.read_text().replace("as id", "as changed"))
    manifest, provenance = reuse_prepare.capture_parse(
        project, baseline[1], identity=IDENTITY, target="dev", deployment_ref="partial"
    )
    original = reuse_prepare.subprocess.run
    calls = []

    def count(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)  # pylint: disable=subprocess-run-check

    monkeypatch.setattr(reuse_prepare.subprocess, "run", count)
    result = reuse_prepare.compare_baselines(
        project,
        baseline[1],
        manifest,
        provenance,
        {"model.probe.orders": (baseline[2], baseline[3]), "model.probe.other": (manifest, provenance)},
    )
    assert result["models"] == {
        "model.probe.orders": "CODE_CHANGED",
        "model.probe.other": "CODE_UNCHANGED",
        "model.probe.consumer": "NO_BASELINE",
    }
    assert len(calls) == 2


def test_invalid_current_project_fails_instead_of_authorizing_reuse(baseline, tmp_path):
    project = tmp_path / "broken"
    shutil.copytree(baseline[0], project)
    (project / "models" / "orders.sql").write_text("select * from {{ ref('missing') }}")
    with pytest.raises(ValueError, match="snapshot digest mismatch"):
        reuse_prepare.compare_baselines(project, baseline[1], baseline[2], baseline[3], {})


@pytest.mark.parametrize("setting", ["vars", "environment"])
def test_fresh_captures_track_declared_context_even_when_sql_does_not_change(baseline, monkeypatch, setting):
    options = {"variables": {"unused": 2}} if setting == "vars" else {"environment_inputs": ("FACTORY_REGION",)}
    monkeypatch.setenv("FACTORY_REGION", "west")
    manifest, provenance = reuse_prepare.capture_parse(
        baseline[0], baseline[1], identity=IDENTITY, target="dev", deployment_ref="changed-context", **options
    )
    result = reuse_prepare.compare_baselines(
        baseline[0],
        baseline[1],
        manifest,
        provenance,
        {"model.probe.orders": (baseline[2], baseline[3])},
    )
    assert result["models"]["model.probe.orders"] == "CONTEXT_CHANGED"
    assert not result["comparisons"]


def test_changed_runtime_profile_cannot_compare_a_different_graph(baseline, tmp_path):
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(PROFILES.replace("schema: default", "schema: changed"))
    with pytest.raises(ValueError, match="profiles digest mismatch"):
        reuse_prepare.compare_baselines(
            baseline[0], profiles, baseline[2], baseline[3], {"model.probe.orders": (baseline[2], baseline[3])}
        )


def test_native_command_errors_fail_and_missing_artifact_falls_back(baseline, monkeypatch):
    original = reuse_prepare.subprocess.run

    def fail(*args, **kwargs):
        return replace_result(original(*args, **kwargs))  # pylint: disable=subprocess-run-check

    def replace_result(result):
        result.returncode = 2
        return result

    monkeypatch.setattr(reuse_prepare.subprocess, "run", fail)
    with pytest.raises(ValueError, match="dbt state comparison failed"):
        reuse_prepare.compare_baselines(
            baseline[0],
            baseline[1],
            baseline[2],
            baseline[3],
            {"model.probe.orders": (baseline[2], baseline[3])},
        )
    result = reuse_prepare.compare_baselines(
        baseline[0],
        baseline[1],
        baseline[2],
        baseline[3],
        {"model.probe.orders": None},
    )
    assert result["models"]["model.probe.orders"] == "NO_BASELINE"


def test_unsupported_native_output_cannot_establish_unchanged_code(baseline, monkeypatch):
    import subprocess  # pylint: disable=import-outside-toplevel

    monkeypatch.setattr(
        reuse_prepare.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, "diagnostic\n", ""),
    )
    result = reuse_prepare.compare_baselines(
        baseline[0], baseline[1], baseline[2], baseline[3], {"model.probe.orders": (baseline[2], baseline[3])}
    )
    assert result["models"]["model.probe.orders"] == "COMPARISON_UNSUPPORTED"
