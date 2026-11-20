"""Bind reuse to validated profile templates consumed by the real dbt parser."""

import json
from pathlib import Path

import pytest

from databricks_dbt_factory import reuse_prepare
from tests.integration.dbt_helpers import PROFILES
from tests.test_reuse import IDENTITY


@pytest.fixture
def profile_project(tmp_path, monkeypatch):
    project, profiles = tmp_path / "project", tmp_path / "profiles"
    (project / "models").mkdir(parents=True)
    profiles.mkdir()
    (project / "dbt_project.yml").write_text('name: probe\nversion: "1.0"\nconfig-version: 2\nprofile: probe\n')
    (project / "models" / "orders.sql").write_text(
        "{{ config(materialized='table', meta={'dbt_factory': {'reuse': {'enabled': true}}}) }} select 1 as id"
    )
    (profiles / "profiles.yml").write_text(
        PROFILES.replace("token: dummy", "token: \"{{ env_var('DBT_ACCESS_TOKEN') }}\"")
    )
    monkeypatch.setenv("DBT_ACCESS_TOKEN", "credential-must-not-be-persisted")
    return project, profiles


def capture(paths, **kwargs):
    return reuse_prepare.capture_parse(*paths, identity=IDENTITY, target="dev", deployment_ref="profile-test", **kwargs)


@pytest.mark.parametrize(
    "change",
    [
        lambda text: text.replace("{{ env_var('DBT_ACCESS_TOKEN') }}", "literal-private-token"),
        lambda text: text + "      client_secret: literal-private-secret\n",
        lambda text: text.replace("{{ env_var('DBT_ACCESS_TOKEN') }}", "{{ env_var('OTHER_TOKEN') }}"),
        lambda text: text.replace("DBT_ACCESS_TOKEN", "DBT_ENV_SECRET_UNVERSIONED_TOKEN"),
        lambda text: text.replace("example.databricks.com", "private:token@example.databricks.com"),
        lambda text: text.replace(
            "{{ env_var('DBT_ACCESS_TOKEN') }}", "{{ env_var('DBT_ACCESS_TOKEN', 'default-secret') }}"
        ),
        lambda text: text.replace("schema: default", "schema: \"{{ env_var('UNBOUND_SCHEMA') }}\""),
        lambda text: text.replace("schema: default", 'schema: "{{ target.name }}"'),
    ],
)
def test_capture_refuses_unbound_or_literal_credentials_before_dbt(profile_project, monkeypatch, change):
    path = profile_project[1] / "profiles.yml"
    path.write_text(change(path.read_text()))

    def unexpected(*_args, **_kwargs):
        raise AssertionError("unvalidated profiles reached dbt")

    monkeypatch.setattr(reuse_prepare.subprocess, "run", unexpected)
    with pytest.raises(ValueError, match="reuse profiles"):
        capture(profile_project)


def test_profile_only_edit_is_detected_without_native_comparison(profile_project, monkeypatch):
    manifest, provenance = capture(profile_project)
    assert b"credential-must-not-be-persisted" not in manifest + provenance
    assert json.loads(provenance)["format_version"] == 2
    path = profile_project[1] / "profiles.yml"
    path.write_text(path.read_text().replace("/warehouses/x", "/warehouses/changed"))

    def unexpected(*_args, **_kwargs):
        raise AssertionError("changed profiles reached dbt")

    monkeypatch.setattr(reuse_prepare.subprocess, "run", unexpected)
    with pytest.raises(ValueError, match="profiles.*mismatch"):
        reuse_prepare.compare_baselines(*profile_project, manifest, provenance, {})


def test_native_parse_consumes_private_profile_snapshot(profile_project, monkeypatch):
    original = reuse_prepare.subprocess.run
    source = profile_project[1] / "profiles.yml"
    used = []

    def mutate_source(command, **kwargs):
        private = Path(command[command.index("--profiles-dir") + 1])
        used.append(private)
        assert private != profile_project[1]
        source.write_text(source.read_text().replace("schema: default", "schema: changed"))
        return original(command, **kwargs)  # pylint: disable=subprocess-run-check

    monkeypatch.setattr(reuse_prepare.subprocess, "run", mutate_source)
    manifest, provenance = capture(profile_project)
    assert json.loads(manifest)["nodes"]["model.probe.orders"]["schema"] == "default"
    assert not used[0].exists()
    with pytest.raises(ValueError, match="profiles.*mismatch"):
        reuse_prepare.verify_profiles(reuse_prepare.read_provenance(provenance, manifest), profile_project[1])


def test_versioned_secret_and_routing_environment_are_bound_without_secret_values(profile_project, monkeypatch):
    path = profile_project[1] / "profiles.yml"
    path.write_text(
        path.read_text()
        .replace("DBT_ACCESS_TOKEN", "DBT_ENV_SECRET_FACTORY_TOKEN")
        .replace("schema: default", "schema: \"{{ env_var('FACTORY_SCHEMA') }}\"")
    )
    monkeypatch.setenv("DBT_ENV_SECRET_FACTORY_TOKEN", "private-secret-value")
    monkeypatch.setenv("FACTORY_SCHEMA", "default")
    manifest, provenance = capture(
        profile_project,
        environment_inputs=("FACTORY_SCHEMA",),
        secret_versions={"DBT_ENV_SECRET_FACTORY_TOKEN": "version-1"},
    )
    assert b"private-secret-value" not in manifest + provenance
    context = json.loads(provenance)["context"]
    assert context["environment_inputs"] == {"FACTORY_SCHEMA": "default"}
    assert context["secret_versions"] == {"DBT_ENV_SECRET_FACTORY_TOKEN": "version-1"}
    monkeypatch.setenv("FACTORY_SCHEMA", "changed")
    compared = reuse_prepare.compare_baselines(*profile_project, manifest, provenance, {})
    assert compared["models"]["model.probe.orders"] == "CONTEXT_CHANGED"


@pytest.mark.parametrize(
    "host, expected",
    [
        ("https://example.databricks.com/", None),
        ("example.databricks.com", None),
        ("https://EXAMPLE.databricks.com", None),
        ("https://different.databricks.com/", "CONTEXT_CHANGED"),
        ("https://private@example.databricks.com/", "CONTEXT_CHANGED"),
        ("https://example.databricks.com/hidden", "CONTEXT_CHANGED"),
    ],
)
def test_runtime_profile_requires_sdk_workspace(profile_project, host, expected):
    manifest, provenance = capture(profile_project)
    parsed = reuse_prepare.read_provenance(provenance, manifest)
    template = reuse_prepare.verify_profiles(parsed, profile_project[1])
    assert reuse_prepare.runtime_profile_reason(template, parsed.context, host) == expected


def test_runtime_declines_unattested_token_principal(profile_project, monkeypatch):
    path = profile_project[1] / "profiles.yml"
    path.write_text(path.read_text().replace("DBT_ACCESS_TOKEN", "DBT_ENV_SECRET_FACTORY_TOKEN"))
    monkeypatch.setenv("DBT_ENV_SECRET_FACTORY_TOKEN", "private-pat")
    manifest, provenance = capture(profile_project, secret_versions={"DBT_ENV_SECRET_FACTORY_TOKEN": "v1"})
    parsed = reuse_prepare.read_provenance(provenance, manifest)
    template = reuse_prepare.verify_profiles(parsed, profile_project[1])
    assert reuse_prepare.runtime_profile_reason(template, parsed.context, "example.databricks.com") == (
        "PROFILE_UNSUPPORTED_AUTH"
    )


def test_capture_discards_comments_and_never_hashes_credentials(profile_project, monkeypatch):
    path = profile_project[1] / "profiles.yml"
    path.write_text(path.read_text() + "# comment-private-token\n")
    original = reuse_prepare.digest_bytes
    hashed = []

    def digest(content):
        hashed.append(content)
        return original(content)

    monkeypatch.setattr(reuse_prepare, "digest_bytes", digest)
    capture(profile_project)
    assert all(b"comment-private-token" not in content for content in hashed)
    assert all(b"credential-must-not-be-persisted" not in content for content in hashed)


def test_new_capture_invalidates_old_profile_even_when_native_graph_is_identical(profile_project, monkeypatch):
    old_manifest, old_provenance = capture(profile_project)
    path = profile_project[1] / "profiles.yml"
    path.write_text(path.read_text().replace("/warehouses/x", "/warehouses/other"))
    manifest, provenance = capture(profile_project)
    reuse_prepare.verify_parsed_manifest(json.loads(manifest), old_manifest)
    assert json.loads(provenance)["profiles_sha256"] != json.loads(old_provenance)["profiles_sha256"]

    def unexpected(*_args, **_kwargs):
        raise AssertionError("cross-profile comparison reached dbt")

    monkeypatch.setattr(reuse_prepare.subprocess, "run", unexpected)
    result = reuse_prepare.compare_baselines(
        *profile_project, manifest, provenance, {"model.probe.orders": (old_manifest, old_provenance)}
    )
    assert result["models"]["model.probe.orders"] == "CONTEXT_CHANGED"
    assert not result["comparisons"]


def test_native_parse_freezes_declared_routing_before_subprocess_launch(profile_project, monkeypatch):
    path = profile_project[1] / "profiles.yml"
    path.write_text(path.read_text().replace("/sql/1.0/warehouses/x", "\"{{ env_var('FACTORY_HTTP_PATH') }}\""))
    monkeypatch.setenv("FACTORY_HTTP_PATH", "/sql/1.0/warehouses/captured")
    original = reuse_prepare.subprocess.run

    def mutate_environment(command, **kwargs):
        monkeypatch.setenv("FACTORY_HTTP_PATH", "/sql/1.0/warehouses/changed")
        kwargs["env"]["FACTORY_HTTP_PATH"] = "/sql/1.0/warehouses/changed"
        private = Path(command[command.index("--profiles-dir") + 1])
        text = (private / "profiles.yml").read_text()
        assert "/sql/1.0/warehouses/captured" in text
        assert "FACTORY_HTTP_PATH" not in text
        assert "DBT_ACCESS_TOKEN" in text
        assert "credential-must-not-be-persisted" not in text
        return original(command, **kwargs)  # pylint: disable=subprocess-run-check

    monkeypatch.setattr(reuse_prepare.subprocess, "run", mutate_environment)
    manifest, provenance = capture(profile_project, environment_inputs=("FACTORY_HTTP_PATH",))
    assert json.loads(provenance)["context"]["environment_inputs"] == {
        "FACTORY_HTTP_PATH": "/sql/1.0/warehouses/captured"
    }
    assert "model.probe.orders" in json.loads(manifest)["nodes"]


def test_missing_routing_environment_cannot_produce_provenance(profile_project, monkeypatch):
    path = profile_project[1] / "profiles.yml"
    path.write_text(path.read_text().replace("schema: default", "schema: \"{{ env_var('FACTORY_SCHEMA') }}\""))
    monkeypatch.delenv("FACTORY_SCHEMA", raising=False)
    with pytest.raises(ValueError, match="routing env_var values must be nonempty literal strings"):
        capture(profile_project, environment_inputs=("FACTORY_SCHEMA",))


@pytest.mark.parametrize(
    "routing",
    ["{{ env_var('UNDECLARED_SCHEMA') }}", "{% if true %}changed{% endif %}", "{# hidden dependency #}changed"],
)
def test_captured_routing_values_cannot_be_rendered_again_by_native_dbt(profile_project, monkeypatch, routing):
    path = profile_project[1] / "profiles.yml"
    path.write_text(path.read_text().replace("schema: default", "schema: \"{{ env_var('FACTORY_SCHEMA') }}\""))
    monkeypatch.setenv("FACTORY_SCHEMA", routing)
    monkeypatch.setenv("UNDECLARED_SCHEMA", "changed")
    with pytest.raises(ValueError, match="routing env_var values must be nonempty literal strings"):
        capture(profile_project, environment_inputs=("FACTORY_SCHEMA",))
