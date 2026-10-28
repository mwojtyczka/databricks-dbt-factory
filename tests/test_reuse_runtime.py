"""Immutable artifacts and concrete state access; manifests come from real dbt parses."""
# pylint: disable=no-spark-argument-in-function
# Tests receive the controlled Spark boundary through the evidence fixture.

import io
import json
import re
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest
from databricks.sdk.errors import AlreadyExists

from databricks_dbt_factory.reuse import ExecutionContext, Relation, ReuseConfig, canonical_json, digest_bytes
from databricks_dbt_factory.reuse_prepare import read_provenance
from databricks_dbt_factory.reuse_runtime import (
    Attempt,
    BuildEvidence,
    EvidenceUnavailable,
    RelationSnapshot,
    claim_owner,
    download_artifact,
    prepare_bundle,
    publish_artifacts,
    initialize_state,
    read_build,
    read_descriptor,
    record_build,
    record_decision,
    snapshot_relation,
    write_bundle,
)
from tests import test_reuse
from tests.test_reuse import CONFIG

captured = test_reuse.captured


@pytest.mark.parametrize(
    "scope", [["model.probe.orders"], [], ["model.probe.missing"], ["model.probe.orders"] * 2, "orders"]
)
def test_preparation_claims_only_validated_job_scope(evidence, captured, monkeypatch, scope):
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel

    store, client, config, _context, build = evidence
    visited = []
    monkeypatch.setattr(reuse_runtime, "verify_runtime_identity", lambda *_args: None)

    def prepare(*args):
        visited.append(args[4].unique_id)
        return {"reason": "NO_BASELINE"}, None

    monkeypatch.setattr(reuse_runtime, "_prepare_model", prepare)
    args = (store, client, config, build.descriptor, captured[0], captured[1], Attempt("job", "run", "prepare", 0))
    if scope in (["model.probe.orders"], []):
        result = reuse_runtime.prepare_run(*args, model_ids=scope)
        assert visited == scope
        assert sorted(result["models"]) == scope
        assert result["model_ids"] == scope
    else:
        with pytest.raises(ValueError, match="model scope"):
            reuse_runtime.prepare_run(*args, model_ids=scope)
        assert not visited


def test_preparation_binds_exact_builds_and_publishes_only_a_small_pointer(evidence, captured, monkeypatch):
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel

    store, client, config, context, build = evidence
    claim_owner(store, config, context, build.output.relation, job_id="job")
    assert record_build(store, client, config, context, build, successful=True) == "RECORDED"
    payload = json.loads(context.canonical_json)
    client.get_workspace_id = lambda: payload["workspace_id"]
    client.metastores = SimpleNamespace(summary=lambda: SimpleNamespace(metastore_id=payload["metastore_id"]))
    client.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name=payload["principal"]))
    monkeypatch.setattr(reuse_runtime, "verify_runtime_identity", lambda *_args: None)

    def observe(_spark, _client, relation):
        if relation == build.output.relation:
            return build.output
        raise EvidenceUnavailable("METADATA_UNAVAILABLE")

    monkeypatch.setattr(reuse_runtime, "snapshot_relation", observe)
    downloads = []
    download = client.files.download

    def counted(path):
        downloads.append(path)
        return download(path)

    monkeypatch.setattr(client.files, "download", counted)
    attempt = Attempt("job", "next-run", "prepare", 0)
    result = reuse_runtime.prepare_run(store, client, config, build.descriptor, captured[0], captured[1], attempt)
    assert len(downloads) == 3  # One verified download per deployment artifact during preparation.
    baseline = read_build(store, client, config, context, build.model_id, build.output, job_id="job")
    decision = result["models"][build.model_id]
    assert decision["reason"] == "CODE_UNCHANGED"
    assert decision["baseline"] == {"key": baseline["key"], "sha256": baseline["sha256"]}
    assert result["attempt"] == asdict(attempt)
    assert result["deployment"] == build.descriptor
    pointer = reuse_runtime.publish_preparation(client, config, result)
    assert set(pointer) == {"path", "sha256"}
    assert len(canonical_json(pointer)) < 1024
    assert json.loads(download_artifact(client, **pointer)) == result
    assert reuse_runtime.publish_preparation(client, config, result) == pointer
    client.files.objects[pointer["path"]] = b"corrupt"
    with pytest.raises(ValueError, match="digest mismatch"):
        reuse_runtime.publish_preparation(client, config, result)


def test_preparation_missing_deployment_disables_reuse_and_unexpected_errors_fail(evidence, captured, monkeypatch):
    from databricks_dbt_factory.reuse_runtime import prepare_run  # pylint: disable=import-outside-toplevel
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel

    store, client, config, _context, build = evidence

    def unexpected(*_args):
        raise RuntimeError("unexpected infrastructure failure")

    monkeypatch.setattr(reuse_runtime, "verify_runtime_identity", unexpected)
    with pytest.raises(RuntimeError, match="unexpected infrastructure"):
        prepare_run(
            store, client, config, build.descriptor, captured[0], captured[1], Attempt("job", "run", "prepare", 0)
        )
    client.files.objects.clear()
    result = prepare_run(
        store, client, config, build.descriptor, captured[0], captured[1], Attempt("job", "run", "prepare", 0)
    )
    assert result["reason"] == "ARTIFACT_UNAVAILABLE"
    assert result["models"] == {}


def test_runtime_identity_checks_both_platform_and_sql_principals(evidence):
    from databricks_dbt_factory.reuse_runtime import verify_runtime_identity  # pylint: disable=import-outside-toplevel

    _store, client, _config, context, _build = evidence
    value = json.loads(context.canonical_json)
    client.get_workspace_id = lambda: value["workspace_id"]
    client.metastores = SimpleNamespace(summary=lambda: SimpleNamespace(metastore_id=value["metastore_id"]))
    client.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name=value["principal"]))
    sql = SimpleNamespace(sql=lambda _query: SimpleNamespace(first=lambda: [value["principal"]]))
    verify_runtime_identity(sql, client, context)
    sql.sql = lambda _query: SimpleNamespace(first=lambda: ["different"])
    with pytest.raises(EvidenceUnavailable, match="CONTEXT_CHANGED"):
        verify_runtime_identity(sql, client, context)


def test_preparation_state_outage_is_a_visible_per_model_fallback(evidence, captured, monkeypatch):
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel

    store, client, config, _context, build = evidence
    monkeypatch.setattr(reuse_runtime, "verify_runtime_identity", lambda *_args: None)

    def unavailable(*_args, **_kwargs):
        raise EvidenceUnavailable("STATE_UNAVAILABLE")

    monkeypatch.setattr(reuse_runtime, "claim_owner", unavailable)
    result = reuse_runtime.prepare_run(
        store, client, config, build.descriptor, captured[0], captured[1], Attempt("job", "run", "prepare", 0)
    )
    assert result["models"][build.model_id] == {"reason": "STATE_UNAVAILABLE", "baseline": None}
    assert result["comparisons"] == []


def test_deployed_views_require_stable_definition_evidence(evidence, captured, monkeypatch):
    from databricks_dbt_factory.reuse_runtime import ViewSnapshot, snapshot_view  # pylint: disable=import-outside-toplevel
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel

    store, client, config, context, build = evidence
    relation = Relation("hive_metastore", "default", "view")
    table = SimpleNamespace(
        catalog_name=relation.catalog,
        schema_name=relation.schema,
        name=relation.identifier,
        table_id="view-id",
        view_definition="select 1 as id",
        table_type=SimpleNamespace(value="VIEW"),
    )
    client.tables = SimpleNamespace(get=lambda _name: table)
    view = snapshot_view(client, relation)
    assert view == ViewSnapshot(relation, "view-id", digest_bytes(b"select 1 as id"))
    consumer = replace(
        build,
        model_id="model.probe.consumer",
        inputs_before=(),
        inputs_after=(),
        output=replace(build.output, relation=Relation("hive_metastore", "default", "consumer")),
    )
    claim_owner(store, config, context, consumer.output.relation, job_id=consumer.attempt.job_id)
    assert record_build(store, client, config, context, consumer, successful=True) == "DEFINITION_UNKNOWN"
    consumer = replace(consumer, views_before=(view,), views_after=(view,))
    assert record_build(store, client, config, context, consumer, successful=True) == "RECORDED"
    changed = replace(view, definition_sha256=digest_bytes(b"select 2 as id"))
    assert (
        record_build(store, client, config, context, replace(consumer, views_after=(changed,)), successful=True)
        == "DEFINITION_CHANGED_DURING_BUILD"
    )
    table.view_definition = "select 2 as id"
    monkeypatch.setattr(reuse_runtime, "verify_runtime_identity", lambda *_args: None)

    def unchanged_output(_spark, _client, relation):
        if relation == consumer.output.relation:
            return consumer.output
        raise EvidenceUnavailable("METADATA_UNAVAILABLE")

    monkeypatch.setattr(reuse_runtime, "snapshot_relation", unchanged_output)
    prepared = reuse_runtime.prepare_run(
        store, client, config, build.descriptor, captured[0], captured[1], Attempt("job", "next", "prepare", 0)
    )
    assert prepared["models"][consumer.model_id]["reason"] == "DEFINITION_CHANGED"
    assert prepared["models"][consumer.model_id]["baseline"] is not None
    table.view_definition = None
    with pytest.raises(EvidenceUnavailable, match="DEFINITION_UNKNOWN"):
        snapshot_view(client, relation)


@pytest.fixture
def local_bundle(captured, tmp_path):
    _, _, manifest, provenance = captured
    return write_bundle(prepare_bundle(manifest, provenance, CONFIG), tmp_path / "artifacts")


class Files:
    def __init__(self):
        self.objects = {}
        self.uploads = []

    def upload(self, path, stream, *, overwrite):
        assert overwrite is False
        self.uploads.append(path)
        if path in self.objects:
            raise AlreadyExists("exists")
        self.objects[path] = stream.read()

    def download(self, path):
        return SimpleNamespace(contents=io.BytesIO(self.objects[path]))


def test_publish_is_verified_idempotent_and_downloads_are_rechecked(local_bundle):
    files = Files()
    client = SimpleNamespace(files=files)
    references = publish_artifacts(client, local_bundle, "/Volumes/state/factory/artifacts/release")
    assert len(references) == 3
    assert publish_artifacts(client, local_bundle, "/Volumes/state/factory/artifacts/release") == references
    for name, reference in references.items():
        assert reference == {
            "path": f"/Volumes/state/factory/artifacts/release/{name}",
            "sha256": digest_bytes(next(path for path in local_bundle if path.name == name).read_bytes()),
        }
        assert download_artifact(client, **reference) == files.objects[reference["path"]]
        files.objects[reference["path"]] = b"changed after publication"
        with pytest.raises(ValueError, match="digest mismatch"):
            download_artifact(client, **reference)


@pytest.mark.parametrize("damage", ["partial", "missing", "wrong-digest"])
def test_invalid_local_bundle_publishes_nothing(local_bundle, damage):
    path = local_bundle[0]
    if damage == "partial":
        path.write_bytes(path.read_bytes()[:20])
    elif damage == "missing":
        path.unlink()
    else:
        renamed = path.with_name(f"manifest_{'0' * 64}.json")
        path.rename(renamed)
        local_bundle = (renamed, *local_bundle[1:])
    files = Files()
    with pytest.raises((ValueError, FileNotFoundError)):
        publish_artifacts(SimpleNamespace(files=files), local_bundle, "/Volumes/state/factory/artifacts")
    assert not files.uploads


@pytest.mark.parametrize("existing", [False, True])
def test_corrupt_remote_content_never_returns_references(local_bundle, monkeypatch, existing):
    files = Files()
    root = "/Volumes/state/factory/artifacts"
    if existing:
        files.objects[f"{root}/{local_bundle[0].name}"] = b"different content"
    else:
        upload = files.upload

        def partial_upload(path, stream, *, overwrite):
            upload(path, stream, overwrite=overwrite)
            files.objects[path] = b"partial upload"

        monkeypatch.setattr(files, "upload", partial_upload)
    with pytest.raises(ValueError, match="digest mismatch"):
        publish_artifacts(SimpleNamespace(files=files), local_bundle, root)


def test_late_upload_failure_does_not_publish_a_descriptor(local_bundle, monkeypatch):
    files = Files()
    upload = files.upload

    def fail_provenance(path, stream, *, overwrite):
        if "/provenance_" in path:
            raise OSError("upload unavailable")
        upload(path, stream, overwrite=overwrite)

    monkeypatch.setattr(files, "upload", fail_provenance)
    with pytest.raises(OSError, match="upload unavailable"):
        publish_artifacts(SimpleNamespace(files=files), local_bundle, "/Volumes/state/factory/artifacts")
    assert files.objects
    assert not any("/descriptor_" in path for path in files.objects)


def test_descriptor_refuses_incompatible_runtime_and_format(local_bundle):
    content = next(path for path in local_bundle if path.name.startswith("descriptor_")).read_bytes()
    descriptor = read_descriptor(content)
    for patch in ({"factory_version": "other"}, {"format_version": True}, {"format_version": 2}):
        with pytest.raises(ValueError, match="reuse descriptor"):
            read_descriptor(canonical_json(descriptor | patch).encode())
    assert json.loads(content) == descriptor


@pytest.mark.parametrize("change", ["version", "model", "reference"])
def test_invalid_descriptor_publishes_nothing(local_bundle, change):
    descriptor_path = next(path for path in local_bundle if path.name.startswith("descriptor_"))
    descriptor = json.loads(descriptor_path.read_bytes())
    if change == "version":
        descriptor["factory_version"] = "other"
    elif change == "model":
        descriptor["models"] = []
    else:
        descriptor["manifest"] = f"manifest_{'0' * 64}.json"
    content = canonical_json(descriptor).encode()
    changed = descriptor_path.with_name(f"descriptor_{digest_bytes(content)}.json")
    changed.write_bytes(content)
    paths = tuple(changed if path == descriptor_path else path for path in local_bundle)
    files = Files()
    with pytest.raises(ValueError, match="reuse (descriptor|bundle)"):
        publish_artifacts(SimpleNamespace(files=files), paths, "/Volumes/state/factory/artifacts")
    assert not files.uploads


@pytest.mark.parametrize(
    "root",
    [
        "/tmp/artifacts",
        "/Volumes/state/factory",
        "/Volumes/other/factory/artifacts",
        "/Volumes/state/factory/artifacts/../escape",
    ],
)
def test_invalid_or_mismatched_volume_publishes_nothing(local_bundle, root):
    files = Files()
    with pytest.raises(ValueError, match="reuse artifacts"):
        publish_artifacts(SimpleNamespace(files=files), local_bundle, root)
    assert not files.uploads


def test_local_immutable_files_reject_corruption_and_symlinks(local_bundle, captured, tmp_path):
    _, _, manifest, provenance = captured
    content = prepare_bundle(manifest, provenance, CONFIG)
    directory = local_bundle[0].parent
    assert write_bundle(content, directory) == local_bundle
    local_bundle[0].write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="immutable artifact"):
        write_bundle(content, directory)

    local_bundle[0].unlink()
    local_bundle[0].symlink_to(tmp_path / "missing")
    with pytest.raises(ValueError, match="immutable artifact"):
        write_bundle(content, directory)


class Rows:
    def __init__(self, values):
        self.values = [SimpleNamespace(asDict=lambda value=value: value) for value in values]

    def collect(self):
        return self.values

    def first(self):
        return self.values[0] if self.values else None


class StateSQL:
    """Controlled Spark boundary; real SQL is verified by the serverless state probe."""

    def __init__(self):
        self.tables = {}
        self.calls = []
        self.unavailable = False

    def sql(self, statement, args=None):
        self.calls.append((statement, args))
        if self.unavailable:
            raise OSError("state unavailable")
        table = re.search(r"`[^`]*`\.`[^`]*`\.`[^`]*`", statement)[0]
        rows = self.tables.setdefault(table, {})
        if statement.startswith("MERGE"):
            rows.setdefault(args["key"], dict(args))
            return Rows([])
        if statement.startswith("CREATE"):
            return Rows([])
        selected = list(rows.values())
        if "get_json_object" in statement:
            return Rows(
                [row for row in selected if json.loads(row["evidence"]).get("lineage_key") == args["lineage_key"]]
            )
        if "key" in args:
            selected = [row for row in selected if row["key"] == args["key"]]
        else:
            selected = [row for row in selected if all(row[key] == value for key, value in args.items())]
            selected.sort(key=lambda row: row["completed_at"], reverse=True)
        return Rows(selected[:2])


@pytest.fixture
def evidence(local_bundle, captured, monkeypatch):
    monkeypatch.setenv("DBT_ACCESS_TOKEN", "dummy-parse-only")
    config = ReuseConfig.from_dict(CONFIG)
    context = read_provenance(captured[3], captured[2]).context
    files = Files()
    client = SimpleNamespace(files=files, config=SimpleNamespace(host="https://example.databricks.com"))
    refs = publish_artifacts(client, local_bundle, "/Volumes/state/factory/artifacts")
    descriptor = next(ref for name, ref in refs.items() if name.startswith("descriptor_"))
    source = RelationSnapshot(Relation("raw", "sales", "orders"), "source-uc", "source-delta", 0)
    output = RelationSnapshot(Relation("scratch", "factory", "orders"), "output-uc", "output-delta", 0)
    # The output mapping comes from the actual dbt parse, not an assumed manifest shape.
    model = next(
        model
        for model in read_descriptor(download_artifact(client, **descriptor))["models"]
        if model["unique_id"] == "model.probe.orders"
    )
    output = replace(output, relation=Relation.from_dict(model["output"], "output"))
    build = BuildEvidence(
        "model.probe.orders",
        Attempt("job", "run", "orders", 0),
        "2026-10-27T10:00:00+00:00",
        descriptor,
        context.digest,
        (source,),
        (source,),
        output,
    )
    return StateSQL(), client, config, context, build


def test_state_is_explicit_and_builds_are_attempt_idempotent(evidence):
    spark, client, config, context, build = evidence
    initialize_state(spark, config)
    assert len(spark.tables) == 3
    claim_owner(spark, config, context, build.output.relation, job_id="job")
    claim_owner(spark, config, context, build.output.relation, job_id="job")
    for _ in range(2):
        assert record_build(spark, client, config, context, build, successful=True) == "RECORDED"
    actual = read_build(spark, client, config, context, build.model_id, build.output, job_id="job")
    assert actual["build"] == json.loads(canonical_json(asdict(build)))
    assert actual["build"]["inputs_before"][0]["version"] == 0
    assert actual["build"]["completed_at"] == build.completed_at
    assert sum(len(rows) for rows in spark.tables.values()) == 2
    with pytest.raises(ValueError, match="conflicting.*attempt"):
        record_build(
            spark, client, config, context, replace(build, completed_at="2026-10-27T11:00:00+00:00"), successful=True
        )
    assert sum(len(rows) for rows in spark.tables.values()) == 2


@pytest.mark.parametrize(
    "field,value",
    [("version", 1), ("uc_id", "new"), ("delta_id", "new"), ("relation", Relation("other", "schema", "orders"))],
)
def test_live_output_must_match_the_build(evidence, field, value):
    spark, client, config, context, build = evidence
    claim_owner(spark, config, context, build.output.relation, job_id="job")
    assert record_build(spark, client, config, context, build, successful=True) == "RECORDED"
    changed = replace(build.output, **{field: value})
    if field == "relation":
        claim_owner(spark, config, context, changed.relation, job_id="job")
    assert read_build(spark, client, config, context, build.model_id, changed, job_id="job") is None


@pytest.mark.parametrize("change", ["version", "identity", "missing", "failed", "output"])
def test_unstable_or_unsuccessful_builds_publish_no_evidence(evidence, change):
    spark, client, config, context, build = evidence
    source = build.inputs_before[0]
    if change == "version":
        build = replace(build, inputs_after=(replace(source, version=1),))
    elif change == "identity":
        build = replace(build, inputs_after=(replace(source, uc_id="new"),))
    elif change == "missing":
        build = replace(build, inputs_before=())
    elif change == "output":
        build = replace(build, output=None)
    reason = record_build(spark, client, config, context, build, successful=change != "failed")
    assert reason in {"INPUT_CHANGED_DURING_BUILD", "INPUT_UNKNOWN", "BUILD_FAILED", "OUTPUT_UNKNOWN"}
    assert not spark.calls


def test_ownership_conflicts_and_state_outages_are_distinct(evidence):
    spark, client, config, context, build = evidence
    claim_owner(spark, config, context, build.output.relation, job_id="job")
    for other_config, job in (
        (replace(config, owner_key="other"), "job"),
        (config, "other-job"),
        (replace(config, environment="other"), "job"),
    ):
        other_context = type(context).from_dict(
            json.loads(context.canonical_json) | {"environment": other_config.environment}
        )
        with pytest.raises(ValueError, match="conflicting.*owner"):
            claim_owner(spark, other_config, other_context, build.output.relation, job_id=job)
    spark.unavailable = True
    assert record_build(spark, client, config, context, build, successful=True) == "STATE_UNAVAILABLE"
    assert (
        record_decision(
            spark,
            config,
            context,
            build.model_id,
            build.attempt,
            reason="STATE_UNAVAILABLE",
            observed_at=build.completed_at,
            evidence={},
            durations={},
        )
        == "STATE_UNAVAILABLE"
    )
    with pytest.raises(EvidenceUnavailable, match="STATE_UNAVAILABLE"):
        read_build(spark, client, config, context, build.model_id, build.output, job_id="job")


def test_decisions_never_update_builds_and_keep_original_completion(evidence):
    spark, client, config, context, build = evidence
    claim_owner(spark, config, context, build.output.relation, job_id="job")
    record_build(spark, client, config, context, build, successful=True)
    for _ in range(2):
        assert (
            record_decision(
                spark,
                config,
                context,
                build.model_id,
                build.attempt,
                reason="REUSED",
                observed_at="2026-10-28T10:00:00+00:00",
                evidence={"quoted": "a'b"},
                durations={"metadata_seconds": 1.2},
            )
            == "RECORDED"
        )
    actual = read_build(spark, client, config, context, build.model_id, build.output, job_id="job")
    assert actual["build"]["completed_at"] == build.completed_at
    assert sum(len(rows) for rows in spark.tables.values()) == 3
    assert all("a'b" not in sql for sql, _ in spark.calls)
    assert not any(sql.startswith(("UPDATE", "DELETE")) for sql, _ in spark.calls)


def test_unavailable_write_readback_does_not_invent_a_conflict_after_success(evidence, monkeypatch):
    sql, client, config, context, build = evidence
    claim_owner(sql, config, context, build.output.relation, job_id="job")
    execute = sql.sql

    def unavailable_readback(statement, args=None):
        if statement.startswith("SELECT") and "factory_reuse_builds" in statement:
            return Rows([])
        return execute(statement, args)

    monkeypatch.setattr(sql, "sql", unavailable_readback)
    assert record_build(sql, client, config, context, build, successful=True) == "STATE_UNAVAILABLE"


@pytest.mark.parametrize("missing", [False, True])
def test_corrupt_or_missing_comparison_artifacts_do_not_produce_a_baseline(evidence, missing):
    spark, client, config, context, build = evidence
    claim_owner(spark, config, context, build.output.relation, job_id="job")
    record_build(spark, client, config, context, build, successful=True)
    if missing:
        del client.files.objects[build.descriptor["path"]]
        with pytest.raises(EvidenceUnavailable, match="ARTIFACT_UNAVAILABLE"):
            read_build(spark, client, config, context, build.model_id, build.output, job_id="job")
        assert record_build(spark, client, config, context, build, successful=True) == "ARTIFACT_UNAVAILABLE"
    else:
        client.files.objects[build.descriptor["path"]] = b"changed"
        with pytest.raises(ValueError, match="digest mismatch"):
            read_build(spark, client, config, context, build.model_id, build.output, job_id="job")


def test_missing_declared_inputs_or_changed_context_cannot_publish(evidence):
    sql, client, config, context, build = evidence
    assert (
        record_build(sql, client, config, context, replace(build, inputs_before=(), inputs_after=()), successful=True)
        == "INPUT_UNKNOWN"
    )
    other = ExecutionContext.from_dict(json.loads(context.canonical_json) | {"principal": "other@example.com"})
    assert record_build(sql, client, config, other, build, successful=True) == "CONTEXT_CHANGED"
    assert not sql.calls


def test_other_execution_context_can_inspect_but_cannot_claim_the_baseline(evidence):
    sql, client, config, context, build = evidence
    claim_owner(sql, config, context, build.output.relation, job_id="job")
    record_build(sql, client, config, context, build, successful=True)
    other = ExecutionContext.from_dict(json.loads(context.canonical_json) | {"principal": "other@example.com"})
    baseline = read_build(sql, client, config, other, build.model_id, build.output, job_id="job")
    assert baseline["build"]["context_sha256"] == context.digest != other.digest
    for field in ("workspace_id", "metastore_id"):
        wrong_scope = ExecutionContext.from_dict(json.loads(context.canonical_json) | {field: "other"})
        with pytest.raises(ValueError, match="conflicting.*owner"):
            read_build(sql, client, config, wrong_scope, build.model_id, build.output, job_id="job")


@pytest.mark.parametrize("damage", ["missing-field", "timestamp", "version", "attempt", "partial-input"])
def test_malformed_builds_are_unusable(evidence, damage):
    sql, client, config, context, build = evidence
    claim_owner(sql, config, context, build.output.relation, job_id="job")
    record_build(sql, client, config, context, build, successful=True)
    row = next(
        row for table, rows in sql.tables.items() if table.endswith("`factory_reuse_builds`") for row in rows.values()
    )
    payload = json.loads(row["evidence"])
    if damage == "missing-field":
        del payload["build"]["output"]
    elif damage == "timestamp":
        payload["build"]["completed_at"] = 123
    elif damage == "version":
        payload["build"]["inputs_before"][0]["version"] = True
    elif damage == "attempt":
        payload["build"]["attempt"]["attempt_number"] = 99
    else:
        payload["build"]["inputs_before"] = []
    row["evidence"] = canonical_json(payload)
    with pytest.raises(EvidenceUnavailable, match="BUILD_INVALID"):
        read_build(sql, client, config, context, build.model_id, build.output, job_id="job")


def test_identifier_components_are_quoted_without_splitting():
    from databricks_dbt_factory.reuse_runtime import quote_relation  # pylint: disable=import-outside-toplevel

    assert quote_relation(Relation("cat", "sch`ema", "a.b'c")) == "`cat`.`sch``ema`.`a.b'c`"


def test_owner_names_are_case_insensitive_without_changing_evidence(evidence):
    sql, _, config, context, build = evidence
    claim_owner(sql, config, context, build.output.relation, job_id="job")
    upper = Relation(*(value.upper() for value in asdict(build.output.relation).values()))
    claim_owner(sql, config, context, upper, job_id="job")
    assert sum(len(rows) for rows in sql.tables.values()) == 1


@pytest.mark.parametrize("damage", [None, "uc-id", "delta-id", "name", "non-delta", "unreadable", "version"])
def test_relation_snapshots_reject_mixed_or_unavailable_observations(damage):
    relation = Relation("catalog", "schema", "orders")
    before = {"table_id": "uc-id", "catalog_name": "catalog", "schema_name": "schema", "name": "orders"}
    after = dict(before)
    details = [{"format": "delta", "id": "delta-id"}, {"format": "delta", "id": "delta-id"}]
    if damage == "uc-id":
        after["table_id"] = "new"
    elif damage == "delta-id":
        details[1]["id"] = "new"
    elif damage == "name":
        after["name"] = "renamed"
    elif damage == "non-delta":
        details[0]["format"] = "parquet"
    tables = iter([SimpleNamespace(**before), SimpleNamespace(**after)])
    client = SimpleNamespace(tables=SimpleNamespace(get=lambda _name: next(tables)))
    commands = []

    def sql(statement):
        commands.append(statement)
        if damage == "unreadable":
            raise OSError("metadata denied")
        if "HISTORY" in statement:
            return Rows([{"version": True if damage == "version" else 3}])
        return Rows([details.pop(0)])

    if damage:
        with pytest.raises(EvidenceUnavailable):
            snapshot_relation(SimpleNamespace(sql=sql), client, relation)
    else:
        assert snapshot_relation(SimpleNamespace(sql=sql), client, relation) == RelationSnapshot(
            relation, "uc-id", "delta-id", 3
        )
        assert len(commands) == 3


@pytest.fixture
def lineage_model(evidence):
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel
    from databricks_dbt_factory.reuse import describe_models  # pylint: disable=import-outside-toplevel

    _store, client, config, context, build = evidence
    manifest = reuse_runtime.load_deployment(client, config, build.descriptor)[1]
    return next(model for model in describe_models(json.loads(manifest), context) if model.unique_id == build.model_id)


@pytest.fixture
def lineage_observations(evidence, monkeypatch):
    store = evidence[0]
    observations = {"rows": [], "targets": [], "unavailable": False}
    original_sql = store.sql

    def sql(statement, args=None):
        if "`system`.`access`.`table_lineage`" in statement:
            assert "event_date >=" in statement
            observations["targets"].append(args)
            if observations["unavailable"]:
                raise OSError("lineage unavailable")
            return Rows(observations["rows"])
        return original_sql(statement, args=args)

    monkeypatch.setattr(store, "sql", sql)
    return observations


@pytest.mark.parametrize("scenario", ["hidden", "declared", "historical", "empty", "unavailable", "path"])
def test_lineage_blocks_undeclared_inputs_and_retains_history(evidence, lineage_model, lineage_observations, scenario):
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel

    store, _client, config, context, _build = evidence
    model = lineage_model
    hidden = {"catalog": "raw", "schema": "sales", "identifier": "hidden.with.dot", "path": None}
    observations = lineage_observations["rows"]
    observations[:] = [] if scenario == "empty" else [hidden]
    lineage_observations["unavailable"] = scenario == "unavailable"
    if scenario == "declared":
        model = replace(model, inputs=(*model.inputs, reuse_runtime.Relation("RAW", "SALES", "hidden.with.dot")))
    if scenario == "path":
        observations[:] = [{"catalog": None, "schema": None, "identifier": None, "path": "s3://bucket/raw"}]
    before = model.dependencies
    result = reuse_runtime.prepare_lineage(store, config, context, model, job_id="job")
    assert model.dependencies == before
    assert lineage_observations["targets"] == [{key: value.lower() for key, value in asdict(model.output).items()}]
    assert (
        result["reason"]
        == {
            "hidden": "UNDECLARED_INPUT",
            "historical": "UNDECLARED_INPUT",
            "path": "UNDECLARED_INPUT",
            "declared": None,
            "empty": None,
            "unavailable": "LINEAGE_UNAVAILABLE",
        }[scenario]
    )
    if scenario not in {"historical", "hidden", "path"}:
        return
    observations.clear()
    retained = reuse_runtime.prepare_lineage(store, config, context, model, job_id="job")
    assert retained["reason"] == "UNDECLARED_INPUT"
    assert retained["blockers"] == result["blockers"]
    if scenario != "path":
        reviewed = replace(model, inputs=(*model.inputs, reuse_runtime.Relation("raw", "sales", "hidden.with.dot")))
        assert reuse_runtime.prepare_lineage(store, config, context, reviewed, job_id="job")["reason"] is None
        assert (
            reuse_runtime.prepare_lineage(store, config, context, model, job_id="job")["reason"] == "UNDECLARED_INPUT"
        )


@pytest.mark.parametrize("changed", ["rename", "output-case", "deployment", "job"])
def test_retained_lineage_binding_and_declared_views(evidence, lineage_model, lineage_observations, changed):
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel

    store, _client, config, context, _build = evidence
    hidden = [{"catalog": "raw", "schema": "sales", "identifier": f"hidden_{i}", "path": None} for i in range(4)]
    lineage_observations["rows"][:] = hidden
    assert reuse_runtime.prepare_lineage(store, config, context, lineage_model, job_id="job")["blockers"] == hidden
    lineage_observations["rows"].clear()
    model = lineage_model
    if changed == "rename":
        model = replace(model, unique_id="model.probe.renamed")
    elif changed == "output-case":
        model = replace(model, output=Relation(*(value.upper() for value in asdict(model.output).values())))
    elif changed == "deployment":
        context = ExecutionContext.from_dict(
            json.loads(context.canonical_json) | {"vars": {"release": "new-deployment"}}
        )
    job_id = "other-job" if changed == "job" else "job"
    result = reuse_runtime.prepare_lineage(store, config, context, model, job_id=job_id)
    assert result["blockers"] == ([] if changed == "job" else hidden)
    model = replace(
        model, views=tuple(Relation(value["catalog"], value["schema"], value["identifier"]) for value in hidden)
    )
    assert reuse_runtime.prepare_lineage(store, config, context, model, job_id=job_id)["reason"] is None
    assert lineage_observations["targets"][-4:] == [asdict(view) for view in model.views]
    assert model.dependencies == lineage_model.dependencies


@pytest.mark.parametrize("damage", ["partial", "marker", "write", "view-outage"])
def test_lineage_incomplete_evidence_disables_reuse(evidence, lineage_model, lineage_observations, monkeypatch, damage):
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel

    store, _client, config, context, _build = evidence
    lineage_observations["rows"][:] = [{"catalog": "raw", "schema": "sales", "identifier": "hidden", "path": None}]
    if damage == "partial":
        lineage_observations["rows"][0]["schema"] = None
    elif damage == "write":
        original_sql = store.sql

        def sql(statement, args=None):
            if statement.startswith("MERGE"):
                raise OSError("write denied")
            return original_sql(statement, args=args)

        monkeypatch.setattr(store, "sql", sql)
    else:
        assert (
            reuse_runtime.prepare_lineage(store, config, context, lineage_model, job_id="job")["reason"]
            == "UNDECLARED_INPUT"
        )
        if damage == "marker":
            row = next(iter(next(iter(store.tables.values())).values()))
            row["evidence"] = json.dumps(json.loads(row["evidence"]) | {"kind": "invalid"})
        else:
            lineage_observations["unavailable"] = True
            lineage_model = replace(lineage_model, views=(Relation("raw", "sales", "view"),))
    result = reuse_runtime.prepare_lineage(store, config, context, lineage_model, job_id="job")
    assert (
        result["reason"]
        == {
            "partial": "LINEAGE_INCOMPLETE",
            "marker": "LINEAGE_STATE_INCOMPLETE",
            "write": "STATE_UNAVAILABLE",
            "view-outage": "LINEAGE_UNAVAILABLE",
        }[damage]
    )


def test_preparation_publishes_lineage_veto_without_baseline(evidence, captured, lineage_observations, monkeypatch):
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel

    store, client, config, _context, build = evidence
    lineage_observations["rows"][:] = [{"catalog": "raw", "schema": "sales", "identifier": "hidden", "path": None}]
    monkeypatch.setattr(reuse_runtime, "verify_runtime_identity", lambda *_args: None)
    result = reuse_runtime.prepare_run(
        store,
        client,
        config,
        build.descriptor,
        captured[0],
        captured[1],
        Attempt("job", "run", "prepare", 0),
        model_ids=[build.model_id],
    )
    pointer = reuse_runtime.publish_preparation(client, config, result)
    decision = json.loads(download_artifact(client, **pointer))["models"][build.model_id]
    assert decision["reason"] == "UNDECLARED_INPUT"
    assert decision["baseline"] is None
    assert decision["lineage"]["blockers"] == lineage_observations["rows"]
    assert not result["comparisons"]


def test_lineage_checks_view_inputs_without_adding_dependencies(evidence, lineage_model, monkeypatch):
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel

    store, _client, config, context, _build = evidence
    view = Relation("raw", "sales", "declared_view")
    hidden = {"catalog": "raw", "schema": "sales", "identifier": "hidden", "path": "s3://backing/path"}
    model = replace(lineage_model, views=(view,))
    original_sql = store.sql

    def sql(statement, args=None):
        if "`system`.`access`.`table_lineage`" in statement:
            if args == asdict(view):
                return Rows([hidden])
            return Rows([asdict(view) | {"path": None}])
        return original_sql(statement, args=args)

    monkeypatch.setattr(store, "sql", sql)
    result = reuse_runtime.prepare_lineage(store, config, context, model, job_id="job")
    assert result == {"reason": "UNDECLARED_INPUT", "blockers": [hidden | {"path": None}]}
    assert model.dependencies == lineage_model.dependencies
    reviewed = replace(model, inputs=(*model.inputs, Relation("raw", "sales", "hidden")))
    assert reuse_runtime.prepare_lineage(store, config, context, reviewed, job_id="job")["reason"] is None


def test_known_lineage_blocker_does_not_repeat_state_writes(evidence, lineage_model, lineage_observations):
    from databricks_dbt_factory import reuse_runtime  # pylint: disable=import-outside-toplevel

    store, _client, config, context, _build = evidence
    lineage_observations["rows"][:] = [{"catalog": "raw", "schema": "sales", "identifier": "hidden", "path": None}]
    first = reuse_runtime.prepare_lineage(store, config, context, lineage_model, job_id="job")
    writes = sum(statement.startswith("MERGE") for statement, _args in store.calls)
    assert writes == 1
    assert reuse_runtime.prepare_lineage(store, config, context, lineage_model, job_id="job") == first
    assert sum(statement.startswith("MERGE") for statement, _args in store.calls) == writes


def test_lineage_policy_compares_physical_names_without_case(lineage_model):
    from databricks_dbt_factory.reuse import lineage_reason  # pylint: disable=import-outside-toplevel

    upper = tuple(Relation(*(value.upper() for value in asdict(item).values())) for item in lineage_model.inputs)
    assert upper and upper != lineage_model.inputs
    assert lineage_reason(lineage_model, upper) is None


def test_lineage_does_not_merge_distinct_lowercase_identifiers(lineage_model):
    from databricks_dbt_factory.reuse import lineage_reason  # pylint: disable=import-outside-toplevel

    model = replace(lineage_model, inputs=(Relation("raw", "sales", "straße"),), views=())
    assert lineage_reason(model, (Relation("raw", "sales", "STRASSE"),)) == "UNDECLARED_INPUT"
