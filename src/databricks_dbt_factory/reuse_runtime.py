"""Immutable reuse artifacts. SDK objects are supplied by the caller, never imported offline."""

import io
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from databricks_dbt_factory.__version__ import __version__
from databricks_dbt_factory.file_io import atomic_write_bytes
from databricks_dbt_factory.reuse import (
    ExecutionContext,
    Relation,
    ReuseConfig,
    canonical_json,
    describe_models,
    digest_bytes,
)
from databricks_dbt_factory.reuse_prepare import compare_baselines, read_provenance


def _artifact_digest(name: str, kind: str = "manifest|provenance|descriptor") -> str:
    match = re.fullmatch(rf"(?:{kind})_([0-9a-f]{{64}})\.json", name)
    if match is None:
        raise ValueError(f"reuse artifact: invalid content-addressed filename {name!r}")
    return match[1]


def prepare_bundle(manifest: bytes, provenance: bytes, config: dict) -> dict[str, bytes]:
    """Prepare exact parse artifacts and a canonical descriptor without writing files."""
    settings = ReuseConfig.from_dict(config)
    parsed = read_provenance(provenance, manifest)
    context = json.loads(parsed.context.canonical_json)
    if (settings.project_id, settings.environment) != (context["project_id"], context["environment"]):
        raise ValueError("reuse descriptor: provenance project/environment mismatch")
    manifest_name = f"manifest_{digest_bytes(manifest)}.json"
    provenance_name = f"provenance_{digest_bytes(provenance)}.json"
    descriptor = canonical_json(
        {
            "format_version": 1,
            "factory_version": __version__,
            "manifest": manifest_name,
            "provenance": provenance_name,
            "config": config,
            "models": [asdict(model) for model in describe_models(json.loads(manifest), parsed.context)],
        }
    ).encode()
    return {
        manifest_name: manifest,
        provenance_name: provenance,
        f"descriptor_{digest_bytes(descriptor)}.json": descriptor,
    }


def read_descriptor(content: bytes) -> dict:
    """Reject a descriptor from an incompatible format or Factory runtime."""
    value = json.loads(content)
    fields = {"format_version", "factory_version", "manifest", "provenance", "config", "models"}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("reuse descriptor: invalid fields")
    if (
        not isinstance(value["format_version"], int)
        or isinstance(value["format_version"], bool)
        or value["format_version"] != 1
    ):
        raise ValueError("reuse descriptor: unsupported format_version")
    if value["factory_version"] != __version__:
        raise ValueError(f"reuse descriptor: install matching Factory wheel {value['factory_version']!r}")
    ReuseConfig.from_dict(value["config"])
    for kind in ("manifest", "provenance"):
        if not isinstance(value[kind], str):
            raise ValueError(f"reuse descriptor: invalid {kind} reference")
        _artifact_digest(value[kind], kind)
    if not isinstance(value["models"], list):
        raise ValueError("reuse descriptor: models must be a list")
    return value


def _validate_bundle(bundle: dict[str, bytes]) -> None:
    for name, content in bundle.items():
        if digest_bytes(content) != _artifact_digest(name):
            raise ValueError(f"reuse artifact digest mismatch: {name}")
    descriptors = [name for name in bundle if name.startswith("descriptor_")]
    if len(descriptors) != 1:
        raise ValueError("reuse bundle: exactly one descriptor is required")
    descriptor = read_descriptor(bundle[descriptors[0]])
    if set(bundle) != {descriptors[0], descriptor["manifest"], descriptor["provenance"]}:
        raise ValueError("reuse bundle: missing or unexpected artifact")
    expected = prepare_bundle(bundle[descriptor["manifest"]], bundle[descriptor["provenance"]], descriptor["config"])
    if bundle != expected:
        raise ValueError("reuse descriptor: does not match captured manifest/provenance")


def write_bundle(bundle: dict[str, bytes], directory: Path) -> tuple[Path, ...]:
    """Publish local immutable files. An incomplete bundle never yields a reference."""
    _validate_bundle(bundle)
    if directory.is_symlink():
        raise ValueError(f"immutable artifact directory must not be a symlink: {directory}")
    paths = tuple(directory / name for name in bundle)
    for path in paths:
        if path.is_symlink() or (path.exists() and (not path.is_file() or path.read_bytes() != bundle[path.name])):
            raise ValueError(f"immutable artifact conflicts with existing content: {path}")
    for path in paths:
        if not path.exists():
            atomic_write_bytes(path, bundle[path.name], 0o644)
    return paths


def download_artifact(workspace_client, path: str, sha256: str) -> bytes:
    """Verify the downloaded bytes immediately before using comparison/evidence artifacts."""
    with workspace_client.files.download(path).contents as stream:
        content = stream.read()
    if digest_bytes(content) != sha256:
        raise ValueError(f"reuse artifact digest mismatch: {path}")
    return content


def publish_artifacts(workspace_client, local_bundle: tuple[Path, ...], volume_path: str) -> dict[str, dict[str, str]]:
    """Upload immutable inputs first and descriptor last; return only after all readbacks pass.

    Failed batches may leave unreferenced immutable files. No cross-object transaction is assumed.
    """
    from databricks.sdk.errors import AlreadyExists  # pylint: disable=import-outside-toplevel

    root = PurePosixPath(volume_path)
    if (
        len(root.parts) < 5
        or root.parts[:2] != ("/", "Volumes")
        or ".." in root.parts
        or str(root) != volume_path.rstrip("/")
    ):
        raise ValueError("reuse artifacts require an absolute /Volumes/catalog/schema/volume path")
    if len(local_bundle) != 3 or any(path.is_symlink() for path in local_bundle):
        raise ValueError("reuse bundle: expected three regular non-symlink artifacts")
    bundle = {path.name: path.read_bytes() for path in local_bundle}
    _validate_bundle(bundle)
    descriptor = read_descriptor(next(content for name, content in bundle.items() if name.startswith("descriptor_")))
    settings = ReuseConfig.from_dict(descriptor["config"])
    if root.parts[2:5] != settings.artifact_volume:
        raise ValueError("reuse artifacts: publication volume differs from descriptor config")
    references = {}
    for name in sorted(bundle, key=lambda name: (name.startswith("descriptor_"), name)):
        path = str(root / name)
        try:
            workspace_client.files.upload(path, io.BytesIO(bundle[name]), overwrite=False)
        except AlreadyExists:
            pass  # Existing content is accepted only after a fresh verified download.
        sha256 = _artifact_digest(name)
        download_artifact(workspace_client, path, sha256)
        references[name] = {"path": path, "sha256": sha256}
    return references


class EvidenceUnavailable(Exception):
    """Expected optimization failure. The runner reports the reason and executes dbt."""


@dataclass(frozen=True)
class RelationSnapshot:
    relation: Relation
    uc_id: str
    delta_id: str
    version: int

    def __post_init__(self):
        Relation.from_dict(asdict(self.relation), "relation evidence")
        if any(not isinstance(value, str) or not value.strip() for value in (self.uc_id, self.delta_id)):
            raise ValueError("relation evidence requires UC and Delta identities")
        if not isinstance(self.version, int) or isinstance(self.version, bool) or self.version < 0:
            raise ValueError("relation evidence requires a nonnegative integer version")

    @classmethod
    def from_dict(cls, value):
        return cls(
            Relation.from_dict(value["relation"], "relation evidence"),
            **{key: item for key, item in value.items() if key != "relation"},
        )


@dataclass(frozen=True)
class ViewSnapshot:
    relation: Relation
    uc_id: str
    definition_sha256: str

    def __post_init__(self):
        Relation.from_dict(asdict(self.relation), "view evidence")
        if not isinstance(self.uc_id, str) or not self.uc_id.strip():
            raise ValueError("view evidence requires a UC identity")
        if not isinstance(self.definition_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", self.definition_sha256):
            raise ValueError("view evidence requires a definition digest")

    @classmethod
    def from_dict(cls, value):
        return cls(Relation.from_dict(value["relation"], "view evidence"), value["uc_id"], value["definition_sha256"])


def snapshot_view(workspace_client, relation: Relation) -> ViewSnapshot:
    """Only dbt-declared views reach this path; do not infer dependencies from their SQL."""
    name = ".".join((relation.catalog, relation.schema, relation.identifier))
    try:
        before = workspace_client.tables.get(name)
        after = workspace_client.tables.get(name)
    except Exception as error:
        raise EvidenceUnavailable("METADATA_UNAVAILABLE") from error
    expected = (relation.catalog.lower(), relation.schema.lower(), relation.identifier.lower())
    observations = []
    for table in (before, after):
        if (
            tuple((getattr(table, key, None) or "").lower() for key in ("catalog_name", "schema_name", "name"))
            != expected
            or getattr(getattr(table, "table_type", None), "value", None) != "VIEW"
            or not isinstance(table.view_definition, str)
            or not table.view_definition.strip()
        ):
            raise EvidenceUnavailable("DEFINITION_UNKNOWN")
        try:
            observations.append(ViewSnapshot(relation, table.table_id, digest_bytes(table.view_definition.encode())))
        except ValueError as error:
            raise EvidenceUnavailable("DEFINITION_UNKNOWN") from error
    if observations[0] != observations[1]:
        raise EvidenceUnavailable("DEFINITION_CHANGED")
    return observations[0]


def verify_runtime_identity(spark, workspace_client, context: ExecutionContext) -> None:
    """Authenticate the execution identity instead of trusting notebook parameters."""
    expected = json.loads(context.canonical_json)
    try:
        observed = {
            "workspace_id": str(workspace_client.get_workspace_id()),
            "metastore_id": workspace_client.metastores.summary().metastore_id,
            "principal": workspace_client.current_user.me().user_name,
        }
        sql_principal = spark.sql("SELECT current_user()").first()[0]
    except Exception as error:
        raise EvidenceUnavailable("CONTEXT_UNKNOWN") from error
    if any(value != expected[key] for key, value in observed.items()) or sql_principal != expected["principal"]:
        raise EvidenceUnavailable("CONTEXT_CHANGED")


def publish_preparation(workspace_client, config: ReuseConfig, result: dict) -> dict[str, str]:
    """Immutable run evidence; only this small verified pointer is placed in a task value."""
    from databricks.sdk.errors import AlreadyExists  # pylint: disable=import-outside-toplevel

    if result.get("format_version") != 1:
        raise ValueError("preparation requires format_version 1")
    Attempt(**result["attempt"])
    content = canonical_json(result).encode()
    digest = digest_bytes(content)
    path = "/Volumes/" + "/".join(config.artifact_volume) + f"/preparation_{digest}.json"
    try:
        workspace_client.files.upload(path, io.BytesIO(content), overwrite=False)
    except AlreadyExists:
        pass
    except OSError as error:
        raise EvidenceUnavailable("ARTIFACT_UNAVAILABLE") from error
    try:
        download_artifact(workspace_client, path, digest)
    except OSError as error:
        raise EvidenceUnavailable("ARTIFACT_UNAVAILABLE") from error
    return {"path": path, "sha256": digest}


@dataclass(frozen=True)
class Attempt:
    job_id: str
    run_id: str
    task_key: str
    attempt_number: int

    def __post_init__(self):
        if any(not isinstance(value, str) or not value.strip() for value in (self.job_id, self.run_id, self.task_key)):
            raise ValueError("attempt requires job/run/task identity")
        if not isinstance(self.attempt_number, int) or isinstance(self.attempt_number, bool) or self.attempt_number < 0:
            raise ValueError("attempt_number must be a nonnegative integer")


@dataclass(frozen=True)
class BuildEvidence:
    model_id: str
    attempt: Attempt
    completed_at: str
    descriptor: dict[str, str]
    context_sha256: str
    inputs_before: tuple[RelationSnapshot, ...]
    inputs_after: tuple[RelationSnapshot, ...]
    output: RelationSnapshot | None
    views_before: tuple[ViewSnapshot, ...] = ()
    views_after: tuple[ViewSnapshot, ...] = ()

    def __post_init__(self):
        if not isinstance(self.model_id, str) or not self.model_id.strip():
            raise ValueError("build evidence requires a model_id")
        _timestamp(self.completed_at)
        if not isinstance(self.context_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", self.context_sha256):
            raise ValueError("build evidence requires a context digest")

    @classmethod
    def from_dict(cls, value):
        return cls(
            **(
                value
                | {
                    "attempt": Attempt(**value["attempt"]),
                    "inputs_before": tuple(RelationSnapshot.from_dict(item) for item in value["inputs_before"]),
                    "inputs_after": tuple(RelationSnapshot.from_dict(item) for item in value["inputs_after"]),
                    "output": RelationSnapshot.from_dict(value["output"]) if value["output"] is not None else None,
                    "views_before": tuple(ViewSnapshot.from_dict(item) for item in value.get("views_before", [])),
                    "views_after": tuple(ViewSnapshot.from_dict(item) for item in value.get("views_after", [])),
                }
            )
        )


def quote_relation(relation: Relation) -> str:
    return ".".join(
        "`" + part.replace("`", "``") + "`" for part in (relation.catalog, relation.schema, relation.identifier)
    )


def snapshot_relation(spark, workspace_client, relation: Relation) -> RelationSnapshot:
    """Bracket the version read with UC and Delta identities; never inspect Delta log files."""
    name = ".".join((relation.catalog, relation.schema, relation.identifier))
    quoted = quote_relation(relation)
    try:
        uc_before = workspace_client.tables.get(name)
        before = spark.sql(f"DESCRIBE DETAIL {quoted}").first().asDict()
        history = spark.sql(f"DESCRIBE HISTORY {quoted} LIMIT 1").first().asDict()
        after = spark.sql(f"DESCRIBE DETAIL {quoted}").first().asDict()
        uc_after = workspace_client.tables.get(name)
    except Exception as error:  # External metadata reads may fail without preventing ordinary dbt execution.
        raise EvidenceUnavailable("METADATA_UNAVAILABLE") from error
    if before.get("format") != "delta" or after.get("format") != "delta":
        raise EvidenceUnavailable("NON_DELTA")
    expected = tuple(part.lower() for part in (relation.catalog, relation.schema, relation.identifier))
    names = []
    for table in (uc_before, uc_after):
        names.append(
            tuple((getattr(table, key, None) or "").lower() for key in ("catalog_name", "schema_name", "name"))
        )
    if (
        any(name != expected for name in names)
        or uc_before.table_id != uc_after.table_id
        or before.get("id") != after.get("id")
    ):
        raise EvidenceUnavailable("RELATION_CHANGED")
    try:
        return RelationSnapshot(relation, uc_before.table_id, before.get("id"), history.get("version"))
    except ValueError as error:
        raise EvidenceUnavailable("RELATION_UNKNOWN") from error


def _state_table(config: ReuseConfig, name: str) -> str:
    return quote_relation(Relation(config.state_catalog, config.state_schema, f"factory_reuse_{name}"))


def initialize_state(spark, config: ReuseConfig) -> None:
    """Explicit setup in an existing UC schema. Runtime model tasks never create state tables."""
    for name in ("owners", "builds", "decisions"):
        columns = "key STRING NOT NULL, evidence STRING NOT NULL"
        if name == "builds":
            columns += ", scope STRING NOT NULL, model_id STRING NOT NULL, output_sha256 STRING NOT NULL, completed_at TIMESTAMP NOT NULL"
        spark.sql(f"CREATE TABLE IF NOT EXISTS {_state_table(config, name)} ({columns}) USING DELTA").collect()


def _state_rows(spark, statement: str, values: dict) -> list[dict]:
    try:
        return [row.asDict() for row in spark.sql(statement, args=values).collect()]
    except Exception as error:  # Restrict the fallback to the Spark storage boundary.
        raise EvidenceUnavailable("STATE_UNAVAILABLE") from error


def _insert_immutable(spark, table: str, key: str, evidence: dict, **projections) -> None:
    # ponytail: serialized owning jobs; concurrent claims require a separate locking design.
    payload = canonical_json(evidence)
    values = {"key": key, "evidence": payload} | projections
    columns = ", ".join(f":{column} AS {column}" for column in values)
    _state_rows(
        spark,
        f"MERGE INTO {table} AS target USING (SELECT {columns}) AS source "
        "ON target.key = source.key WHEN NOT MATCHED THEN INSERT *",
        values,
    )
    rows = _state_rows(spark, f"SELECT evidence FROM {table} WHERE key = :key LIMIT 2", {"key": key})
    if not rows:
        raise EvidenceUnavailable("STATE_UNAVAILABLE")
    if len(rows) != 1 or rows[0]["evidence"] != payload:
        raise ValueError(f"conflicting owner or attempt evidence at {table}; never overwrite an existing record")


def _scope(config: ReuseConfig, context: ExecutionContext, job_id: str) -> dict:
    value = json.loads(context.canonical_json)
    if (config.project_id, config.environment) != (value["project_id"], value["environment"]):
        raise ValueError("reuse context project/environment differs from config")
    if not isinstance(job_id, str) or not job_id.strip():
        raise ValueError("reuse scope requires a job_id")
    return {
        "project_id": config.project_id,
        "environment": config.environment,
        "owner_key": config.owner_key,
        "workspace_id": value["workspace_id"],
        "metastore_id": value["metastore_id"],
        "job_id": job_id,
    }


def _digest(value) -> str:
    return digest_bytes(canonical_json(value).encode())


def _owner_key(output: Relation) -> str:
    return _digest([part.lower() for part in (output.catalog, output.schema, output.identifier)])


def claim_owner(spark, config: ReuseConfig, context: ExecutionContext, output: Relation, *, job_id: str) -> None:
    """One logical owner and deployed job per physical name. This registry is not a lock."""
    _insert_immutable(
        spark,
        _state_table(config, "owners"),
        _owner_key(output),
        {
            "format_version": 1,
            "scope": _scope(config, context, job_id),
            "output": {key: value.lower() for key, value in asdict(output).items()},
        },
    )


def _assert_owner(spark, config, scope, output):
    rows = _state_rows(
        spark,
        f"SELECT evidence FROM {_state_table(config, 'owners')} WHERE key = :key LIMIT 2",
        {"key": _owner_key(output)},
    )
    if not rows:
        raise EvidenceUnavailable("OWNER_UNKNOWN")
    expected = canonical_json(
        {"format_version": 1, "scope": scope, "output": {key: value.lower() for key, value in asdict(output).items()}}
    )
    if len(rows) != 1 or rows[0]["evidence"] != expected:
        raise ValueError(f"conflicting declared owner for {quote_relation(output)}")


def _timestamp(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("reuse evidence timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("reuse evidence timestamp requires a timezone")
    return parsed.astimezone(timezone.utc)


def _descriptor_path(config, reference):
    if (
        not isinstance(reference, dict)
        or set(reference) != {"path", "sha256"}
        or not isinstance(reference["sha256"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", reference["sha256"])
    ):
        raise ValueError("reuse descriptor requires a path and SHA-256")
    path = PurePosixPath(reference["path"])
    if path.parts[:5] != ("/", "Volumes", *config.artifact_volume) or ".." in path.parts:
        raise ValueError("reuse descriptor volume differs from config")
    if _artifact_digest(path.name, "descriptor") != reference["sha256"]:
        raise ValueError("reuse descriptor filename/digest mismatch")
    return path


def load_deployment(workspace_client, config, reference, *, deployments=None) -> tuple[dict, bytes, bytes]:
    """Read immutable artifacts; only preparation shares a verified, attempt-local cache."""
    path = _descriptor_path(config, reference)
    key = (config, str(path), reference["sha256"])
    if deployments is not None and key in deployments:
        return deployments[key]

    def download(path, sha256):
        try:
            return download_artifact(workspace_client, str(path), sha256)
        except ValueError:
            raise  # A known digest mismatch is a deployment error, never missing evidence.
        except Exception as error:
            raise EvidenceUnavailable("ARTIFACT_UNAVAILABLE") from error

    descriptor = read_descriptor(download(path, reference["sha256"]))
    if ReuseConfig.from_dict(descriptor["config"]) != config:
        raise ValueError("reuse descriptor config mismatch")
    bundle = {path.name: canonical_json(descriptor).encode()}
    for kind in ("manifest", "provenance"):
        name = descriptor[kind]
        bundle[name] = download(path.parent / name, _artifact_digest(name, kind))
    _validate_bundle(bundle)
    result = descriptor, bundle[descriptor["manifest"]], bundle[descriptor["provenance"]]
    if deployments is not None:
        deployments[key] = result
    return result


def _verified_descriptor(workspace_client, config, reference, deployments=None):
    descriptor, manifest, provenance = load_deployment(workspace_client, config, reference, deployments=deployments)
    return descriptor, read_provenance(provenance, manifest)


def _build_reason(build: BuildEvidence, descriptor: dict) -> str | None:
    model = next((model for model in descriptor["models"] if model["unique_id"] == build.model_id), None)
    if model is None or model["reason"] is not None:
        return "MODEL_INELIGIBLE"
    if build.output is None:
        return "OUTPUT_UNKNOWN"
    if asdict(build.output.relation) != model["output"]:
        raise ValueError("build output differs from deployment descriptor")
    expected = {canonical_json(value) for value in model["inputs"]}
    for snapshots in (build.inputs_before, build.inputs_after):
        actual = [canonical_json(asdict(snapshot.relation)) for snapshot in snapshots]
        if len(set(actual)) != len(actual) or set(actual) != expected:
            return "INPUT_UNKNOWN"
    expected_views = {canonical_json(value) for value in model["views"]}
    for view_snapshots in (build.views_before, build.views_after):
        actual = [canonical_json(asdict(snapshot.relation)) for snapshot in view_snapshots]
        if len(set(actual)) != len(actual) or set(actual) != expected_views:
            return "DEFINITION_UNKNOWN"
    if sorted(build.views_before, key=lambda item: item.relation) != sorted(
        build.views_after, key=lambda item: item.relation
    ):
        return "DEFINITION_CHANGED_DURING_BUILD"
    if sorted(build.inputs_before, key=lambda item: item.relation) != sorted(
        build.inputs_after, key=lambda item: item.relation
    ):
        return "INPUT_CHANGED_DURING_BUILD"
    return None


def record_build(
    spark, workspace_client, config: ReuseConfig, context: ExecutionContext, build: BuildEvidence, *, successful: bool
) -> str:
    """Only a successful, stable build with published artifacts supplies reusable evidence."""
    # One fallback surrounds publication so no state outage can fail an already successful model.
    # pylint: disable=too-many-try-statements
    if successful is not True:
        return "BUILD_FAILED"
    if build.output is None:
        return "OUTPUT_UNKNOWN"
    if sorted(build.inputs_before, key=lambda item: item.relation) != sorted(
        build.inputs_after, key=lambda item: item.relation
    ):
        return "INPUT_CHANGED_DURING_BUILD"
    try:
        descriptor, provenance = _verified_descriptor(workspace_client, config, build.descriptor)
        reason = _build_reason(build, descriptor)
        if reason:
            return reason
        if build.context_sha256 != context.digest or provenance.context != context:
            return "CONTEXT_CHANGED"
        scope = _scope(config, context, build.attempt.job_id)
        _assert_owner(spark, config, scope, build.output.relation)
        payload = {"format_version": 1, "scope": scope, "build": asdict(build)}
        key = _digest([scope, build.model_id, asdict(build.attempt)])
        _insert_immutable(
            spark,
            _state_table(config, "builds"),
            key,
            payload,
            scope=_digest(scope),
            model_id=build.model_id,
            output_sha256=_digest(asdict(build.output)),
            completed_at=_timestamp(build.completed_at),
        )
    except EvidenceUnavailable as error:
        return str(error)
    return "RECORDED"


def read_build(
    spark,
    workspace_client,
    config: ReuseConfig,
    context: ExecutionContext,
    model_id: str,
    output: RelationSnapshot,
    *,
    job_id: str,
    deployments=None,
) -> dict | None:
    """Select by live output evidence, then verify the complete record and immutable artifacts."""
    scope = _scope(config, context, job_id)
    _assert_owner(spark, config, scope, output.relation)
    rows = _state_rows(
        spark,
        f"SELECT key, evidence FROM {_state_table(config, 'builds')} "
        "WHERE scope = :scope AND model_id = :model_id AND output_sha256 = :output_sha256 "
        "ORDER BY completed_at DESC, key DESC LIMIT 1",
        {"scope": _digest(scope), "model_id": model_id, "output_sha256": _digest(asdict(output))},
    )
    if not rows:
        return None
    try:
        record = json.loads(rows[0]["evidence"])
        if (
            set(record) != {"format_version", "scope", "build"}
            or not isinstance(record["format_version"], int)
            or isinstance(record["format_version"], bool)
            or record["format_version"] != 1
        ):
            raise ValueError("invalid build record")
        build = BuildEvidence.from_dict(record["build"])
        key = _digest([scope, build.model_id, asdict(build.attempt)])
        if (
            record["scope"] != scope
            or build.model_id != model_id
            or build.output != output
            or build.attempt.job_id != job_id
            or rows[0]["key"] != key
        ):
            raise ValueError("build record scope/output mismatch")
    except (KeyError, TypeError, ValueError) as error:
        raise EvidenceUnavailable("BUILD_INVALID") from error
    descriptor, provenance = _verified_descriptor(workspace_client, config, build.descriptor, deployments)
    if (
        _scope(config, provenance.context, job_id) != scope
        or build.context_sha256 != provenance.context.digest
        or _build_reason(build, descriptor)
    ):
        raise EvidenceUnavailable("BUILD_INVALID")
    return record | {"key": key, "sha256": _digest(record)}


def record_decision(
    spark,
    config: ReuseConfig,
    context: ExecutionContext,
    model_id: str,
    attempt: Attempt,
    *,
    reason: str,
    observed_at: str,
    evidence: dict,
    durations: dict,
) -> str:
    """Decision history never writes successful-build evidence or changes its completion time."""
    if not all(isinstance(value, str) and value.strip() for value in (model_id, reason)):
        raise ValueError("decision requires a model and reason")
    _timestamp(observed_at)
    if any(not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0 for value in durations.values()):
        raise ValueError("decision durations must be nonnegative seconds")
    scope = _scope(config, context, attempt.job_id)
    payload = {
        "format_version": 1,
        "scope": scope,
        "model_id": model_id,
        "attempt": asdict(attempt),
        "reason": reason,
        "observed_at": observed_at,
        "evidence": evidence,
        "durations": durations,
    }
    key = _digest([scope, model_id, asdict(attempt), observed_at])
    try:
        _insert_immutable(spark, _state_table(config, "decisions"), key, payload)
    except EvidenceUnavailable as error:
        return str(error)
    return "RECORDED"


def _prepare_model(spark, workspace_client, config, context, model, attempt, deployments):
    # Keep expected metadata/state outages within this model's fallback boundary.
    # pylint: disable=too-many-try-statements
    if model.reason:
        return {"reason": model.reason, "baseline": None}, None
    try:
        claim_owner(spark, config, context, model.output, job_id=attempt.job_id)
        output = snapshot_relation(spark, workspace_client, model.output)
        build = read_build(
            spark,
            workspace_client,
            config,
            context,
            model.unique_id,
            output,
            job_id=attempt.job_id,
            deployments=deployments,
        )
        if build is None:
            return {"reason": "NO_BASELINE", "baseline": None}, None
        decision = {"reason": None, "baseline": {"key": build["key"], "sha256": build["sha256"]}}
        views = [asdict(snapshot_view(workspace_client, relation)) for relation in model.views]
        if sorted(views, key=canonical_json) != sorted(build["build"].get("views_after", []), key=canonical_json):
            return decision | {"reason": "DEFINITION_CHANGED"}, None
        _descriptor, manifest, provenance = load_deployment(
            workspace_client, config, build["build"]["descriptor"], deployments=deployments
        )
        return decision, (manifest, provenance)
    except EvidenceUnavailable as error:
        return {"reason": str(error), "baseline": None}, None


def prepare_run(
    spark, workspace_client, config, deployment, project, profiles, attempt, *, secret_versions=None, model_ids=None
) -> dict:
    # pylint: disable=too-many-locals
    """Collect exact successful baselines and publish code evidence, never permission to skip.

    Expected evidence outages yield visible normal-execution reasons. Programming/configuration
    errors propagate to the preparation task and retain Lakeflow's normal failure behavior.
    """
    result = {
        "format_version": 1,
        "deployment": deployment,
        "attempt": asdict(attempt),
        "reason": None,
        "models": {},
        "comparisons": [],
    }
    # Cache only within this preparation attempt. Model tasks always perform fresh verification.
    deployments: dict = {}
    try:
        _descriptor, manifest, provenance = load_deployment(
            workspace_client, config, deployment, deployments=deployments
        )
        context = read_provenance(provenance, manifest).context
        verify_runtime_identity(spark, workspace_client, context)
    except EvidenceUnavailable as error:
        return result | {"reason": str(error)}
    models = describe_models(json.loads(manifest), context)
    if model_ids is not None:
        if (
            not isinstance(model_ids, list)
            or not all(isinstance(uid, str) for uid in model_ids)
            or len(set(model_ids)) != len(model_ids)
            or set(model_ids) - {model.unique_id for model in models}
        ):
            raise ValueError("preparation model scope must contain unique declared model ids")
        models = tuple(model for model in models if model.unique_id in model_ids)
        result["model_ids"] = sorted(model_ids)
    if json.loads(context.canonical_json)["secret_versions"] != (secret_versions or {}):
        return result | {"reason": "CONTEXT_CHANGED"}
    decisions, baselines = {}, {}
    for model in models:
        decisions[model.unique_id], baseline = _prepare_model(
            spark, workspace_client, config, context, model, attempt, deployments
        )
        if baseline is not None:
            baselines[model.unique_id] = baseline
    compared = compare_baselines(project, profiles, manifest, provenance, baselines)
    for uid, decision in decisions.items():
        if decision["reason"] is None:
            decision["reason"] = compared["models"][uid]
    return result | {"context_sha256": context.digest, "models": decisions, "comparisons": compared["comparisons"]}
