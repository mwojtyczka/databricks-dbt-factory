"""Offline reuse contracts. Eligibility is a candidate check, never permission to skip dbt."""

import hashlib
import json
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter

from databricks_dbt_factory.utils import DYNAMIC_VALUE_REFERENCE


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _object(value, required, label, optional=()):
    if not isinstance(value, dict) or set(value) - set(required) - set(optional) or set(required) - set(value):
        raise ValueError(f"{label}: expected fields {sorted(required)}, optional {sorted(optional)}")
    return value


def _text(value, label):
    if not isinstance(value, str) or not value.strip() or "\0" in value or DYNAMIC_VALUE_REFERENCE.search(value):
        raise ValueError(f"{label}: expected a nonempty static string")
    return value


@dataclass(frozen=True, order=True)
class Relation:
    catalog: str
    schema: str
    identifier: str

    @classmethod
    def from_dict(cls, value, label):
        fields = ("catalog", "schema", "identifier")
        value = _object(value, fields, label)
        return cls(*(_text(value[key], f"{label}.{key}") for key in fields))


@dataclass(frozen=True)
class ReuseConfig:
    project_id: str
    environment: str
    owner_key: str
    state_catalog: str
    state_schema: str
    artifact_volume: tuple[str, str, str]

    @classmethod
    def from_dict(cls, value):
        fields = ("project_id", "environment", "owner_key", "state_catalog", "state_schema")
        value = _object(value, (*fields, "format_version", "artifact_volume"), "reuse config")
        if (
            not isinstance(value["format_version"], int)
            or isinstance(value["format_version"], bool)
            or value["format_version"] != 1
        ):
            raise ValueError("reuse config: format_version must be 1")
        volume = _object(value["artifact_volume"], ("catalog", "schema", "volume"), "reuse config.artifact_volume")
        return cls(
            *(_text(value[key], f"reuse config.{key}") for key in fields),
            tuple(_text(volume[key], f"reuse config.artifact_volume.{key}") for key in ("catalog", "schema", "volume")),
        )


@dataclass(frozen=True)
class ExecutionContext:
    """Canonical non-secret context; source/deployment digests deliberately live elsewhere."""

    canonical_json: str

    @classmethod
    def from_dict(cls, value):
        strings = ("project_id", "environment", "workspace_id", "metastore_id", "principal", "target")
        mappings = ("versions", "vars", "environment_inputs", "secret_versions")
        value = _object(value, (*strings, *mappings), "reuse context")
        for key in strings:
            _text(value[key], f"reuse context.{key}")
        for key in mappings:
            if not isinstance(value[key], dict) or not all(isinstance(name, str) for name in value[key]):
                raise ValueError(f"reuse context.{key}: expected an object with string keys")
        required_versions = {"python", "dbt-core", "dbt-databricks", "databricks-sql-connector", "databricks-sdk"}
        if not required_versions <= value["versions"].keys():
            raise ValueError("reuse context.versions: missing runtime versions")
        for name, version in value["versions"].items():
            _text(version, f"reuse context.versions.{name}")
        for name, version in value["secret_versions"].items():
            _text(version, f"reuse context.secret_versions.{name}")
        for name, item in value["environment_inputs"].items():
            if name.upper().startswith("DBT_ENV_SECRET_"):
                raise ValueError("reuse context: secret environment values must use non-secret version identifiers")
            if item is not None and not isinstance(item, str):
                raise ValueError(f"reuse context.environment_inputs.{name}: expected string or null")
        return cls(canonical_json(value))

    @property
    def digest(self):
        return digest_bytes(self.canonical_json.encode())


@dataclass(frozen=True)
class ModelDescriptor:
    unique_id: str
    output: Relation | None
    inputs: tuple[Relation, ...]
    extra_inputs: tuple[Relation, ...]
    dependencies: tuple[str, ...]
    policy_digest: str
    reason: str | None
    definitions: tuple[str, ...] = ()
    views: tuple[Relation, ...] = ()


def _policy(model):
    label = f"{model['unique_id']} reuse metadata"
    meta = model.get("config", {}).get("meta", {})
    if not isinstance(meta, dict):
        raise ValueError(f"{label}: meta must be an object")
    factory = meta.get("dbt_factory", {})
    if not isinstance(factory, dict):
        raise ValueError(f"{label}: dbt_factory must be an object")
    if "reuse" not in factory:
        return None
    policy = _object(factory["reuse"], ("enabled",), label, ("extra_inputs",))
    if not isinstance(policy["enabled"], bool):
        raise ValueError(f"{label}: enabled must be a boolean")
    if not isinstance(policy.get("extra_inputs", []), list):
        raise ValueError(f"{label}: extra_inputs must be a list")
    for item in policy.get("extra_inputs", []):
        Relation.from_dict(item, label)
    return policy


def eligibility_reason(model, context: ExecutionContext | None) -> str | None:
    policy = _policy(model)
    if not policy or not policy["enabled"]:
        return "NOT_OPTED_IN"
    if model.get("resource_type") != "model":
        return "UNSUPPORTED_RESOURCE"
    if model.get("language") != "sql":
        return "UNSUPPORTED_LANGUAGE"
    config = model.get("config", {})
    if config.get("materialized") != "table":
        return "UNSUPPORTED_MATERIALIZATION"
    if config.get("pre-hook") or config.get("post-hook"):
        return "HOOKS_PRESENT"
    if context is None:
        return "CONTEXT_UNKNOWN"
    return None


def _relation(node):
    identifier = node.get("identifier") if node.get("resource_type") == "source" else node.get("alias")
    values = (node.get("database"), node.get("schema"), identifier)
    if not all(isinstance(item, str) and item.strip() and not DYNAMIC_VALUE_REFERENCE.search(item) for item in values):
        return None
    return Relation(*values)


def _producer_key(relation: Relation) -> Relation:
    # UC identifiers are case insensitive; descriptors retain their original components.
    return Relation(relation.catalog.casefold(), relation.schema.casefold(), relation.identifier.casefold())


def _definition_reason(node):
    config = node.get("config", {})
    return (
        "DEFINITION_UNSUPPORTED"
        if node.get("language") != "sql" or config.get("pre-hook") or config.get("post-hook")
        else None
    )


def _input_relations(uid, nodes, dependencies):
    inputs = set()
    definitions = set()
    views = set()
    reason = None
    pending = sorted(dependencies, reverse=True)
    visited = set()
    while pending:
        parent = pending.pop()
        if parent in visited:
            continue
        visited.add(parent)
        if parent not in nodes:
            raise ValueError(f"{uid}: missing declared dependency {parent}")
        upstream = nodes[parent]
        relation = _relation(upstream)
        materialization = upstream.get("config", {}).get("materialized")
        if upstream.get("resource_type") == "model" and materialization in {"view", "ephemeral"}:
            definitions.add(parent)
            reason = reason or _definition_reason(upstream)
            if materialization == "view" and relation is None:
                reason = reason or "RELATION_UNKNOWN"
            if materialization == "view" and relation is not None:
                views.add(relation)
            pending.extend(sorted(upstream.get("depends_on", {}).get("nodes", []), reverse=True))
            continue
        if relation is None:
            reason = reason or "RELATION_UNKNOWN"
        else:
            inputs.add(relation)
        if upstream.get("resource_type") == "model" and upstream.get("config", {}).get("materialized") not in {
            "table",
            "incremental",
        }:
            reason = reason or "DEPENDENCY_UNSUPPORTED"
        elif upstream.get("resource_type") not in {"model", "source", "seed", "snapshot"}:
            reason = reason or "DEPENDENCY_UNSUPPORTED"
    return inputs, reason, definitions, views


def _declared_dependencies(uid, node, extra, producers):
    dependencies = set(node.get("depends_on", {}).get("nodes", []))
    for relation in extra:
        if len(producers.get(_producer_key(relation), [])) > 1:
            raise ValueError(f"ambiguous extra input for {uid}: {producers[_producer_key(relation)]}")
        dependencies.update(producers.get(_producer_key(relation), []))
    if uid in dependencies:
        raise ValueError(f"{uid}: cannot declare its own output as an input")
    return dependencies


def describe_models(manifest, context: ExecutionContext | None) -> tuple[ModelDescriptor, ...]:
    # pylint: disable=too-many-locals
    """Resolve declarations; live UC/Delta checks still have to establish physical eligibility."""
    nodes = manifest.get("nodes", {}) | manifest.get("sources", {})
    producers: dict[Relation, list[str]] = {}
    for uid, node in nodes.items():
        relation = _relation(node)
        if relation and node.get("resource_type") in {"model", "seed", "snapshot"}:
            producers.setdefault(_producer_key(relation), []).append(uid)
    descriptors = []
    global_hooks = any(node.get("resource_type") == "operation" for node in nodes.values())
    for uid, node in sorted(nodes.items()):
        policy = _policy(node)
        if not policy or not policy["enabled"]:
            continue
        output = _relation(node)
        reason = eligibility_reason(node, context)
        if output and len(producers.get(_producer_key(output), [])) > 1:
            raise ValueError(f"ambiguous output relation for {uid}: {producers[_producer_key(output)]}")
        if output is None:
            reason = reason or "RELATION_UNKNOWN"
        if global_hooks:
            reason = reason or "HOOKS_PRESENT"
        extra = tuple(sorted({Relation.from_dict(item, uid) for item in policy.get("extra_inputs", [])}))
        dependencies = _declared_dependencies(uid, node, extra, producers)
        inputs, input_reason, definitions, views = _input_relations(uid, nodes, dependencies)
        reason = reason or input_reason
        inputs.update(relation for relation in extra if not producers.get(_producer_key(relation)))
        descriptors.append(
            ModelDescriptor(
                uid,
                output,
                tuple(sorted(inputs)),
                extra,
                tuple(sorted(dependencies)),
                digest_bytes(canonical_json(policy).encode()),
                reason,
                tuple(sorted(definitions)),
                tuple(sorted(views)),
            )
        )
    graph = {uid: node.get("depends_on", {}).get("nodes", []) for uid, node in nodes.items()}
    graph.update({item.unique_id: item.dependencies for item in descriptors})
    try:
        TopologicalSorter(graph).prepare()
    except CycleError as error:
        raise ValueError(f"reuse dependency cycle: {error.args[1]}") from error
    return tuple(descriptors)


def lineage_reason(model: ModelDescriptor, observed_inputs) -> str | None:
    return "UNDECLARED_INPUT" if set(observed_inputs) - set(model.inputs + model.views) else None
