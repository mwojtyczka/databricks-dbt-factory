"""Capture a fresh parse and bind it to a clean deployment snapshot and non-secret context."""

import importlib.metadata
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import yaml

from databricks_dbt_factory.reuse import ExecutionContext, canonical_json, describe_models, digest_bytes


_PROFILE_ENV_VAR = re.compile(r"\{\{\s*env_var\(\s*(['\"])([A-Za-z_][A-Za-z0-9_]*)\1\s*\)\s*\}\}")


@dataclass(frozen=True)
class Provenance:
    manifest_sha256: str
    snapshot_sha256: str
    profiles_sha256: str
    deployment_ref: str
    context: ExecutionContext


def _validate_profile_token(variable, payload):
    if variable != "DBT_ACCESS_TOKEN" and not (
        variable and variable.startswith("DBT_ENV_SECRET_") and variable in payload["secret_versions"]
    ):
        raise ValueError("reuse profiles: token requires SDK env_var or a versioned secret env_var")
    if variable in payload["environment_inputs"]:
        raise ValueError("reuse profiles: credentials cannot be captured as environment inputs")


def _validate_literal_profile_value(value, message):
    if not isinstance(value, str) or not value.strip() or any(marker in value for marker in ("{{", "{%", "{#")):
        raise ValueError("reuse profiles: " + message)


def _validate_profile_value(key, item, payload):
    if key == "threads" and isinstance(item, int) and not isinstance(item, bool) and item > 0:
        return
    if not isinstance(item, str) or not item.strip():
        raise ValueError("reuse profiles: expected nonempty scalar template values")
    match = _PROFILE_ENV_VAR.fullmatch(item)
    variable = match[2] if match else None
    if key == "token":
        _validate_profile_token(variable, payload)
        return
    if variable is None:
        _validate_literal_profile_value(item, "only single env_var expressions without defaults are supported")
    else:
        if variable == "DBT_ACCESS_TOKEN" or variable.startswith("DBT_ENV_SECRET_"):
            raise ValueError("reuse profiles: secret env_var is only supported for token")
        if variable not in payload["environment_inputs"]:
            raise ValueError("reuse profiles: every routing env_var must be declared in environment_inputs")
        _validate_literal_profile_value(
            payload["environment_inputs"][variable], "routing env_var values must be nonempty literal strings"
        )
    if key == "host":
        hostname = payload["environment_inputs"].get(variable) if variable else item
        if not isinstance(hostname, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", hostname):
            raise ValueError("reuse profiles: host must be a bare hostname without credentials")


def _validate_profile_output(output, payload):
    fields = {"type", "host", "http_path", "catalog", "schema", "threads", "token"}
    if (
        not isinstance(output, dict)
        or set(output) - fields
        or not {"type", "host", "http_path", "schema", "token"} <= output.keys()
        or output["type"] != "databricks"
    ):
        raise ValueError("reuse profiles: only basic Databricks PAT outputs are supported")
    for key, item in output.items():
        _validate_profile_value(key, item, payload)


def profile_template(directory: Path, context: ExecutionContext) -> bytes:
    """Accept basic Databricks PAT profiles; hash only validated, unresolved template values.

    Credentials use the notebook SDK token or versioned DBT_ENV_SECRET_ references. Other
    fields are literal or a single declared env_var without defaults. Nested adapter options
    and other auth modes require a separately reviewed contract before reuse supports them.
    """
    payload = json.loads(context.canonical_json)
    try:
        value = yaml.safe_load((Path(directory) / "profiles.yml").read_bytes())
    except yaml.YAMLError:
        raise ValueError("reuse profiles: invalid YAML") from None
    name_pattern = r"[A-Za-z_][A-Za-z0-9_-]*"
    if (
        not isinstance(value, dict)
        or len(value) != 1
        or not all(isinstance(name, str) and re.fullmatch(name_pattern, name) for name in value)
    ):
        raise ValueError("reuse profiles: expected one statically named profile")
    profile = next(iter(value.values()))
    if (
        not isinstance(profile, dict)
        or set(profile) != {"target", "outputs"}
        or profile["target"] != payload["target"]
        or not isinstance(profile["outputs"], dict)
        or payload["target"] not in profile["outputs"]
    ):
        raise ValueError("reuse profiles: expected static target and outputs")
    for name, output in profile["outputs"].items():
        if not isinstance(name, str) or not re.fullmatch(name_pattern, name):
            raise ValueError("reuse profiles: expected static output names")
        _validate_profile_output(output, payload)
    # YAML comments and discarded duplicate values never enter hashes or private snapshots.
    return canonical_json(value).encode()


def verify_profiles(provenance: Provenance, directory: Path) -> bytes:
    """Return the exact validated template to use for subsequent dbt invocations."""
    content = profile_template(directory, provenance.context)
    if digest_bytes(content) != provenance.profiles_sha256:
        raise ValueError("reuse profiles digest mismatch; deploy the captured profile template")
    return content


def private_profiles_bytes(template: bytes, context: ExecutionContext) -> bytes:
    """Freeze declared routing values; leave credential env_var references unresolved."""
    profile = json.loads(template)
    environment = json.loads(context.canonical_json)["environment_inputs"]
    settings = next(iter(profile.values()))
    for output in settings["outputs"].values():
        for key, value in output.items():
            if key == "token" or not isinstance(value, str):
                continue
            variable = _PROFILE_ENV_VAR.fullmatch(value)
            if variable:
                output[key] = environment[variable[2]]
    return canonical_json(profile).encode()


def runtime_profile_reason(template: bytes, context: ExecutionContext, host: str) -> str | None:
    """The supported notebook path must use the SDK token and its authenticated host."""
    payload = json.loads(context.canonical_json)
    profile = next(iter(json.loads(template).values()))
    output = profile["outputs"][payload["target"]]
    token = _PROFILE_ENV_VAR.fullmatch(output["token"])
    if token is None or token[2] != "DBT_ACCESS_TOKEN":
        return "PROFILE_UNSUPPORTED_AUTH"
    configured_host = output["host"]
    variable = _PROFILE_ENV_VAR.fullmatch(configured_host)
    if variable:
        configured_host = os.environ.get(variable[2])
    parsed = urlparse(host if "://" in host else "//" + host)
    if parsed.username is not None or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        return "CONTEXT_CHANGED"
    if not configured_host or configured_host.lower() != parsed.netloc.lower():
        return "CONTEXT_CHANGED"
    return None


def runtime_versions() -> dict[str, str]:
    """Dependency upgrades invalidate broadly; compare only pinned execution environments."""
    return {"python": platform.python_version()} | {
        distribution.metadata["Name"].lower().replace("_", "-"): distribution.version
        for distribution in importlib.metadata.distributions()
    }


def snapshot_digest(directory: Path) -> str:
    """Hash a dedicated deployment tree, including installed packages. Profiles live outside it."""
    directory = Path(directory)
    if directory.is_symlink():
        raise ValueError("reuse snapshot: symlink root is unsupported")
    if not directory.is_dir():
        raise ValueError("reuse snapshot: expected a deployment directory")
    files = []
    for path in sorted(directory.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"reuse snapshot: symlink is unsupported: {path}")
        if path.is_file():
            if path.name in {"profiles.yml", ".databrickscfg", ".env"} or path.name.startswith(".env."):
                raise ValueError(f"reuse snapshot: keep credentials outside the deployment tree: {path.name}")
            files.append((path.relative_to(directory).as_posix(), digest_bytes(path.read_bytes())))
    return digest_bytes(canonical_json(files).encode())


def verify_snapshot(provenance: Provenance, directory: Path) -> str:
    digest = snapshot_digest(directory)
    if digest != provenance.snapshot_sha256:
        raise ValueError("reuse snapshot digest mismatch: deploy the exact captured project and locked packages")
    return digest


def read_provenance(content: bytes, manifest: bytes) -> Provenance:
    try:
        value = json.loads(content)
    except ValueError as error:
        raise ValueError("reuse provenance: expected JSON") from error
    fields = {
        "format_version",
        "manifest_sha256",
        "snapshot_sha256",
        "profiles_sha256",
        "deployment_ref",
        "context",
        "context_sha256",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError(f"reuse provenance: expected fields {sorted(fields)}")
    if (
        not isinstance(value["format_version"], int)
        or isinstance(value["format_version"], bool)
        or value["format_version"] != 2
    ):
        raise ValueError("reuse provenance: format_version must be 2")
    for key in ("manifest_sha256", "snapshot_sha256", "profiles_sha256", "context_sha256"):
        if not isinstance(value[key], str) or not re.fullmatch(r"[0-9a-f]{64}", value[key]):
            raise ValueError(f"reuse provenance: invalid {key}")
    if not isinstance(value["deployment_ref"], str) or not value["deployment_ref"].strip():
        raise ValueError("reuse provenance: deployment_ref is required")
    context = ExecutionContext.from_dict(value["context"])
    if context.digest != value["context_sha256"]:
        raise ValueError("reuse provenance: context digest mismatch")
    if digest_bytes(manifest) != value["manifest_sha256"]:
        raise ValueError("reuse provenance: manifest digest mismatch; capture provenance with this parse")
    return Provenance(
        value["manifest_sha256"], value["snapshot_sha256"], value["profiles_sha256"], value["deployment_ref"], context
    )


def capture_parse(
    project_directory: Path,
    profiles_directory: Path,
    *,
    identity: dict,
    target: str,
    deployment_ref: str,
    variables: dict | None = None,
    environment_inputs: tuple[str, ...] = (),
    secret_versions: dict | None = None,
) -> tuple[bytes, bytes]:
    """Run in the pinned execution environment. Caller supplies non-secret vars/version identifiers.

    The project must be a clean, immutable deployment tree with its locked packages already
    installed. This helper never infers provenance for an existing manifest, resolves packages,
    or hashes credentials. Only validated profile templates are copied;
    target/log output lives outside the snapshot.
    """
    # pylint: disable=too-many-locals
    project = Path(project_directory).resolve()
    profiles = Path(profiles_directory).resolve()
    if profiles == project or project in profiles.parents:
        raise ValueError("reuse capture: profiles directory must be outside the project snapshot")
    identity_fields = {"project_id", "environment", "workspace_id", "metastore_id", "principal"}
    if not isinstance(identity, dict) or set(identity) != identity_fields:
        raise ValueError(f"reuse capture: identity requires {sorted(identity_fields)}")
    if not isinstance(deployment_ref, str) or not deployment_ref.strip():
        raise ValueError("reuse capture: deployment_ref is required")
    environment = os.environ.copy()
    context = ExecutionContext.from_dict(
        identity
        | {
            "target": target,
            "versions": runtime_versions(),
            "vars": {} if variables is None else variables,
            "environment_inputs": {name: environment.get(name) for name in environment_inputs},
            "secret_versions": {} if secret_versions is None else secret_versions,
        }
    )
    template = profile_template(profiles, context)
    before = snapshot_digest(project_directory)
    with tempfile.TemporaryDirectory(prefix="factory-reuse-parse-") as directory:
        private_profiles = Path(directory) / "profiles"
        private_profiles.mkdir()
        (private_profiles / "profiles.yml").write_bytes(private_profiles_bytes(template, context))
        command = [
            sys.executable,
            "-c",
            "from dbt.cli.main import cli; cli()",
            "--no-partial-parse",
            "parse",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(private_profiles),
            "--target",
            target,
            "--vars",
            canonical_json({} if variables is None else variables),
            "--target-path",
            str(Path(directory) / "target"),
            "--log-path",
            str(Path(directory) / "logs"),
        ]
        result = subprocess.run(command, env=environment, capture_output=True, check=False, timeout=300)  # noqa: S603
        if result.returncode:
            raise ValueError(f"reuse capture: dbt parse failed (exit {result.returncode}); no provenance produced")
        manifest = (Path(directory) / "target" / "manifest.json").read_bytes()
    if before != snapshot_digest(project_directory):
        raise ValueError("reuse capture: snapshot changed during parse; no provenance produced")
    if json.loads(manifest)["metadata"]["dbt_version"] != json.loads(context.canonical_json)["versions"]["dbt-core"]:
        raise ValueError("reuse capture: parsed engine version differs from context")
    provenance = {
        "format_version": 2,
        "profiles_sha256": digest_bytes(template),
        "manifest_sha256": digest_bytes(manifest),
        "snapshot_sha256": before,
        "deployment_ref": deployment_ref,
        "context": json.loads(context.canonical_json),
        "context_sha256": context.digest,
    }
    return manifest, canonical_json(provenance).encode()


def _parse_graph(manifest):
    """Ignore only dbt's parse timestamps when checking the deployed fresh parse."""
    return {
        section: {
            uid: {key: value for key, value in node.items() if key != "created_at"}
            for uid, node in manifest.get(section, {}).items()
        }
        for section in (
            "nodes",
            "sources",
            "macros",
            "unit_tests",
            "semantic_models",
            "saved_queries",
            "metrics",
            "exposures",
        )
    }


def verify_parsed_manifest(parsed: dict, manifest: bytes) -> None:
    """Bind a fresh native parse to the captured deployment graph."""
    if _parse_graph(parsed) != _parse_graph(json.loads(manifest)):
        raise ValueError("reuse preparation fresh parse differs from deployment; verify profiles, vars and environment")


def _native_modified(project, template, context, baseline, environment, manifest):
    """Ask dbt itself; baseline artifacts and current output must never share a directory."""
    with tempfile.TemporaryDirectory(prefix="factory-reuse-state-") as directory:
        root = Path(directory)
        profiles = root / "profiles"
        profiles.mkdir()
        (profiles / "profiles.yml").write_bytes(template)
        state = root / "baseline"
        state.mkdir()
        (state / "manifest.json").write_bytes(baseline)
        command = [
            sys.executable,
            "-c",
            "from dbt.cli.main import cli; cli()",
            "--no-partial-parse",
            "--no-use-colors",
            "ls",
            "--select",
            "state:modified",
            "--resource-type",
            "model",
            "--state",
            str(state),
            "--output",
            "json",
            "--output-keys",
            "unique_id",
            "--project-dir",
            str(Path(project).resolve()),
            "--profiles-dir",
            str(Path(profiles).resolve()),
            "--target",
            context["target"],
            "--vars",
            canonical_json(context["vars"]),
            "--target-path",
            str(root / "current"),
            "--log-path",
            str(root / "logs"),
        ]
        result = subprocess.run(command, env=environment, capture_output=True, text=True, check=False, timeout=300)  # noqa: S603
        diagnostics = {"stdout": result.stdout, "stderr": result.stderr, "returncode": result.returncode}
        if result.returncode:
            raise ValueError(f"dbt state comparison failed (exit {result.returncode}): {canonical_json(diagnostics)}")
        changed = set()
        for line in result.stdout.splitlines():
            try:
                value = json.loads(line)
            except ValueError:
                continue  # dbt emits diagnostics alongside the selected node JSON records.
            if isinstance(value, dict) and set(value) == {"unique_id"} and isinstance(value["unique_id"], str):
                changed.add(value["unique_id"])
        # A successful command without a fresh manifest cannot establish comparison support.
        if not (root / "current" / "manifest.json").is_file():
            return None, diagnostics
        parsed = json.loads((root / "current" / "manifest.json").read_bytes())
        verify_parsed_manifest(parsed, manifest)
        return changed, diagnostics


def _comparison_reason(current, previous):
    if current is None or current.reason:
        return current.reason if current else "MODEL_INELIGIBLE"
    if previous is None or previous.reason:
        return "NO_BASELINE"
    if current.policy_digest != previous.policy_digest:
        return "POLICY_CHANGED"
    if (
        current.output,
        current.inputs,
        current.extra_inputs,
        current.dependencies,
        current.definitions,
        current.views,
    ) != (
        previous.output,
        previous.inputs,
        previous.extra_inputs,
        previous.dependencies,
        previous.definitions,
        previous.views,
    ):
        return "DEPENDENCIES_CHANGED"
    return None


def _baseline_groups(models, current, manifest, baselines):
    reasons = {uid: model.reason or "NO_BASELINE" for uid, model in models.items()}
    groups: dict[tuple[str, str], dict] = {}
    parsed_baselines = {}
    schema = json.loads(manifest)["metadata"]["dbt_schema_version"]
    for uid, baseline in baselines.items():
        if baseline is None:
            continue
        artifact_key = baseline
        if artifact_key not in parsed_baselines:
            previous = read_provenance(baseline[1], baseline[0])
            old_manifest = json.loads(baseline[0])
            old_models = {model.unique_id: model for model in describe_models(old_manifest, previous.context)}
            parsed_baselines[artifact_key] = (previous, old_models, old_manifest["metadata"]["dbt_schema_version"])
        previous, old_models, old_schema = parsed_baselines[artifact_key]
        if previous.context != current.context or previous.profiles_sha256 != current.profiles_sha256:
            reasons[uid] = "CONTEXT_CHANGED"
            continue
        reason = _comparison_reason(models.get(uid), old_models.get(uid))
        if reason:
            reasons[uid] = reason
            continue
        if old_schema != schema:
            reasons[uid] = "COMPARISON_UNSUPPORTED"
            continue
        key = (previous.manifest_sha256, previous.context.digest)
        groups.setdefault(key, {"manifest": baseline[0], "models": []})["models"].append(uid)
    return reasons, groups


def compare_baselines(project, profiles, manifest: bytes, provenance: bytes, baselines: dict) -> dict:
    """Per-model successful artifacts, grouped by manifest/context. This is code evidence only.

    Baseline entries are verified manifest/provenance bytes or None. Runtime callers bind each
    entry to an exact immutable successful-build reference in the published preparation result.
    """
    current = read_provenance(provenance, manifest)
    verify_snapshot(current, project)
    template = verify_profiles(current, profiles)
    context = json.loads(current.context.canonical_json)
    environment = os.environ.copy()
    models = {model.unique_id: model for model in describe_models(json.loads(manifest), current.context)}
    if runtime_versions() != context["versions"] or any(
        environment.get(name) != value for name, value in context["environment_inputs"].items()
    ):
        return {"models": {uid: "CONTEXT_CHANGED" for uid in models}, "comparisons": []}
    reasons, groups = _baseline_groups(models, current, manifest, baselines)
    comparisons = []
    for (manifest_sha256, context_sha256), group in groups.items():
        changed, diagnostics = _native_modified(
            project,
            private_profiles_bytes(template, current.context),
            context,
            group["manifest"],
            environment,
            manifest,
        )
        comparisons.append(
            {"manifest_sha256": manifest_sha256, "context_sha256": context_sha256, "diagnostics": diagnostics}
        )
        for uid in group["models"]:
            if changed is None:
                reasons[uid] = "COMPARISON_UNSUPPORTED"
            else:
                reasons[uid] = (
                    "CODE_CHANGED" if changed.intersection((uid, *models[uid].definitions)) else "CODE_UNCHANGED"
                )
    return {"models": reasons, "comparisons": comparisons}
