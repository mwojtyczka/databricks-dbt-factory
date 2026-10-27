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

from databricks_dbt_factory.reuse import ExecutionContext, canonical_json, digest_bytes


@dataclass(frozen=True)
class Provenance:
    manifest_sha256: str
    snapshot_sha256: str
    deployment_ref: str
    context: ExecutionContext


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
    fields = {"format_version", "manifest_sha256", "snapshot_sha256", "deployment_ref", "context", "context_sha256"}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError(f"reuse provenance: expected fields {sorted(fields)}")
    if (
        not isinstance(value["format_version"], int)
        or isinstance(value["format_version"], bool)
        or value["format_version"] != 1
    ):
        raise ValueError("reuse provenance: format_version must be 1")
    for key in ("manifest_sha256", "snapshot_sha256", "context_sha256"):
        if not isinstance(value[key], str) or not re.fullmatch(r"[0-9a-f]{64}", value[key]):
            raise ValueError(f"reuse provenance: invalid {key}")
    if not isinstance(value["deployment_ref"], str) or not value["deployment_ref"].strip():
        raise ValueError("reuse provenance: deployment_ref is required")
    context = ExecutionContext.from_dict(value["context"])
    if context.digest != value["context_sha256"]:
        raise ValueError("reuse provenance: context digest mismatch")
    if digest_bytes(manifest) != value["manifest_sha256"]:
        raise ValueError("reuse provenance: manifest digest mismatch; capture provenance with this parse")
    return Provenance(value["manifest_sha256"], value["snapshot_sha256"], value["deployment_ref"], context)


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
    installed. This helper never infers provenance for an existing manifest, copies profiles,
    resolves packages, or hashes credentials. Target/log output lives outside the snapshot.
    """
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
    before = snapshot_digest(project_directory)
    with tempfile.TemporaryDirectory(prefix="factory-reuse-parse-") as directory:
        command = [
            sys.executable,
            "-c",
            "from dbt.cli.main import cli; cli()",
            "--no-partial-parse",
            "parse",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(profiles),
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
        "format_version": 1,
        "manifest_sha256": digest_bytes(manifest),
        "snapshot_sha256": before,
        "deployment_ref": deployment_ref,
        "context": json.loads(context.canonical_json),
        "context_sha256": context.digest,
    }
    return manifest, canonical_json(provenance).encode()
