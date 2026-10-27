"""Immutable reuse artifacts. SDK objects are supplied by the caller, never imported offline."""

import io
import json
import re
from dataclasses import asdict
from pathlib import Path, PurePosixPath

from databricks_dbt_factory.__version__ import __version__
from databricks_dbt_factory.file_io import atomic_write_bytes
from databricks_dbt_factory.reuse import ReuseConfig, canonical_json, describe_models, digest_bytes
from databricks_dbt_factory.reuse_prepare import read_provenance


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
