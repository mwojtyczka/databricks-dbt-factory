"""Immutable artifact transport; dbt supplies the manifest used to build each bundle."""

import io
import json
from types import SimpleNamespace

import pytest
from databricks.sdk.errors import AlreadyExists

from databricks_dbt_factory.reuse import canonical_json, digest_bytes
from databricks_dbt_factory.reuse_runtime import (
    download_artifact,
    prepare_bundle,
    publish_artifacts,
    read_descriptor,
    write_bundle,
)
from tests import test_reuse
from tests.test_reuse import CONFIG

captured = test_reuse.captured


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
