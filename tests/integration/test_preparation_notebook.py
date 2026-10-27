"""Execute the packaged preparation notebook with controlled platform boundaries."""

import json
import os
import runpy
from importlib import resources
from types import SimpleNamespace

import pytest
import databricks.sdk

from databricks_dbt_factory import reuse_runtime
from tests.test_reuse import CONFIG


@pytest.mark.parametrize("failure", [None, "unavailable", "unexpected"])
def test_shipped_preparation_notebook_publishes_pointer_and_cleans_credentials(monkeypatch, tmp_path, failure):
    values = {
        "factory_reuse_config": json.dumps(CONFIG),
        "factory_reuse_deployment": json.dumps(
            {"path": "/Volumes/state/factory/artifacts/descriptor.json", "sha256": "a" * 64}
        ),
        "project_directory": str(tmp_path),
        "profiles_directory": str(tmp_path / "profiles"),
        "factory_job_id": "job",
        "factory_run_id": "run",
        "factory_task_key": "prepare",
        "factory_attempt_number": "0",
        "factory_secret_versions": "{}",
    }
    published, exited = {}, []
    dbutils = SimpleNamespace(
        widgets=SimpleNamespace(text=lambda *_args: None, get=values.__getitem__),
        jobs=SimpleNamespace(taskValues=SimpleNamespace(set=lambda **kwargs: published.update(kwargs))),
        notebook=SimpleNamespace(exit=exited.append),
    )
    client = SimpleNamespace(
        config=SimpleNamespace(
            host="https://example.databricks.com/", authenticate=lambda: {"Authorization": "Bearer private-token"}
        )
    )
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda: client)

    def prepare(*args, **kwargs):
        assert args[4].resolve() == tmp_path.resolve()
        assert os.environ["DBT_ACCESS_TOKEN"] == "private-token"
        assert os.environ["DBT_HOST"] == "example.databricks.com"
        assert kwargs == {"secret_versions": {}}
        if failure == "unexpected":
            raise RuntimeError("unexpected task failure")
        return {"format_version": 1, "reason": None, "models": {}}

    def publish(*_args):
        if failure == "unavailable":
            raise reuse_runtime.EvidenceUnavailable("ARTIFACT_UNAVAILABLE")
        return {"path": "/Volumes/state/factory/artifacts/preparation.json", "sha256": "b" * 64}

    monkeypatch.setattr(reuse_runtime, "prepare_run", prepare)
    monkeypatch.setattr(reuse_runtime, "publish_preparation", publish)
    notebook = resources.files("databricks_dbt_factory") / "notebook" / "prepare_dbt_reuse.py"
    if failure == "unexpected":
        with pytest.raises(RuntimeError, match="unexpected task failure"):
            runpy.run_path(str(notebook), init_globals={"dbutils": dbutils, "spark": object()})
        assert not published
    else:
        runpy.run_path(str(notebook), init_globals={"dbutils": dbutils, "spark": object()})
        pointer = json.loads(published["value"])
        assert published["key"] == "factory_reuse_preparation"
        assert pointer == (
            {} if failure else {"path": "/Volumes/state/factory/artifacts/preparation.json", "sha256": "b" * 64}
        )
        assert json.loads(exited[0])["reason"] == ("ARTIFACT_UNAVAILABLE" if failure else None)
    assert "DBT_ACCESS_TOKEN" not in os.environ
    assert "DBT_HOST" not in os.environ
