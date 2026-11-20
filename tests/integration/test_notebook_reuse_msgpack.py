"""Opted-in model tasks use a pre-built msgpack only when it matches the captured deployment graph."""

import pytest

from tests import test_reuse, test_reuse_runtime as state_tests
from tests.integration import test_notebook_reuse as notebook_tests

local_bundle = state_tests.local_bundle
evidence = state_tests.evidence
notebook_runtime = notebook_tests.notebook_runtime
matching_preparation = notebook_tests.matching_preparation
model_invocations = notebook_tests.model_invocations
STALE = "\n-- edited after the msgpack was built"


@pytest.fixture(scope="module", params=["matching", "stale"])
def captured(request, tmp_path_factory):
    """Overrides the shared capture so its snapshot ships `target/partial_parse.msgpack`."""
    return test_reuse.capture_project(tmp_path_factory.mktemp("msgpack"), "" if request.param == "matching" else STALE)


def test_forced_model_uses_msgpack_only_when_it_matches(request, matching_preparation, model_invocations, monkeypatch):
    from dbt.cli.main import dbtRunner  # pylint: disable=import-outside-toplevel

    store, _client, _config, _context, _build, _snapshots, values = matching_preparation
    values["factory_force_rebuild"] = "true"
    commands, invoke = [], dbtRunner.invoke

    def record(runner, args):
        commands.append(args[0])
        return invoke(runner, args)

    monkeypatch.setattr(dbtRunner, "invoke", record)
    result = notebook_tests.execute_notebook(values, store)
    assert result["model_run"].reason == "FORCED"
    assert result["model_run"].publication == "RECORDED"
    assert len(model_invocations) == 1
    # A stale msgpack is never injected: the task falls back to a fresh parse of the deployed project.
    assert commands == (["run"] if request.node.callspec.params["captured"] == "matching" else ["parse", "run"])
    assert not result["runner"].manifest.nodes["model.probe.orders"].raw_code.endswith(STALE)
