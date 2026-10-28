"""Acceptance project and generated gates verified with an actual dbt parse."""

import json
from types import SimpleNamespace

import pytest

from databricks_dbt_factory import reuse_runtime
from databricks_dbt_factory.reuse_prepare import capture_parse, read_provenance
from tests.databricks.acceptance_setup import render_config, write_project
from tests.databricks.acceptance_runner import execute, ObservedSpark
from tests.test_reuse import IDENTITY


@pytest.mark.parametrize(
    "variant",
    ["base", "unrelated", "macro", "vars", "split", "expensive", "native", "environment", "engine_previous", "partial"],
)
def test_generated_acceptance_graph(tmp_path, monkeypatch, bundle_validator, variant):
    monkeypatch.setenv("DBT_ACCESS_TOKEN", "parse-only")
    monkeypatch.setenv("FACTORY_ACCEPTANCE_CONTEXT", "2" if variant == "environment" else "1")
    config, variables = write_project(
        tmp_path, "example.databricks.com", "catalog", "scratch", "warehouse", variant, 10
    )
    identity = IDENTITY | {"project_id": config["project_id"], "environment": config["environment"]}
    manifest, provenance = capture_parse(
        tmp_path / "project",
        tmp_path / "profiles",
        identity=identity,
        target="probe",
        deployment_ref="test",
        variables=variables,
        environment_inputs=["FACTORY_ACCEPTANCE_CONTEXT"],
    )
    parsed = json.loads(manifest)
    context = read_provenance(provenance, manifest).context
    assert json.loads(context.canonical_json)["environment_inputs"] == {
        "FACTORY_ACCEPTANCE_CONTEXT": "2" if variant == "environment" else "1"
    }
    generated = render_config(
        tmp_path,
        parsed,
        context,
        config,
        {"path": "/Volumes/catalog/scratch/artifacts/descriptor.json", "sha256": "a" * 64},
        "/Workspace/acceptance/files",
        "/Volumes/catalog/scratch/artifacts/factory.whl",
        variant,
    )
    assert not list(bundle_validator.iter_errors(generated))
    jobs = generated["resources"]["jobs"]
    assert "acceptance" in jobs
    environment_job = next(job for job in jobs.values() if "environments" in job)
    dependencies = environment_job["environments"][0]["spec"]["dependencies"]
    assert ("dbt-core==1.12.4" if variant == "engine_previous" else "dbt-core==1.12.5") in dependencies
    assert "dbt-databricks==1.12.7" in dependencies
    tasks = {task["task_key"]: task for key, job in jobs.items() if key != "baseline" for task in job["tasks"]}
    assert {dep["task_key"] for dep in tasks["required_gate_test"]["depends_on"]} >= {"orders_model"}
    if variant != "split":
        assert "required_gate_test" in {dep["task_key"] for dep in tasks["report_model"]["depends_on"]}
    else:
        assert tasks["silver"]["depends_on"] == [{"task_key": "bronze"}]
    orders = parsed["nodes"]["model.factory_acceptance.orders"]
    assert orders["depends_on"]["macros"] == ["macro.factory_acceptance.outer"]
    assert parsed["macros"]["macro.factory_acceptance.outer"]["depends_on"]["macros"] == [
        "macro.factory_acceptance.inner"
    ]
    if variant == "native":
        assert "dbt_task" in tasks["orders_model"]
    else:
        parameters = tasks["orders_model"]["notebook_task"]["base_parameters"]
        assert parameters["project_directory"] == "/Workspace/acceptance/files/project"
        assert parameters["profiles_directory"] == "/Workspace/acceptance/files/profiles"
        assert json.loads(parameters.get("factory_dbt_vars", "{}")) == variables


def test_baseline_has_no_reuse_preparation(tmp_path, monkeypatch):
    monkeypatch.setenv("DBT_ACCESS_TOKEN", "parse-only")
    config, variables = write_project(tmp_path, "example.databricks.com", "catalog", "scratch", "warehouse", "vars")
    manifest, provenance = capture_parse(
        tmp_path / "project",
        tmp_path / "profiles",
        identity=IDENTITY | {"project_id": config["project_id"], "environment": config["environment"]},
        target="probe",
        deployment_ref="test",
        variables=variables,
    )
    generated = render_config(
        tmp_path,
        json.loads(manifest),
        read_provenance(provenance, manifest).context,
        config,
        {"path": "/Volumes/catalog/scratch/artifacts/descriptor.json", "sha256": "a" * 64},
        "/Workspace/acceptance/files",
        "/Volumes/catalog/scratch/artifacts/factory.whl",
        "vars",
    )
    baseline = generated["resources"]["jobs"]["baseline"]["tasks"]
    assert len(baseline) == 3
    for task in baseline:
        params = task["notebook_task"]["base_parameters"]
        assert not any(name.startswith("factory_reuse_") for name in params)
        assert json.loads(params["factory_dbt_vars"]) == variables


def test_runner_skip_does_not_import_dbt(tmp_path):
    values = {}
    dbutils = SimpleNamespace(widgets=SimpleNamespace(text=values.setdefault, get=values.__getitem__))
    notebook = tmp_path / "skip.py"
    notebook.write_text(
        "from types import SimpleNamespace\nmodel_run = SimpleNamespace(reason='REUSED', publication=None)\n"
    )
    previous = reuse_runtime.REUSE_ENABLED
    report = execute(dbutils, None, notebook=notebook)
    assert report["reason"] == "REUSED"
    assert not report["imported_dbt"]
    assert report["invocations"] == {"run": 0, "test": 0, "parse": 0}
    assert reuse_runtime.REUSE_ENABLED is previous


def test_runner_counts_real_parse_and_persists_failed_attempt(tmp_path, monkeypatch):
    monkeypatch.setenv("DBT_ACCESS_TOKEN", "parse-only")
    write_project(tmp_path, "example.databricks.com", "catalog", "scratch", "warehouse", "base")
    values = {
        "report_dir": "/Volumes/catalog/scratch/artifacts/metrics",
        "probe_stage": "failed",
        "probe_job_id": "1",
        "probe_run_id": "2",
        "probe_task_key": "orders",
        "probe_attempt": "1",
    }
    dbutils = SimpleNamespace(widgets=SimpleNamespace(text=values.setdefault, get=values.__getitem__))
    uploaded = {}

    def upload(path, stream, overwrite):
        assert overwrite
        uploaded[path] = json.load(stream)

    notebook = tmp_path / "real_parse.py"
    notebook.write_text(
        "from dbt.cli.main import dbtRunner\n"
        f"result = dbtRunner().invoke(['parse', '--project-dir', {str(tmp_path / 'project')!r}, "
        f"'--profiles-dir', {str(tmp_path / 'profiles')!r}, '--target', 'probe', "
        f"'--target-path', {str(tmp_path / 'target')!r}, '--log-path', {str(tmp_path / 'logs')!r}])\n"
        "assert result.success\nraise RuntimeError('required gate failed')\n"
    )
    with pytest.raises(RuntimeError, match="required gate failed"):
        execute(dbutils, None, notebook=notebook, client=SimpleNamespace(files=SimpleNamespace(upload=upload)))
    report = next(iter(uploaded.values()))
    assert report["success"] is False
    assert report["invocations"] == {"run": 0, "test": 0, "parse": 1}
    assert report["imported_dbt"]
    assert report["report_path"].endswith("/failed_1_2_orders_1.json")


def test_spark_observation_delegates_actions_and_arguments():
    calls = []
    row = object()
    frame = SimpleNamespace(collect=lambda: [row], first=lambda: row)

    def sql(statement, **kwargs):
        calls.append((statement, kwargs))
        return frame

    events = []
    observed = ObservedSpark(SimpleNamespace(sql=sql, version="real-version"), events)
    assert observed.sql("SELECT :id", args={"id": 7}).collect() == [row]
    assert observed.sql("DESCRIBE DETAIL input").first() is row
    assert calls == [("SELECT :id", {"args": {"id": 7}}), ("DESCRIBE DETAIL input", {})]
    assert [event["kind"] for event in events] == ["SELECT", "DESCRIBE"]
    assert all(event["elapsed_seconds"] >= 0 for event in events)
    assert all(len(event["sha256"]) == 64 for event in events)
    assert observed.version == "real-version"


def test_sql_observation_delegates_cursor_and_restores_method(tmp_path, monkeypatch):
    from databricks.sql.client import Cursor  # pylint: disable=import-outside-toplevel

    calls = []

    def original(instance, statement, parameters=None):
        calls.append((statement, parameters))
        return instance

    monkeypatch.setattr(Cursor, "execute", original)
    notebook = tmp_path / "connector.py"
    notebook.write_text(
        "from databricks.sql.client import Cursor\n"
        "cursor = Cursor.__new__(Cursor)\ncursor.active_command_id = None\n"
        "assert cursor.execute('SELECT :id', parameters={'id': 7}) is cursor\n"
    )
    values = {}
    dbutils = SimpleNamespace(widgets=SimpleNamespace(text=values.setdefault, get=values.__getitem__))
    report = execute(dbutils, None, notebook=notebook)
    assert calls == [("SELECT :id", {"id": 7})]
    assert len(report["dbsql"]) == 1
    assert report["dbsql"][0]["kind"] == "SELECT"
    assert report["dbsql"][0]["query_id"] is None
    assert report["dbsql"][0]["elapsed_seconds"] >= 0
    assert len(report["dbsql"][0]["sha256"]) == 64
    assert Cursor.execute is original


def test_runner_environment_is_restored(tmp_path, monkeypatch):
    monkeypatch.setenv("FACTORY_ACCEPTANCE_CONTEXT", "previous")
    values = {"probe_environment": "2"}
    dbutils = SimpleNamespace(widgets=SimpleNamespace(text=values.setdefault, get=values.__getitem__))
    notebook = tmp_path / "environment.py"
    notebook.write_text(
        "import os\nassert os.environ['FACTORY_ACCEPTANCE_CONTEXT'] == '2'\nraise RuntimeError('interrupted')\n"
    )
    with pytest.raises(RuntimeError, match="interrupted"):
        execute(dbutils, None, notebook=notebook)
    import os  # pylint: disable=import-outside-toplevel

    assert os.environ["FACTORY_ACCEPTANCE_CONTEXT"] == "previous"


def test_mutation_marker_precedes_actual_cursor_call(tmp_path, monkeypatch):
    from databricks.sql.client import Cursor  # pylint: disable=import-outside-toplevel

    uploaded = {}

    def upload(path, stream, overwrite):
        assert overwrite
        uploaded[path] = json.load(stream)

    def original(instance, statement):
        assert statement.startswith("/* dbt metadata */ CREATE TABLE")
        assert uploaded["/Volumes/catalog/scratch/artifacts/metrics/executing.json"] == {
            "kind": "CREATE",
            "task_key": "orders_model",
            "stage": "interrupt",
            "phase": "cursor_execute",
            "sha256": __import__("hashlib").sha256(statement.encode()).hexdigest(),
        }
        return instance

    monkeypatch.setattr(Cursor, "execute", original)
    notebook = tmp_path / "mutation.py"
    notebook.write_text(
        "from databricks.sql.client import Cursor\ncursor = Cursor.__new__(Cursor)\n"
        "cursor.active_command_id = None\ncursor.execute('/* dbt metadata */ CREATE TABLE target AS SELECT 1')\n"
    )
    values = {
        "report_dir": "/Volumes/catalog/scratch/artifacts/metrics",
        "probe_task_key": "orders_model",
        "probe_stage": "interrupt",
    }
    dbutils = SimpleNamespace(widgets=SimpleNamespace(text=values.setdefault, get=values.__getitem__))
    report = execute(dbutils, None, notebook=notebook, client=SimpleNamespace(files=SimpleNamespace(upload=upload)))
    assert report["dbsql"][0]["kind"] == "CREATE"


def test_context_variants_preserve_project_and_change_one_input(tmp_path, monkeypatch):
    from databricks_dbt_factory.reuse_prepare import snapshot_digest  # pylint: disable=import-outside-toplevel

    monkeypatch.setenv("DBT_ACCESS_TOKEN", "parse-only")
    snapshots, contexts = {}, {}
    for variant in ("macro", "vars", "environment"):
        root = tmp_path / variant
        config, variables = write_project(root, "example.databricks.com", "catalog", "scratch", "warehouse", variant)
        monkeypatch.setenv("FACTORY_ACCEPTANCE_CONTEXT", "2" if variant == "environment" else "1")
        manifest, provenance = capture_parse(
            root / "project",
            root / "profiles",
            identity=IDENTITY | {"project_id": config["project_id"], "environment": config["environment"]},
            target="probe",
            deployment_ref=variant,
            variables=variables,
            environment_inputs=["FACTORY_ACCEPTANCE_CONTEXT"],
        )
        snapshots[variant] = snapshot_digest(root / "project")
        contexts[variant] = json.loads(read_provenance(provenance, manifest).context.canonical_json)
    assert len(set(snapshots.values())) == 1
    assert contexts["macro"] | {"vars": {"factor": 2}} == contexts["vars"]
    assert contexts["vars"] | {"environment_inputs": {"FACTORY_ACCEPTANCE_CONTEXT": "2"}} == contexts["environment"]


def test_engine_upgrade_preserves_project_and_profiles(tmp_path):
    projects = []
    profiles = []
    settings = []
    for variant in ("base", "engine_previous"):
        root = tmp_path / variant
        settings.append(write_project(root, "example.databricks.com", "catalog", "scratch", "warehouse", variant))
        projects.append(
            {
                path.relative_to(root / "project").as_posix(): path.read_bytes()
                for path in (root / "project").rglob("*")
                if path.is_file()
            }
        )
        profiles.append((root / "profiles" / "profiles.yml").read_bytes())
    assert projects[0] == projects[1]
    assert profiles[0] == profiles[1]
    assert settings[0] == settings[1]


def test_partial_edit_only_invalidates_report_against_real_dbt(tmp_path, monkeypatch):
    from databricks_dbt_factory.reuse_prepare import compare_baselines  # pylint: disable=import-outside-toplevel

    monkeypatch.setenv("DBT_ACCESS_TOKEN", "parse-only")
    monkeypatch.setenv("FACTORY_ACCEPTANCE_CONTEXT", "1")
    captures, files = {}, {}
    for variant in ("base", "partial"):
        root = tmp_path / variant
        config, variables = write_project(root, "example.databricks.com", "catalog", "scratch", "warehouse", variant)
        captures[variant] = capture_parse(
            root / "project",
            root / "profiles",
            identity=IDENTITY | {"project_id": config["project_id"], "environment": config["environment"]},
            target="probe",
            deployment_ref=variant,
            variables=variables,
            environment_inputs=["FACTORY_ACCEPTANCE_CONTEXT"],
        )
        files[variant] = {
            path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*") if path.is_file()
        }
    assert files["base"].keys() == files["partial"].keys()
    assert {name for name in files["base"] if files["base"][name] != files["partial"][name]} == {
        "project/models/report.sql"
    }
    assert (
        files["partial"]["project/models/report.sql"]
        == files["base"]["project/models/report.sql"] + b" where o.id >= 0"
    )
    current = tmp_path / "partial"
    result = compare_baselines(
        current / "project",
        current / "profiles",
        *captures["partial"],
        baselines={
            "model.factory_acceptance.orders": captures["base"],
            "model.factory_acceptance.report": captures["base"],
        },
    )
    assert result["models"] == {
        "model.factory_acceptance.orders": "CODE_UNCHANGED",
        "model.factory_acceptance.report": "CODE_CHANGED",
    }
    assert len(result["comparisons"]) == 1
