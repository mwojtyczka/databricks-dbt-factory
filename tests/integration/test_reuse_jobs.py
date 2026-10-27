"""Lakeflow preparation over real parsed dbt graphs, without workspace access."""

import copy
import json
from dataclasses import replace

import pytest
import yaml

from databricks_dbt_factory.dbt_factory import DbtFactory
from databricks_dbt_factory.dbt_task import DbtTaskOptions
from databricks_dbt_factory.job_spec import render_job_spec, render_partitioned_job_spec
from databricks_dbt_factory.reuse import ReuseDeployment
from databricks_dbt_factory.reuse_prepare import capture_parse, read_provenance
from databricks_dbt_factory.task_factory import DbtDependencyResolver, ModelTaskFactory, TestTaskFactory as DbtTests
from tests.integration.dbt_helpers import PROFILES
from tests.test_reuse import CONFIG, IDENTITY


@pytest.fixture(scope="module")
def reuse_graph(tmp_path_factory):
    root = tmp_path_factory.mktemp("reuse-graph")
    project, profiles = root / "project", root / "profiles"
    (project / "models").mkdir(parents=True)
    (project / "tests").mkdir()
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(PROFILES)
    (project / "dbt_project.yml").write_text('name: probe\nversion: "1.0"\nconfig-version: 2\nprofile: probe\n')
    for name, sql in {
        "producer": "{{ config(materialized='table', tags=['bronze']) }} select 1 as id",
        "consumer": "{{ config(materialized='table', tags=['silver'], meta={'dbt_factory': {'reuse': "
        "{'enabled': true, 'extra_inputs': [{'catalog': 'hive_metastore', 'schema': 'default', "
        "'identifier': 'producer'}]}}}) }} select 1 as id",
        "factory_reuse_prepare": "{{ config(materialized='table', tags=['bronze'], "
        "meta={'dbt_factory': {'reuse': {'enabled': true}}}) }} select 1 as id",
    }.items():
        (project / "models" / f"{name}.sql").write_text(sql)
    (project / "tests" / "producer_gate.sql").write_text("select * from {{ ref('producer') }} where id is null")
    manifest, provenance = capture_parse(project, profiles, identity=IDENTITY, target="dev", deployment_ref="r1")
    settings = ReuseDeployment(
        CONFIG,
        read_provenance(provenance, manifest).context,
        {"path": "/Volumes/state/factory/artifacts/descriptor.json", "sha256": "a" * 64},
        "./prepare.py",
    )
    return json.loads(manifest), settings


def factory(settings, bundled=False):
    options = DbtTaskOptions(notebook_path="./runner.py", project_directory=".", profiles_directory="../profiles")
    resolver = DbtDependencyResolver()
    return DbtFactory(
        {
            "model": ModelTaskFactory(resolver, options, "--target dev"),
            "test": DbtTests(resolver, options, "--target dev"),
        },
        bundle_tests=bundled,
        reuse=settings,
    )


@pytest.mark.parametrize("bundled", [False, True])
def test_preparation_preserves_declared_input_test_gates_and_original_manifest(reuse_graph, bundled):
    manifest, settings = reuse_graph
    before = copy.deepcopy(manifest)
    tasks = factory(settings, bundled).create_tasks(manifest)
    assert manifest == before
    prep = [t for t in tasks if "factory_reuse_models" in t["notebook_task"]["base_parameters"]]
    assert len(prep) == 1
    assert prep[0]["task_key"] != "factory_reuse_prepare_model"
    assert not prep[0]["depends_on"]
    models = json.loads(prep[0]["notebook_task"]["base_parameters"]["factory_reuse_models"])
    assert models == ["model.probe.consumer", "model.probe.factory_reuse_prepare"]
    by_key = {t["task_key"]: t for t in tasks}
    consumer = by_key["consumer_model"]
    deps = {d["task_key"] for d in consumer["depends_on"]}
    assert prep[0]["task_key"] in deps
    assert ("producer_test" if bundled else "producer_gate_test") in deps
    assert consumer.get("run_if", "ALL_SUCCESS") == "ALL_SUCCESS"
    params = consumer["notebook_task"]["base_parameters"]
    assert params["factory_reuse_model"] == "model.probe.consumer"
    assert params["factory_reuse_preparation"] == (
        "{{tasks." + prep[0]["task_key"] + ".values.factory_reuse_preparation}}"
    )
    assert "factory_reuse_model" not in by_key["producer_model"]["notebook_task"]["base_parameters"]


def test_split_scopes_preparation_to_each_owning_job(reuse_graph):
    manifest, settings = reuse_graph
    partitions = factory(settings).create_partitions(manifest, ["bronze", "silver"])
    assert [p.name for p in partitions] == ["bronze", "silver"]
    assert partitions[1].depends_on == ["bronze"]
    for partition, uid in zip(partitions, ["model.probe.factory_reuse_prepare", "model.probe.consumer"]):
        prep = [t for t in partition.tasks if "factory_reuse_models" in t["notebook_task"]["base_parameters"]]
        assert len(prep) == 1
        assert json.loads(prep[0]["notebook_task"]["base_parameters"]["factory_reuse_models"]) == [uid]


def test_opt_out_preserves_ordinary_jobs(reuse_graph):
    manifest, settings = reuse_graph
    assert factory(replace(settings, context=None)).create_tasks(manifest) == factory(None).create_tasks(manifest)
    plain = copy.deepcopy(manifest)
    for node in plain["nodes"].values():
        node["config"]["meta"] = {}
    assert factory(settings).create_tasks(plain) == factory(None).create_tasks(plain)


@pytest.mark.parametrize("split", [False, True])
def test_reuse_jobs_validate_against_bundle_schema(reuse_graph, tmp_path, bundle_validator, split):
    manifest, settings = reuse_graph
    template = tmp_path / "template.yml"
    template.write_text(
        yaml.safe_dump(
            {
                "resources": {
                    "jobs": {
                        "probe": {
                            "name": "probe",
                            "environments": [{"environment_key": "Default", "spec": {"client": "4"}}],
                        }
                    }
                }
            }
        )
    )
    if split:
        output = render_partitioned_job_spec(
            str(template), factory(settings).create_partitions(manifest, ["bronze", "silver"])
        )
    else:
        output = render_job_spec(str(template), factory(settings).create_tasks(manifest))
    spec = yaml.safe_load(output)
    assert not list(bundle_validator.iter_errors(spec))
    for job in spec["resources"]["jobs"].values():
        assert job["max_concurrent_runs"] == 1
        assert job["parameters"] == [{"name": "factory_force_rebuild", "default": "false"}]
        for task in job["tasks"]:
            if "run_job_task" in task:
                assert task["run_job_task"]["job_parameters"] == {
                    "factory_force_rebuild": "{{job.parameters.factory_force_rebuild}}"
                }


@pytest.mark.parametrize(
    "conflict",
    [
        {"max_concurrent_runs": 2},
        {"parameters": [{"name": "factory_force_rebuild", "default": "true"}]},
        {"parameters": [{"name": "factory_reuse_model", "default": "other"}]},
        {"parameters": [{"name": "dbt_commands", "default": "[]"}]},
    ],
)
@pytest.mark.parametrize("split", [False, True])
def test_reuse_job_template_conflicts_are_refused(reuse_graph, tmp_path, conflict, split):
    manifest, settings = reuse_graph
    template = tmp_path / "template.yml"
    template.write_text(yaml.safe_dump({"resources": {"jobs": {"probe": {"name": "probe", **conflict}}}}))
    with pytest.raises(ValueError, match="reuse.*(concurr|parameter)"):
        if split:
            render_partitioned_job_spec(
                str(template), factory(settings).create_partitions(manifest, ["bronze", "silver"])
            )
        else:
            render_job_spec(str(template), factory(settings).create_tasks(manifest))


@pytest.fixture(scope="module")
def large_manifest(tmp_path_factory):
    root = tmp_path_factory.mktemp("reuse-task-limit")
    project, profiles = root / "project", root / "profiles"
    (project / "models").mkdir(parents=True)
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(PROFILES)
    (project / "dbt_project.yml").write_text('name: probe\nversion: "1.0"\nconfig-version: 2\nprofile: probe\n')
    for index in range(1000):
        policy = ", meta={'dbt_factory': {'reuse': {'enabled': true}}}" if index == 0 else ""
        (project / "models" / f"m{index:04}.sql").write_text(
            "{{ config(materialized='table', tags=['batch']" + policy + ") }} select 1 as id"
        )
    manifest, _ = capture_parse(project, profiles, identity=IDENTITY, target="dev", deployment_ref="large")
    return json.loads(manifest)


@pytest.mark.parametrize("split", [False, True])
def test_preparation_counts_in_final_task_limit(large_manifest, reuse_graph, split):
    manifest = copy.deepcopy(large_manifest)
    build = factory(reuse_graph[1])
    generate = (lambda value: build.create_partitions(value, ["batch"])[0].tasks) if split else build.create_tasks
    with pytest.raises(ValueError, match="1,001"):
        generate(manifest)
    del manifest["nodes"]["model.probe.m0999"]
    assert len(generate(manifest)) == 1000


@pytest.mark.parametrize(
    "bad_paths",
    [
        {"project_directory": None},
        {"profiles_directory": None},
        {"profiles_directory": "{{job.parameters.profiles}}"},
    ],
)
def test_preparation_requires_static_explicit_runtime_paths(reuse_graph, bad_paths):
    build = factory(reuse_graph[1])
    for task_factory in build.task_factories.values():
        task_factory.task_options = replace(task_factory.task_options, **bad_paths)
    with pytest.raises(ValueError, match="reuse.*(explicit|static)"):
        build.create_tasks(reuse_graph[0])


def test_preparation_metadata_shares_notebook_parameter_byte_limit(reuse_graph):
    manifest, settings = reuse_graph
    settings = replace(settings, config=settings.config | {"owner_key": "x" * 1_000_000})
    with pytest.raises(ValueError, match="base_parameters.*bytes"):
        factory(settings).create_tasks(manifest)


@pytest.mark.parametrize("problem", ["cycle", "ownership"])
def test_declared_graph_errors_are_refused_before_partitioning(reuse_graph, tmp_path, problem):
    project, profiles = tmp_path / "project", tmp_path / "profiles"
    (project / "models").mkdir(parents=True)
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(PROFILES)
    (project / "dbt_project.yml").write_text('name: probe\nversion: "1.0"\nconfig-version: 2\nprofile: probe\n')
    for name, other in (("one", "two"), ("two", "one")):
        extra = "'extra_inputs': [{'catalog': 'hive_metastore', 'schema': 'default', 'identifier': '" + other + "'}],"
        sql = (
            "{{ config(materialized='table', tags=['batch'], "
            + ("alias='Shared', " if name == "one" else "alias='shared', ") * (problem == "ownership")
            + "meta={'dbt_factory': {'reuse': {"
            + (extra if problem == "cycle" else "")
            + "'enabled': true}}}) }} select 1 as id"
        )
        (project / "models" / f"{name}.sql").write_text(sql)
    if problem == "ownership":
        # dbt Databricks refuses case-insensitive duplicate outputs before emitting a manifest.
        with pytest.raises(ValueError, match="dbt parse failed"):
            capture_parse(project, profiles, identity=IDENTITY, target="dev", deployment_ref="invalid")
        return
    manifest, _ = capture_parse(project, profiles, identity=IDENTITY, target="dev", deployment_ref="invalid")
    with pytest.raises(ValueError, match="reuse dependency cycle|ambiguous output relation"):
        factory(reuse_graph[1]).create_partitions(json.loads(manifest), ["batch"])
