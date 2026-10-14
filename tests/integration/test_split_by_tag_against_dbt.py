import json
import random
from pathlib import Path

import pytest
import yaml

from databricks_dbt_factory.main import main
from databricks_dbt_factory.utils import generate_task_key
from tests.conftest import create_dbt_factory
from tests.integration.conftest import PROFILES
from tests.integration.conftest import invoke_dbt as _dbt

TEMPLATE = Path(__file__).resolve().parent.parent / "test_data" / "job_definition_template.yaml"

SHOP_PROJECT = 'models:\n  probe:\n    finance:\n      +tags: ["finance"]\n'

SHOP_MODELS = {
    "staging/stg_orders.sql": "select 1 as id",
    "staging/stg_customers.sql": "select 1 as id",
    "finance/revenue.sql": "select id from {{ ref('stg_orders') }}",
    "marts/customers.sql": "select id from {{ ref('stg_customers') }}",
    "marts/campaign.sql": (
        "{{ config(tags=['marketing']) }}\n"
        "select r.id from {{ ref('revenue') }} r join {{ ref('customers') }} c using (id)"
    ),
}

SHOP_SCHEMA = """\
version: 2
models:
  - name: customers
    config:
      tags: ["marketing"]
  - name: revenue
    columns:
      - name: id
        data_tests: [not_null]
  - name: campaign
    columns:
      - name: id
        data_tests:
          - relationships:
              arguments: {to: ref('revenue'), field: id}
unit_tests:
  - name: revenue_keeps_ids
    model: revenue
    given: [{input: ref('stg_orders'), rows: [{id: 1}]}]
    expect: {rows: [{id: 1}]}
"""


def _parse(root: Path, models: dict[str, str], schema_yml: str = "", project_yml: str = "") -> dict:
    (root / "dbt_project.yml").write_text(
        f'name: probe\nprofile: probe\nversion: "1.0"\nconfig-version: 2\n{project_yml}', encoding="utf-8"
    )
    (root / "profiles.yml").write_text(PROFILES, encoding="utf-8")
    for path, sql in models.items():
        (root / "models" / path).parent.mkdir(parents=True, exist_ok=True)
        (root / "models" / path).write_text(sql, encoding="utf-8")
    if schema_yml:
        (root / "models" / "schema.yml").write_text(schema_yml, encoding="utf-8")
    result = _dbt(root, "parse", "--quiet")
    assert result.success, result.exception
    return json.loads((root / "target" / "manifest.json").read_text(encoding="utf-8"))


def _models_tagged(root: Path, tag: str) -> set[str]:
    result = _dbt(
        root,
        "ls",
        "--quiet",
        "--select",
        f"tag:{tag}",
        "--resource-type",
        "model",
        "--output",
        "json",
        "--output-keys",
        "unique_id",
    )
    assert result.success and isinstance(result.result, list), result.exception
    return {generate_task_key(json.loads(line)["unique_id"]) for line in result.result}


def _split(manifest: dict, *tags: str, bundle_tests: bool = False) -> dict:
    partitions = create_dbt_factory(bundle_tests=bundle_tests).create_partitions(manifest, tags)
    return {partition.name: partition for partition in partitions}


def _keys(partition) -> set[str]:
    return {task["task_key"] for task in partition.tasks}


@pytest.fixture
def shop(tmp_path) -> tuple[Path, dict]:
    return tmp_path, _parse(tmp_path, SHOP_MODELS, SHOP_SCHEMA, SHOP_PROJECT)


def test_each_job_holds_the_models_dbt_selects_by_its_tag(shop):
    root, manifest = shop
    jobs = _split(manifest, "finance", "marketing")

    models = {name: {key for key in _keys(job) if key.endswith("_model")} for name, job in jobs.items()}
    assert models == {
        "default": {"stg_orders_model", "stg_customers_model"},
        "finance": _models_tagged(root, "finance"),
        "marketing": _models_tagged(root, "marketing"),
    }
    assert models["finance"] == {"revenue_model"}
    assert models["marketing"] == {"customers_model", "campaign_model"}


def test_jobs_run_after_the_jobs_they_depend_on(shop):
    _, manifest = shop
    partitions = create_dbt_factory().create_partitions(manifest, ["finance", "marketing"])

    assert [(partition.name, partition.depends_on) for partition in partitions] == [
        ("default", []),
        ("finance", ["default"]),
        ("marketing", ["default", "finance"]),
    ]


@pytest.mark.parametrize("bundle_tests", [False, True])
def test_tests_run_in_the_last_job_of_their_parents(shop, bundle_tests):
    _, manifest = shop
    jobs = _split(manifest, "finance", "marketing", bundle_tests=bundle_tests)

    assert "relationships_campaign_id__id__ref_revenue__test" in _keys(jobs["marketing"])
    finance = _keys(jobs["finance"])
    if bundle_tests:
        assert finance == {"revenue_model", "revenue_test"}
    else:
        assert "not_null_revenue_id_test" in finance
        assert any("revenue_keeps_ids" in key for key in finance)


def test_a_test_spanning_independent_jobs_orders_them(tmp_path):
    manifest = _parse(
        tmp_path,
        {
            "left.sql": "{{ config(tags=['left']) }} select 1 as id",
            "right.sql": "{{ config(tags=['right']) }} select 1 as id",
        },
        "version: 2\nmodels:\n  - name: right\n    columns:\n      - name: id\n        data_tests:\n"
        "          - relationships:\n              arguments: {to: ref('left'), field: id}\n",
    )
    partitions = create_dbt_factory().create_partitions(manifest, ["left", "right"])

    holder = next(p for p in partitions if "relationships_right_id__id__ref_left__test" in _keys(p))
    assert ({"left", "right"} - {holder.name}).pop() in holder.depends_on


@pytest.mark.parametrize("bundle_tests", [False, True])
def test_source_tests_run_in_the_job_of_the_source_tags(tmp_path, bundle_tests):
    manifest = _parse(
        tmp_path,
        {"stg.sql": "select id from {{ source('raw', 'events') }}"},
        "version: 2\nsources:\n  - name: raw\n    tags: [ingest]\n    tables:\n      - name: events\n"
        "        columns:\n          - name: id\n            data_tests: [not_null]\n",
    )
    jobs = _split(manifest, "ingest", bundle_tests=bundle_tests)

    assert _keys(jobs["ingest"]) == {"raw_events_test" if bundle_tests else "source_not_null_raw_events_id_test"}
    assert _keys(jobs["default"]) == {"stg_model"}
    assert jobs["default"].depends_on == ["ingest"]


def test_a_pattern_makes_one_job_per_matching_tag(tmp_path):
    manifest = _parse(
        tmp_path,
        {
            "sales.sql": "{{ config(tags=['domain:sales']) }} select 1 as id",
            "ops.sql": "{{ config(tags=['domain:ops', 'nightly']) }} select 1 as id",
        },
    )

    assert {name: _keys(job) for name, job in _split(manifest, "domain:*").items()} == {
        "domain_ops": {"ops_model"},
        "domain_sales": {"sales_model"},
    }


def test_jobs_that_would_depend_on_each_other_are_refused(tmp_path):
    manifest = _parse(
        tmp_path,
        {
            "first.sql": "{{ config(tags=['finance']) }} select 1 as id",
            "second.sql": "{{ config(tags=['marketing']) }} select id from {{ ref('first') }}",
            "third.sql": "{{ config(tags=['finance']) }} select id from {{ ref('second') }}",
        },
    )

    with pytest.raises(ValueError, match="depend on each other") as error:
        create_dbt_factory().create_partitions(manifest, ["finance", "marketing"])
    assert all(name in str(error.value) for name in ("second_model", "third_model"))


def test_a_model_with_two_split_tags_is_refused(tmp_path):
    manifest = _parse(tmp_path, {"both.sql": "{{ config(tags=['finance', 'marketing']) }} select 1 as id"})

    with pytest.raises(ValueError, match=r"'both'.*finance, marketing"):
        create_dbt_factory().create_partitions(manifest, ["finance", "marketing"])


def test_cli_writes_a_valid_bundle_run_by_an_orchestrator(shop, monkeypatch, bundle_validator):
    root, _ = shop
    target = root / "resources" / "job.yaml"
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            str(root / "target" / "manifest.json"),
            "--input-job-spec-path",
            str(TEMPLATE),
            "--target-job-spec-path",
            str(target),
            "--split-by-tag",
            "finance",
            "--split-by-tag",
            "marketing",
        ],
    )

    main()

    spec = yaml.safe_load(target.read_text(encoding="utf-8"))
    assert not [error.message for error in bundle_validator.iter_errors(spec)]
    jobs = spec["resources"]["jobs"]
    assert [(task["task_key"], task["run_job_task"]["job_id"]) for task in jobs["dbt_sql_job"]["tasks"]] == [
        ("default", "${resources.jobs.dbt_sql_job_default.id}"),
        ("finance", "${resources.jobs.dbt_sql_job_finance.id}"),
        ("marketing", "${resources.jobs.dbt_sql_job_marketing.id}"),
    ]
    assert all(task["notebook_task"] for key in jobs if key != "dbt_sql_job" for task in jobs[key]["tasks"])


def test_cli_refuses_to_split_a_job_spec_in_place(shop, monkeypatch, capsys):
    root, _ = shop
    spec = root / "job.yaml"
    spec.write_bytes(TEMPLATE.read_bytes())
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--dbt-manifest-path",
            str(root / "target" / "manifest.json"),
            "--input-job-spec-path",
            str(spec),
            "--target-job-spec-path",
            str(root / "models" / ".." / "job.yaml"),
            "--split-by-tag",
            "finance",
            "--split-by-tag",
            "marketing",
        ],
    )

    with pytest.raises(SystemExit):
        main()
    assert "cannot update the job spec in place" in capsys.readouterr().err
    assert spec.read_bytes() == TEMPLATE.read_bytes()
    assert not list(root.glob("run_dbt_command_*.py"))


def test_a_split_tag_that_matches_nothing_is_refused(shop):
    _, manifest = shop

    with pytest.raises(ValueError, match="'finanse' matches no tag"):
        create_dbt_factory().create_partitions(manifest, ["finance", "finanse"])


def _random_layout(rng: random.Random) -> tuple[dict[str, str], str, list[str]]:
    jobs = ["a", "b", "c", None]
    rng.shuffle(jobs)
    rank: dict[str, int] = {}
    models: dict[str, str] = {}
    tests: dict[str, list[str]] = {}
    for index in range(12):
        name = f"m{index}"
        refs = rng.sample(sorted(rank), k=min(len(rank), rng.randint(0, 2)))
        rank[name] = rng.randint(max((rank[ref] for ref in refs), default=0), len(jobs) - 1)
        tag = jobs[rank[name]]
        config = f"{{{{ config(tags=['{tag}']) }}}}\n" if tag else ""
        models[f"{name}.sql"] = config + "select 1 as id\n" + "".join(f"-- {{{{ ref('{ref}') }}}}\n" for ref in refs)
        if rng.random() < 0.5:
            tests.setdefault(name, []).append("not_null")
    for left, right in {tuple(rng.sample(sorted(rank), 2)) for _ in range(3)}:
        tests.setdefault(left, []).append(f"relationships: {{arguments: {{to: ref('{right}'), field: id}}}}")
    schema = "version: 2\nmodels:\n" + "".join(
        f"  - name: {name}\n    columns:\n      - name: id\n        data_tests:\n"
        + "".join(f"          - {test}\n" for test in model_tests)
        for name, model_tests in sorted(tests.items())
    )
    return models, schema, sorted({tag for tag in (jobs[job] for job in rank.values()) if tag is not None})


@pytest.mark.parametrize("seed", range(8))
def test_splitting_keeps_every_task_and_dependency(tmp_path, seed):
    models, schema, tags = _random_layout(random.Random(seed))
    manifest = _parse(tmp_path, models, schema)

    for bundle_tests in (False, True):
        unsplit = {
            task["task_key"]: task for task in create_dbt_factory(bundle_tests=bundle_tests).create_tasks(manifest)
        }
        partitions = create_dbt_factory(bundle_tests=bundle_tests).create_partitions(manifest, tags)
        owner = {task["task_key"]: partition.name for partition in partitions for task in partition.tasks}
        assert sorted(owner) == sorted(unsplit) == sorted(task["task_key"] for p in partitions for task in p.tasks)

        upstream: dict[str, set[str]] = {}
        for partition in partitions:
            assert all(dependency in upstream for dependency in partition.depends_on)
            upstream[partition.name] = set(partition.depends_on).union(*(upstream[d] for d in partition.depends_on))
            for task in partition.tasks:
                _assert_kept(task, unsplit[task["task_key"]], owner, partition.name, upstream[partition.name])


def _assert_kept(task: dict, original: dict, owner: dict[str, str], job: str, upstream: set[str]) -> None:
    assert {**task, "depends_on": []} == {**original, "depends_on": []}
    assert all(owner[dependency["task_key"]] == job for dependency in task["depends_on"])
    for dependency in original["depends_on"]:
        kept_in_job = owner[dependency["task_key"]] == job
        assert dependency in task["depends_on"] if kept_in_job else owner[dependency["task_key"]] in upstream
