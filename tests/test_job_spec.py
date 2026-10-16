import os
import stat

import pytest
import yaml

from databricks_dbt_factory import job_spec
from databricks_dbt_factory.job_partition import JobPartition
from databricks_dbt_factory.job_spec import render_partitioned_job_spec, replace_tasks_in_job_spec


def _write(path, content: dict) -> str:
    path.write_text(yaml.safe_dump(content), encoding="utf-8")
    return str(path)


_TEMPLATE = {
    "name": "shop",
    "schedule": {"quartz_cron_expression": "0 0 8 * * ?", "timezone_id": "UTC"},
    "email_notifications": {"on_failure": ["data@example.com"]},
    "health": {"rules": [{"metric": "RUN_DURATION_SECONDS", "op": "GREATER_THAN", "value": 3600}]},
    "max_concurrent_runs": 3,
    "queue": {"enabled": True},
    "environments": [{"environment_key": "Default", "spec": {"environment_version": "4"}}],
}

_PARTITIONS = [
    JobPartition("default", [{"task_key": "stg_model", "depends_on": []}], []),
    JobPartition("finance", [{"task_key": "revenue_model", "depends_on": []}], ["default"]),
]


def test_split_spec_runs_the_jobs_from_an_orchestrator_holding_the_triggers(tmp_path):
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": {"shop": _TEMPLATE}}})

    jobs = yaml.safe_load(render_partitioned_job_spec(spec, _PARTITIONS))["resources"]["jobs"]

    assert jobs == {
        "shop": {
            "name": "shop",
            "schedule": _TEMPLATE["schedule"],
            "email_notifications": _TEMPLATE["email_notifications"],
            "health": _TEMPLATE["health"],
            "max_concurrent_runs": 3,
            "queue": {"enabled": True},
            "tasks": [
                {
                    "task_key": "default",
                    "depends_on": [],
                    "run_job_task": {"job_id": "${resources.jobs.shop_default.id}"},
                },
                {
                    "task_key": "finance",
                    "depends_on": [{"task_key": "default"}],
                    "run_job_task": {"job_id": "${resources.jobs.shop_finance.id}"},
                },
            ],
        },
        "shop_default": {
            "name": "shop_default",
            "max_concurrent_runs": 3,
            "queue": {"enabled": True},
            "environments": _TEMPLATE["environments"],
            "tasks": _PARTITIONS[0].tasks,
        },
        "shop_finance": {
            "name": "shop_finance",
            "max_concurrent_runs": 3,
            "queue": {"enabled": True},
            "environments": _TEMPLATE["environments"],
            "tasks": _PARTITIONS[1].tasks,
        },
    }


def test_split_spec_renames_the_orchestrator_and_its_jobs(tmp_path):
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": {"shop": _TEMPLATE}}})

    jobs = yaml.safe_load(render_partitioned_job_spec(spec, _PARTITIONS, "nightly"))["resources"]["jobs"]

    assert {key: job["name"] for key, job in jobs.items()} == {
        "nightly": "nightly",
        "nightly_default": "nightly_default",
        "nightly_finance": "nightly_finance",
    }


def test_split_spec_refuses_to_overwrite_an_existing_job(tmp_path):
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": {"shop": _TEMPLATE, "shop_finance": {"name": "x"}}}})

    with pytest.raises(ValueError, match="shop_finance"):
        render_partitioned_job_spec(spec, _PARTITIONS)


def test_replace_tasks_raises_valueerror_when_jobs_empty(tmp_path):
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": {}}})
    with pytest.raises(ValueError):
        replace_tasks_in_job_spec(spec, [{"task_key": "t"}], str(tmp_path / "out.yaml"))


def test_replace_tasks_raises_valueerror_when_resources_missing(tmp_path):
    spec = _write(tmp_path / "in.yaml", {"foo": "bar"})
    with pytest.raises(ValueError):
        replace_tasks_in_job_spec(spec, [{"task_key": "t"}], str(tmp_path / "out.yaml"))


def test_replace_tasks_raises_valueerror_when_resources_null(tmp_path):
    # `resources:` with a null value: the read chain must not raise AttributeError.
    spec = _write(tmp_path / "in.yaml", {"resources": None})
    with pytest.raises(ValueError):
        replace_tasks_in_job_spec(spec, [{"task_key": "t"}], str(tmp_path / "out.yaml"))


def test_replace_tasks_raises_valueerror_when_resources_is_not_a_mapping(tmp_path):
    # `resources:` holding a list: `.get('resources').get(...)` raised AttributeError, which
    # `main`'s `except (ValueError, FileNotFoundError)` does not catch, so the CLI printed a
    # traceback for a malformed *input file*.
    spec = _write(tmp_path / "in.yaml", {"resources": [1, 2]})
    with pytest.raises(ValueError):
        replace_tasks_in_job_spec(spec, [{"task_key": "t"}], str(tmp_path / "out.yaml"))


def test_replace_tasks_raises_valueerror_when_jobs_is_not_a_mapping(tmp_path):
    # `jobs:` holding a list: indexing it with `['jobs']` and then `next(iter(...))` raised
    # TypeError, likewise escaping the CLI's error handling.
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": ["a", "b"]}})
    with pytest.raises(ValueError):
        replace_tasks_in_job_spec(spec, [{"task_key": "t"}], str(tmp_path / "out.yaml"))


@pytest.mark.parametrize("job_value", ["just a string", ["a", "b"], 7], ids=["string", "list", "int"])
def test_replace_tasks_raises_valueerror_when_the_job_is_not_a_mapping(tmp_path, job_value):
    # The third level: `jobs` is a dict, but the job it holds is not. The code assigns
    # `first_job['tasks']`, so a non-mapping raised TypeError — which `main`'s
    # `except (ValueError, FileNotFoundError)` does not catch, printing a traceback for a malformed
    # *input file*. Same family as the `resources`/`jobs` cases above; guarding one level at a time is
    # what let this round trip three times.
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": {"my_job": job_value}}})
    with pytest.raises(ValueError):
        replace_tasks_in_job_spec(spec, [{"task_key": "t"}], str(tmp_path / "out.yaml"))


def test_replace_tasks_refuses_to_overwrite_a_different_job_with_new_name(tmp_path):
    # Two jobs; --new-job-name equals the *second* job's key. Renaming the first job onto that key
    # (`jobs[new] = jobs.pop(first)`) would silently drop `beta`'s whole definition. Refuse rather
    # than destroy an unrelated job.
    spec = _write(
        tmp_path / "in.yaml",
        {"resources": {"jobs": {"alpha": {"tasks": []}, "beta": {"tasks": [{"task_key": "keep"}]}}}},
    )
    with pytest.raises(ValueError, match="beta"):
        replace_tasks_in_job_spec(spec, [{"task_key": "t"}], str(tmp_path / "out.yaml"), new_job_name="beta")


def test_replace_tasks_allows_new_name_equal_to_first_job_key(tmp_path):
    # Renaming the first job to the key it already has is a no-op rename, not a collision.
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": {"alpha": {"tasks": []}}}})
    target = tmp_path / "out.yaml"

    replace_tasks_in_job_spec(spec, [{"task_key": "t"}], str(target), new_job_name="alpha")

    written = yaml.safe_load(target.read_text(encoding="utf-8"))
    assert written["resources"]["jobs"]["alpha"]["tasks"] == [{"task_key": "t"}]
    assert written["resources"]["jobs"]["alpha"]["name"] == "alpha"


def test_replace_tasks_writes_tasks_into_first_job(tmp_path):
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": {"my_job": {"tasks": []}}}})
    target = tmp_path / "out.yaml"

    replace_tasks_in_job_spec(spec, [{"task_key": "orders_model"}], str(target))

    written = yaml.safe_load(target.read_text(encoding="utf-8"))
    assert written["resources"]["jobs"]["my_job"]["tasks"] == [{"task_key": "orders_model"}]


def test_replace_tasks_in_place_update(tmp_path):
    # input path == target path: the file is updated in place, tasks replaced, other content kept.
    path = tmp_path / "job.yaml"
    _write(path, {"resources": {"jobs": {"my_job": {"name": "keep", "tasks": [{"task_key": "old"}]}}}})

    replace_tasks_in_job_spec(str(path), [{"task_key": "new"}], str(path))

    written = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert written["resources"]["jobs"]["my_job"]["tasks"] == [{"task_key": "new"}]
    assert written["resources"]["jobs"]["my_job"]["name"] == "keep"


@pytest.mark.skipif(os.name == "nt", reason="requires POSIX permission-bit semantics")
def test_new_target_inherits_input_template_mode(tmp_path):
    source = tmp_path / "in.yaml"
    spec = _write(source, {"resources": {"jobs": {"my_job": {"tasks": []}}}})
    source.chmod(0o640)
    target = tmp_path / "out.yaml"

    replace_tasks_in_job_spec(spec, [{"task_key": "new"}], str(target))

    assert stat.S_IMODE(target.stat().st_mode) == 0o640


@pytest.mark.skipif(os.name == "nt", reason="requires POSIX permission-bit semantics")
def test_existing_target_preserves_its_mode(tmp_path):
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": {"my_job": {"tasks": []}}}})
    target = tmp_path / "out.yaml"
    target.write_text("old\n", encoding="utf-8")
    target.chmod(0o604)

    replace_tasks_in_job_spec(spec, [{"task_key": "new"}], str(target))

    assert stat.S_IMODE(target.stat().st_mode) == 0o604


@pytest.mark.parametrize(
    "target_kind",
    [
        "directory",
        pytest.param(
            "symlink",
            marks=pytest.mark.skipif(
                os.name == "nt", reason="Windows symlink creation may require elevated privileges"
            ),
        ),
    ],
)
def test_non_regular_target_is_rejected(tmp_path, target_kind):
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": {"my_job": {"tasks": []}}}})
    target = tmp_path / "out.yaml"
    if target_kind == "directory":
        target.mkdir()
    else:
        link_target = tmp_path / "linked.yaml"
        link_target.write_text("keep\n", encoding="utf-8")
        os.symlink(link_target, target)

    with pytest.raises(ValueError, match="regular non-symlink file"):
        replace_tasks_in_job_spec(spec, [{"task_key": "new"}], str(target))


@pytest.mark.skipif(os.name == "nt", reason="Windows symlink creation may require elevated privileges")
def test_prepared_target_stays_anchored_when_a_parent_symlink_changes(tmp_path):
    source = tmp_path / "in.yaml"
    _write(source, {"resources": {"jobs": {"my_job": {"tasks": []}}}})
    first_directory = tmp_path / "first"
    second_directory = tmp_path / "second"
    first_directory.mkdir()
    second_directory.mkdir()
    linked_directory = tmp_path / "current"
    linked_directory.symlink_to(first_directory, target_is_directory=True)

    destination = job_spec.resolve_job_spec_destination(linked_directory / "out.yaml")
    artifact = job_spec.prepare_job_spec("rendered\n", str(source), destination)
    linked_directory.unlink()
    linked_directory.symlink_to(second_directory, target_is_directory=True)

    job_spec.write_job_spec(artifact)

    assert (first_directory / "out.yaml").read_text(encoding="utf-8") == "rendered\n"
    assert not (second_directory / "out.yaml").exists()


def test_replace_tasks_write_is_atomic_on_failure(tmp_path, monkeypatch):
    # If serialization fails, an existing in-place target is left intact and no temp file leaks.
    path = tmp_path / "job.yaml"
    original = {"resources": {"jobs": {"my_job": {"tasks": [{"task_key": "original"}]}}}}
    _write(path, original)

    def boom(*_args, **_kwargs):
        raise RuntimeError("serialization failed")

    monkeypatch.setattr(job_spec.yaml, "dump", boom)

    with pytest.raises(RuntimeError):
        replace_tasks_in_job_spec(str(path), [{"task_key": "new"}], str(path))

    # original untouched, and no stray .job_spec_*.tmp left behind
    assert yaml.safe_load(path.read_text(encoding="utf-8")) == original
    assert [p.name for p in tmp_path.iterdir()] == ["job.yaml"]
