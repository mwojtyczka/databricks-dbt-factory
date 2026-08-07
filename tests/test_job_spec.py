import pytest
import yaml

from databricks_dbt_factory import job_spec
from databricks_dbt_factory.job_spec import replace_tasks_in_job_spec


def _write(path, content: dict) -> str:
    path.write_text(yaml.safe_dump(content), encoding="utf-8")
    return str(path)


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
