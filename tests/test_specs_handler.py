import pytest
import yaml

from databricks_dbt_factory.SpecsHandler import replace_tasks_in_job_spec


def _write(path, content: dict) -> str:
    path.write_text(yaml.safe_dump(content), encoding="utf-8")
    return str(path)


def test_replace_tasks_raises_keyerror_when_jobs_empty(tmp_path):
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": {}}})
    with pytest.raises(KeyError):
        replace_tasks_in_job_spec(spec, [{"task_key": "t"}], str(tmp_path / "out.yaml"))


def test_replace_tasks_raises_keyerror_when_resources_missing(tmp_path):
    spec = _write(tmp_path / "in.yaml", {"foo": "bar"})
    with pytest.raises(KeyError):
        replace_tasks_in_job_spec(spec, [{"task_key": "t"}], str(tmp_path / "out.yaml"))


def test_replace_tasks_raises_keyerror_when_resources_null(tmp_path):
    # `resources:` with a null value: the read chain must not raise AttributeError.
    spec = _write(tmp_path / "in.yaml", {"resources": None})
    with pytest.raises(KeyError):
        replace_tasks_in_job_spec(spec, [{"task_key": "t"}], str(tmp_path / "out.yaml"))


def test_replace_tasks_writes_tasks_into_first_job(tmp_path):
    spec = _write(tmp_path / "in.yaml", {"resources": {"jobs": {"my_job": {"tasks": []}}}})
    target = tmp_path / "out.yaml"

    replace_tasks_in_job_spec(spec, [{"task_key": "orders_model"}], str(target))

    written = yaml.safe_load(target.read_text(encoding="utf-8"))
    assert written["resources"]["jobs"]["my_job"]["tasks"] == [{"task_key": "orders_model"}]
