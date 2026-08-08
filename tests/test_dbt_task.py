import json

import pytest

from databricks_dbt_factory.DbtTask import DbtTask, DbtTaskOptions, TaskType


def _notebook_task_with_serialized_base_parameters_size(size: int) -> DbtTask:
    options = DbtTaskOptions(
        task_type=TaskType.NOTEBOOK,
        notebook_path="./runner.py",
        project_directory="prøject",
        profiles_directory="prøfiles",
    )
    empty_parameters = {
        "dbt_commands": json.dumps([""]),
        "project_directory": options.project_directory,
        "profiles_directory": options.profiles_directory,
    }
    fixed_size = len(json.dumps(empty_parameters, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    command = "x" * (size - fixed_size)
    expected_parameters = {**empty_parameters, "dbt_commands": json.dumps([command])}
    assert len(json.dumps(expected_parameters, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) == size
    return DbtTask(task_key="large_test_bundle", commands=[command], options=options)


def test_notebook_task_with_job_cluster_key():
    options = DbtTaskOptions(
        task_type="notebook",
        notebook_path="./notebooks/dbt_runner.py",
        source="WORKSPACE",
        project_directory="/project",
        profiles_directory="/profiles",
        job_cluster_key="dbt_cluster",
    )
    task = DbtTask(
        task_key="my_model",
        commands=["dbt run --select my_model --target dev"],
        options=options,
        depends_on=["upstream_model"],
    )

    result = task.to_dict()

    assert result["job_cluster_key"] == "dbt_cluster"
    assert "environment_key" not in result
    assert result["task_key"] == "my_model"
    assert result["depends_on"] == [{"task_key": "upstream_model"}]
    assert result["notebook_task"]["source"] == "WORKSPACE"
    assert result["notebook_task"]["base_parameters"]["project_directory"] == "/project"
    assert result["notebook_task"]["base_parameters"]["profiles_directory"] == "/profiles"


def test_notebook_task_without_job_cluster_key_uses_environment():
    options = DbtTaskOptions(
        task_type="notebook",
        notebook_path="./notebooks/dbt_runner.py",
    )
    task = DbtTask(task_key="my_model", commands=["dbt run --select my_model"], options=options)

    result = task.to_dict()

    assert result["environment_key"] == "Default"
    assert "job_cluster_key" not in result


def test_dbt_task_without_job_cluster_key_uses_environment():
    options = DbtTaskOptions(environment_key="Default", task_type=TaskType.DBT)
    task = DbtTask(task_key="my_model", commands=["dbt run --select my_model"], options=options)

    result = task.to_dict()

    assert result["environment_key"] == "Default"
    assert "job_cluster_key" not in result


def test_task_type_string_is_coerced_to_enum():
    options = DbtTaskOptions(task_type="notebook", notebook_path="./runner.py")
    assert options.task_type is TaskType.NOTEBOOK


def test_task_type_invalid_value_raises():
    with pytest.raises(ValueError, match="not a valid TaskType"):
        DbtTaskOptions(task_type="Notebook")
    with pytest.raises(ValueError, match="not a valid TaskType"):
        DbtTaskOptions(task_type="dbt_task")


def test_notebook_task_rejects_warehouse_schema_catalog():
    for kwargs in (
        {"warehouse_id": "wh123"},
        {"schema": "silver"},
        {"catalog": "main"},
    ):
        with pytest.raises(ValueError, match="notebook tasks connect via profiles.yml"):
            DbtTaskOptions(task_type=TaskType.NOTEBOOK, notebook_path="/n", **kwargs)


def test_notebook_task_rejects_multiple_incompatible_fields_at_once():
    with pytest.raises(ValueError, match=r"warehouse_id, schema, catalog"):
        DbtTaskOptions(
            task_type=TaskType.NOTEBOOK,
            notebook_path="/n",
            warehouse_id="wh123",
            schema="silver",
            catalog="main",
        )


def test_dbt_task_still_accepts_warehouse_schema_catalog():
    options = DbtTaskOptions(
        task_type=TaskType.DBT,
        warehouse_id="wh123",
        schema="silver",
        catalog="main",
    )
    assert options.warehouse_id == "wh123"


def test_task_type_defaults_to_notebook():
    assert DbtTaskOptions(notebook_path="./runner.py").task_type is TaskType.NOTEBOOK


def test_notebook_task_requires_notebook_path():
    with pytest.raises(ValueError, match="notebook_path is required"):
        DbtTaskOptions(task_type=TaskType.NOTEBOOK)


@pytest.mark.parametrize("size", [999_999, 1_000_000])
def test_notebook_task_accepts_base_parameters_at_or_below_one_megabyte(size):
    task = _notebook_task_with_serialized_base_parameters_size(size)

    result = task.to_dict()

    base_parameters = result["notebook_task"]["base_parameters"]
    assert len(json.dumps(base_parameters, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) == size


def test_notebook_task_rejects_base_parameters_above_one_megabyte():
    task = _notebook_task_with_serialized_base_parameters_size(1_000_001)

    with pytest.raises(
        ValueError,
        match=r"large_test_bundle.*1,000,001 bytes.*1,000,000 bytes.*bundle.*--task-type dbt",
    ):
        task.to_dict()


def test_native_dbt_task_is_not_subject_to_the_notebook_parameter_limit():
    command = "x" * 1_000_001
    task = DbtTask(
        task_key="large_native_task",
        commands=[command],
        options=DbtTaskOptions(task_type=TaskType.DBT),
    )

    result = task.to_dict()

    assert result["dbt_task"]["commands"] == [command]


def test_notebook_task_rejects_a_dynamic_reference_formed_across_serialized_commands():
    task = DbtTask(
        task_key="split_reference_test",
        commands=[
            "dbt test --select 'fqn:pkg.{{job.parameters.`first'",
            "dbt test --select 'fqn:pkg.second`}}'",
        ],
        options=DbtTaskOptions(task_type=TaskType.NOTEBOOK, notebook_path="./runner.py"),
    )

    with pytest.raises(ValueError, match="serialized dbt_commands.*dynamic value reference"):
        task.to_dict()
