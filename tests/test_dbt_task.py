from databricks_dbt_factory.DbtTask import DbtTask, DbtTaskOptions


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
    options = DbtTaskOptions(environment_key="Default")
    task = DbtTask(task_key="my_model", commands=["dbt run --select my_model"], options=options)

    result = task.to_dict()

    assert result["environment_key"] == "Default"
    assert "job_cluster_key" not in result
