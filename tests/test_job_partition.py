from databricks_dbt_factory.dbt_task import DbtTask, DbtTaskOptions, TaskType
from databricks_dbt_factory.job_partition import TaskPlacement, partition_tasks


def _task(key: str, *depends_on: str) -> DbtTask:
    return DbtTask(key, [f"dbt build --select {key}"], DbtTaskOptions(task_type=TaskType.DBT), list(depends_on))


def test_a_test_depending_on_a_test_runs_in_the_job_of_the_resource_they_test():
    tasks = [_task("a_model"), _task("check_test", "a_model"), _task("recheck_test", "check_test")]
    tasks.append(_task("b_model", "recheck_test"))
    placements = {
        "a_model": TaskPlacement("model 'a'", frozenset({"a"}), False),
        "check_test": TaskPlacement("test 'check'", frozenset(), True),
        "recheck_test": TaskPlacement("test 'recheck'", frozenset(), True),
        "b_model": TaskPlacement("model 'b'", frozenset({"b"}), False),
    }

    jobs = partition_tasks(tasks, placements, ["a", "b"])

    assert [(job.name, [task["task_key"] for task in job.tasks], job.depends_on) for job in jobs] == [
        ("a", ["a_model", "check_test", "recheck_test"], []),
        ("b", ["b_model"], ["a"]),
    ]
