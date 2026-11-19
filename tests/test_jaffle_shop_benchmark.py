"""Scenario sequencing and results-row extraction for examples/jaffle_shop_benchmark (no workspace needed)."""

import importlib.util
import sys
from pathlib import Path

_PATH = Path(__file__).resolve().parents[1] / "examples" / "jaffle_shop_benchmark" / "bench.py"
_SPEC = importlib.util.spec_from_file_location("jaffle_bench", _PATH)
assert _SPEC is not None and _SPEC.loader is not None
bench = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = bench
_SPEC.loader.exec_module(bench)


def test_plan_alternates_arm_order_and_runs_each_scenario_per_arm():
    steps = bench.plan(reps=2)
    runs = [(s.rep, s.arm, s.scenario) for s in steps if s.kind == "run"]
    assert runs == [
        (1, "off", "cold"), (1, "off", "rerun"), (1, "off", "append"), (1, "off", "edit"),
        (1, "on", "cold"), (1, "on", "rerun"), (1, "on", "append"), (1, "on", "edit"),
        (2, "on", "cold"), (2, "on", "rerun"), (2, "on", "append"), (2, "on", "edit"),
        (2, "off", "cold"), (2, "off", "rerun"), (2, "off", "append"), (2, "off", "edit"),
    ]  # fmt: skip
    narrow = [(s.arm, s.scenario) for s in bench.plan(1, ("on",), ("cold", "rerun")) if s.kind == "run"]
    assert narrow == [("on", "cold"), ("on", "rerun")]
    # Each arm starts from the base project and ends on the edit variant.
    assert [s.variant for s in steps if s.kind == "variant"][:2] == ["base", "edit"]
    first_run = next(i for i, s in enumerate(steps) if s.kind == "run")
    assert [s.kind for s in steps[:first_run]] == ["variant", "reset"]


def test_result_rows_flag_reuse_and_sum_only_known_query_times():
    meta = {"benchmark_run_id": "b", "rep": 1, "arm": "on", "scenario": "rerun", "step_kind": "run"}
    run = {"run_id": 7, "start_ms": 0, "end_ms": 60_000}
    tasks = [
        {"task_key": "orders_model", "state": "SUCCESS", "start_ms": 0, "end_ms": 5_000},
        {"task_key": "stg_orders_model", "state": "SUCCESS", "start_ms": 0, "end_ms": 2_000},
        {"task_key": "failing_test", "state": "FAILED", "start_ms": 0, "end_ms": None},
    ]
    metrics = [
        {"task_key": "orders_model", "reason": "REUSED", "invocations": {"run": 0, "test": 0},
         "dbsql": [{"query_id": "q1"}, {"query_id": "q-unknown"}]},
        {"task_key": "stg_orders_model", "reason": "UNDECLARED_INPUT", "invocations": {"run": 1, "test": 0},
         "dbsql": []},
    ]  # fmt: skip
    rows = {r["task_key"]: r for r in bench.result_rows(meta, run, tasks, metrics, {"q1": 1250.0})}
    assert rows["orders_model"]["reused"] is True
    assert rows["orders_model"]["warehouse_query_ms"] == 1250.0
    assert rows["stg_orders_model"]["reused"] is False
    assert rows["stg_orders_model"]["dbt_run_invocations"] == 1
    assert rows["stg_orders_model"]["warehouse_query_ms"] is None
    assert rows["failing_test"]["task_seconds"] is None
    assert rows["failing_test"]["reused"] is None
    assert rows["orders_model"]["run_seconds"] == 60.0


def test_insert_statement_quotes_values_and_uses_null_for_missing_cells():
    sql = bench.insert_statement([{"arm": "o'n", "rep": 1, "reused": True}])
    assert "'o''n'" in sql
    assert "NULL" in sql and "true" in sql
    assert sql.count("(") == sql.count(")") and sql.startswith("INSERT INTO")
