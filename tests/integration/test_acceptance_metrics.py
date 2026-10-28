"""Check exact attribution and honest limits of acceptance measurement SQL."""

import pytest

from tests.databricks.acceptance_metrics import collect_measurements


def runs():
    return [
        {
            "run_id": 101,
            "run": {
                "start_time": 1000,
                "end_time": 9000,
                "tasks": [{"run_id": 102, "run_job_output": {"run_id": 103}}],
            },
            "metrics": [{"run_id": "103", "dbsql": [{"query_id": "cursor-id'"}]}],
        }
    ]


def test_queries_bind_exact_ids_and_time_bounds_and_preserve_evidence():
    calls = []

    def execute(statement, *, parameters):
        calls.append((statement, parameters))
        if "system.query.history" in statement:
            return [
                [
                    "cursor-id'",
                    "FINISHED",
                    "SELECT",
                    {"warehouse_id": "wh"},
                    3,
                    4,
                    5,
                    12,
                    7,
                    9,
                    10,
                    11,
                    12,
                    "start",
                    "end",
                    {"job_info": {"job_run_id": "103"}},
                ]
            ]
        return []

    evidence = collect_measurements(execute, runs(), "wh")
    assert len(calls) == 3
    query, parameters = calls[0]
    assert "statement_id IN (" in query
    assert "query_source.job_info.job_run_id IN (" in query
    assert set(parameters.values()) >= {"101", "102", "103", "cursor-id'", 1000, 9000}
    assert "cursor-id'" not in query
    assert "start_time >= timestamp_millis(CAST(:start_ms AS BIGINT))" in query
    assert "start_time < timestamp_millis(CAST(:end_ms AS BIGINT))" in query
    row = evidence["query_history"]["rows"][0]
    assert row["statement_id"] == "cursor-id'"
    assert row["total_task_duration_ms"] == 7
    assert row["query_source"]["job_info"]["job_run_id"] == "103"
    assert evidence["query_history"]["complete"] is False
    assert evidence["query_history"]["captured_statement_ids_complete"] is True
    assert "Spark" in evidence["query_history"]["reason"]
    assert evidence["warehouse_usage"]["allocation"] == "unallocated"
    assert evidence["job_billing"]["complete"] is False
    assert "savings" not in str(evidence).lower()


def test_billing_joins_effective_price_and_sums_signed_corrections():
    calls = []

    def execute(statement, *, parameters):
        calls.append((statement, parameters))
        return []

    collect_measurements(execute, runs(), "wh")
    billing, parameters = calls[1]
    assert "u.usage_metadata.job_run_id IN (" in billing
    for predicate in (
        "u.cloud = p.cloud",
        "u.sku_name = p.sku_name",
        "u.usage_unit = p.usage_unit",
        "u.usage_start_time >= p.price_start_time",
        "u.usage_end_time <= p.price_end_time",
        "p.price_end_time IS NULL",
    ):
        assert predicate in billing
    assert "COALESCE(p.pricing.effective_list.default, p.pricing.default)" in billing
    assert "SUM(u.usage_quantity * COALESCE(p.pricing.effective_list.default, p.pricing.default)) OVER" in billing
    assert "record_type =" not in billing
    assert "u.usage_start_time < timestamp_millis(CAST(:end_ms AS BIGINT))" in billing
    assert "u.usage_end_time > timestamp_millis(CAST(:start_ms AS BIGINT))" in billing
    assert set(parameters.values()) >= {"101", "102", "103"}
    warehouse, parameters = calls[2]
    assert "u.usage_metadata.warehouse_id = :warehouse_id" in warehouse
    assert parameters["warehouse_id"] == "wh"


def test_missing_or_inaccessible_telemetry_cannot_look_complete():
    def execute(_statement, *, parameters):
        assert parameters
        raise PermissionError("system tables denied")

    evidence = collect_measurements(execute, runs(), "wh")
    assert all(section["status"] == "unavailable" for section in evidence.values())
    assert all(section["complete"] is False for section in evidence.values())
    assert all(section["rows"] == [] for section in evidence.values())
    empty = collect_measurements(lambda *_args, **_kwargs: [], runs(), "wh")
    assert empty["query_history"]["status"] == "incomplete"
    assert empty["query_history"]["missing_statement_ids"] == ["cursor-id'"]


@pytest.mark.parametrize("run", [{"run_id": 1, "run": {}}, {"run_id": 1, "run": {"start_time": 9, "end_time": 1}}])
def test_missing_time_bounds_never_issue_unbounded_sql(run):
    def execute(*_args, **_kwargs):
        pytest.fail("unbounded query")

    result = collect_measurements(execute, [run], "wh")
    assert all(section["status"] == "unavailable" for section in result.values())


def test_run_only_attribution_stays_incomplete_without_cursor_ids():
    report = runs()
    report[0]["metrics"][0]["dbsql"] = [{"query_id": None}]
    calls = []

    def execute(statement, *, parameters):
        calls.append((statement, parameters))
        return []

    evidence = collect_measurements(execute, report, "wh")
    assert "statement_id IN (" not in calls[0][0]
    assert evidence["query_history"]["complete"] is False
    assert evidence["query_history"]["status"] == "incomplete"


def test_raw_correction_and_unpriced_rows_are_preserved_as_provisional():
    rows = [
        ['{"record_type":"ORIGINAL","usage_quantity":4}', "USD", "0.5", "start", None, "3", "1.5"],
        ['{"record_type":"RETRACTION","usage_quantity":-1}', "USD", "0.5", "start", None, "3", "1.5"],
        ['{"record_type":"RESTATEMENT","usage_quantity":2}', None, None, None, None, "2", None],
    ]

    def execute(statement, *, parameters):
        assert parameters
        return rows if "system.billing.usage" in statement else []

    evidence = collect_measurements(execute, runs(), "wh")
    billing = evidence["job_billing"]
    assert [row["usage_record"] for row in billing["rows"]] == [row[0] for row in rows]
    assert billing["rows"][2]["list_price"] is None
    assert billing["complete"] is False
    assert billing["status"] == "incomplete"


def test_interrupted_query_without_metrics_is_bound_and_counted_in_coverage():
    report = runs()
    report[0]["metrics"] = []
    report[0]["interrupted_query_id"] = "interrupted-id'"
    calls = []

    def execute(statement, *, parameters):
        calls.append((statement, parameters))
        return []

    evidence = collect_measurements(execute, report, "wh")
    query, parameters = calls[0]
    assert "statement_id IN (" in query
    assert "interrupted-id'" in parameters.values()
    assert "interrupted-id'" not in query
    history = evidence["query_history"]
    assert history["expected_statement_ids"] == ["interrupted-id'"]
    assert history["missing_statement_ids"] == ["interrupted-id'"]
    assert history["captured_statement_ids_complete"] is False
    assert history["complete"] is False
