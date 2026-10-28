"""Bounded system-table evidence; shared warehouse usage is never run-attributed."""

QUERY_COLUMNS = (
    "statement_id",
    "execution_status",
    "statement_type",
    "compute",
    "execution_duration_ms",
    "waiting_for_compute_duration_ms",
    "waiting_at_capacity_duration_ms",
    "total_duration_ms",
    "total_task_duration_ms",
    "read_bytes",
    "read_rows",
    "written_bytes",
    "written_rows",
    "start_time",
    "end_time",
    "query_source",
)
BILLING_COLUMNS = (
    "usage_record",
    "currency_code",
    "list_price",
    "price_start_time",
    "price_end_time",
    "net_usage_quantity",
    "net_list_cost",
)


# This test driver keeps the three bounded evidence queries together.
# pylint: disable=too-complex,too-many-locals
def collect_measurements(execute_sql, runs, warehouse_id):
    """Collect raw evidence from driver run dictionaries using named SQL parameters.

    Billing is provisional: system tables are delayed and subsequent corrections may
    change even populated results. List prices do not establish invoiced costs.
    """
    result = {
        "query_history": {
            "status": "unavailable",
            "complete": False,
            "rows": [],
            "captured_statement_ids_complete": False,
        },
        "job_billing": {"status": "unavailable", "complete": False, "rows": []},
        "warehouse_usage": {"status": "unavailable", "complete": False, "rows": [], "allocation": "unallocated"},
    }
    bounds = [(run.get("run", {}).get("start_time"), run.get("run", {}).get("end_time")) for run in runs]
    if not bounds or any(not start or not end or end <= start for start, end in bounds):
        for section in result.values():
            section["reason"] = "Missing or invalid run time bounds"
        return result
    parameters = {"start_ms": min(start for start, _ in bounds), "end_ms": max(end for _, end in bounds)}
    run_ids = set()

    def collect_ids(value):
        if isinstance(value, dict):
            if value.get("run_id") is not None:
                run_ids.add(str(value["run_id"]))
            for child in value.values():
                collect_ids(child)
        elif isinstance(value, list):
            for child in value:
                collect_ids(child)

    collect_ids(runs)
    query_ids = {
        str(event["query_id"])
        for run in runs
        for metric in run.get("metrics", [])
        for event in metric.get("dbsql", [])
        if event.get("query_id")
    }

    query_ids.update(str(run["interrupted_query_id"]) for run in runs if run.get("interrupted_query_id"))

    def placeholders(prefix, values):
        names = []
        for index, value in enumerate(sorted(values)):
            name = f"{prefix}_{index}"
            parameters[name] = value
            names.append(":" + name)
        return ", ".join(names)

    run_parameters = placeholders("run", run_ids)
    query_parameters = placeholders("query", query_ids)
    if not run_ids:
        for section in result.values():
            section["reason"] = "Missing actual run identifiers"
        return result
    predicates = [f"query_source.job_info.job_run_id IN ({run_parameters})"]
    if query_ids:
        predicates.append(f"statement_id IN ({query_parameters})")
    query = f"""SELECT {", ".join(QUERY_COLUMNS)}
FROM system.query.history
WHERE start_time >= timestamp_millis(CAST(:start_ms AS BIGINT))
  AND start_time < timestamp_millis(CAST(:end_ms AS BIGINT))
  AND ({" OR ".join(predicates)})
ORDER BY start_time, statement_id"""

    def collect(section, statement, columns, values):
        try:
            rows = execute_sql(statement, parameters=values)
            section["rows"] = [dict(zip(columns, row, strict=True)) for row in (rows or [])]
            section["status"] = "incomplete"
        except Exception as error:  # pylint: disable=broad-exception-caught
            # Telemetry availability must not replace acceptance results.
            section["reason"] = f"{type(error).__name__}: {error}"

    collect(result["query_history"], query, QUERY_COLUMNS, parameters.copy())
    history = result["query_history"]
    history["expected_statement_ids"] = sorted(query_ids)
    history["missing_statement_ids"] = sorted(query_ids - {row["statement_id"] for row in history["rows"]})
    if history["status"] != "unavailable":
        history["captured_statement_ids_complete"] = bool(query_ids) and not history["missing_statement_ids"]
        history["reason"] = (
            "Cursor identifier coverage is reported separately; serverless Spark history may be incomplete"
        )

    billing = """SELECT to_json(struct(u.*)) AS usage_record,
       p.currency_code, COALESCE(p.pricing.effective_list.default, p.pricing.default) AS list_price, p.price_start_time, p.price_end_time,
       SUM(u.usage_quantity) OVER (
           PARTITION BY u.cloud, u.sku_name, u.usage_unit, p.currency_code) AS net_usage_quantity,
       SUM(u.usage_quantity * COALESCE(p.pricing.effective_list.default, p.pricing.default)) OVER (
           PARTITION BY u.cloud, u.sku_name, u.usage_unit, p.currency_code) AS net_list_cost
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.cloud = p.cloud AND u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
  AND u.usage_start_time >= p.price_start_time
  AND (p.price_end_time IS NULL OR u.usage_end_time <= p.price_end_time)
WHERE u.usage_start_time < timestamp_millis(CAST(:end_ms AS BIGINT))
  AND u.usage_end_time > timestamp_millis(CAST(:start_ms AS BIGINT))
  AND """
    billing_parameters = {key: value for key, value in parameters.items() if not key.startswith("query_")}
    collect(
        result["job_billing"],
        billing + f"u.usage_metadata.job_run_id IN ({run_parameters})",
        BILLING_COLUMNS,
        billing_parameters,
    )
    warehouse_parameters = {
        "start_ms": parameters["start_ms"],
        "end_ms": parameters["end_ms"],
        "warehouse_id": warehouse_id,
    }
    collect(
        result["warehouse_usage"],
        billing + "u.usage_metadata.warehouse_id = :warehouse_id",
        BILLING_COLUMNS,
        warehouse_parameters,
    )
    for key in ("job_billing", "warehouse_usage"):
        section = result[key]
        if section["status"] != "unavailable":
            section["reason"] = "Provisional list-price evidence; delayed ingestion and corrections prevent finality"
    return result
