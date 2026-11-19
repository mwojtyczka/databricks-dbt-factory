# Databricks notebook source
# ruff: noqa: F821 - `spark` is provided by the notebook runtime
# MAGIC %md
# MAGIC # jaffle-shop model reuse benchmark
# MAGIC Off vs on per scenario. Run wall time is per job run; overhead is the control steps (reset, append)
# MAGIC plus the reuse preparation task. Warehouse time and job DBUs are read from system tables when this
# MAGIC notebook runs; query history lags minutes and billing lags hours, so rerun it later for complete rows.

# COMMAND ----------

RESULTS = "test_jg_catalog.jaffle_bench_results.reuse_benchmark_runs"

# COMMAND ----------

# Off vs on: median arm-job wall time per scenario, plus the reuse outcome of the on arm.
spark.sql(f"""
WITH runs AS (
  SELECT benchmark_run_id, rep, arm, scenario, job_run_id, MAX(run_seconds) AS run_seconds
  FROM {RESULTS} WHERE step_kind = 'run' GROUP BY ALL
)
SELECT scenario, arm, COUNT(*) AS runs, ROUND(AVG(run_seconds), 1) AS avg_run_seconds,
       ROUND(MIN(run_seconds), 1) AS min_run_seconds, ROUND(MAX(run_seconds), 1) AS max_run_seconds
FROM runs GROUP BY scenario, arm ORDER BY scenario, arm
""").display()

# COMMAND ----------

# Per task on the on arm: what executed, what was reused and why.
spark.sql(f"""
SELECT benchmark_run_id, rep, scenario, task_key, task_state, ROUND(task_seconds, 1) AS task_seconds,
       dbt_run_invocations, dbt_test_invocations, reuse_reason, reused
FROM {RESULTS} WHERE arm = 'on' AND step_kind = 'run' ORDER BY benchmark_run_id, rep, scenario, task_key
""").display()

# COMMAND ----------

# Overhead of control steps and the reuse preparation task, and savings against the off arm.
spark.sql(f"""
SELECT benchmark_run_id, rep, arm, scenario, step_kind, ROUND(SUM(task_seconds), 1) AS seconds
FROM {RESULTS} WHERE step_kind IN ('reset', 'append') OR task_key LIKE '%prepare%'
GROUP BY ALL ORDER BY benchmark_run_id, rep, arm, scenario
""").display()

# COMMAND ----------

# Run-over-run history of cold and append wall times.
spark.sql(f"""
SELECT benchmark_run_id, arm, scenario, MIN(recorded_at) AS recorded_at, MAX(run_seconds) AS run_seconds, SUM(dbus) AS dbus
FROM {RESULTS} WHERE step_kind = 'run' GROUP BY ALL ORDER BY recorded_at
""").display()

# COMMAND ----------

# Warehouse work and serverless job DBUs per arm job run. The benchmark owns its warehouse and runs one job at a
# time, so every warehouse query inside a job run's window belongs to that run.
spark.sql(f"""
WITH runs AS (
  SELECT DISTINCT benchmark_run_id, arm, scenario, CAST(job_run_id AS STRING) AS job_run_id
  FROM {RESULTS} WHERE step_kind = 'run'
),
windows AS (
  SELECT CAST(run_id AS STRING) AS run_id, MIN(period_start_time) AS s, MAX(period_end_time) AS e
  FROM system.lakeflow.job_run_timeline WHERE CAST(run_id AS STRING) IN (SELECT job_run_id FROM runs) GROUP BY ALL
),
warehouses AS (SELECT DISTINCT warehouse_id FROM system.compute.warehouses WHERE warehouse_name LIKE '%jaffle-bench%'),
queries AS (
  SELECT w.run_id, SUM(h.total_duration_ms) / 1000 AS warehouse_seconds, COUNT(*) AS queries
  FROM windows w JOIN system.query.history h
    ON h.compute.warehouse_id IN (SELECT warehouse_id FROM warehouses) AND h.start_time BETWEEN w.s AND w.e
  GROUP BY ALL
),
dbus AS (
  SELECT CAST(usage_metadata.job_run_id AS STRING) AS run_id, SUM(usage_quantity) AS job_dbus
  FROM system.billing.usage WHERE CAST(usage_metadata.job_run_id AS STRING) IN (SELECT job_run_id FROM runs) GROUP BY ALL
)
SELECT r.benchmark_run_id, r.scenario, r.arm, ROUND(q.warehouse_seconds, 1) AS warehouse_seconds, q.queries,
       ROUND(d.job_dbus, 3) AS job_dbus
FROM runs r LEFT JOIN queries q ON q.run_id = r.job_run_id LEFT JOIN dbus d ON d.run_id = r.job_run_id
ORDER BY r.benchmark_run_id, r.scenario, r.arm
""").display()
