# Databricks notebook source
# ruff: noqa: F821 - `spark` is provided by the notebook runtime
# MAGIC %md
# MAGIC # jaffle-shop model reuse benchmark
# MAGIC Off vs on per scenario. Run wall time is per job run; overhead is the control steps (reset, append)
# MAGIC plus the reuse preparation task. DBUs stay NULL until `bench.py backfill` (billing lags hours).

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
