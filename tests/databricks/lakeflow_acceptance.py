"""Exercise generated Lakeflow jobs and retain measured, attributable run evidence."""
# pylint: skip-file
# mypy: ignore-errors

import io
import hashlib
import json
import re
import shutil
import subprocess
import time
import uuid
import zipfile
from importlib import resources
from pathlib import Path


def prepare_acceptance_bundle(directory, catalog, schema, volume, warehouse, wheel, target):
    for name in (catalog, schema, volume, target):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("acceptance requires simple SQL/bundle identifiers")
    wheel = Path(wheel)
    if not wheel.is_file():
        raise ValueError("acceptance requires a built matching Factory wheel")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    for name in ("acceptance_setup.py", "acceptance_runner.py"):
        shutil.copyfile(Path(__file__).with_name(name), directory / name)
    shipped = resources.files("databricks_dbt_factory") / "notebook" / "prepare_dbt_reuse.py"
    (directory / "prepare_dbt_reuse.py").write_bytes(shipped.read_bytes())
    wheel_path = f"/Volumes/{catalog}/{schema}/{volume}/{wheel.name}"
    job = {
        "name": schema + "_bootstrap",
        "max_concurrent_runs": 1,
        "timeout_seconds": 1200,
        "tags": {"aidevkit_project": "dbt-factory", "purpose": "reuse-acceptance"},
        "parameters": [
            {"name": name, "default": value}
            for name, value in {
                "action": "setup",
                "variant": "base",
                "rows": "100000000",
            }.items()
        ],
        "tasks": [
            {
                "task_key": "bootstrap",
                "environment_key": "Default",
                "notebook_task": {
                    "notebook_path": "./acceptance_setup.py",
                    "base_parameters": {
                        "catalog": catalog,
                        "schema": schema,
                        "volume": volume,
                        "warehouse_id": warehouse,
                        "mode": "serverless",
                        "job_id": "{{job.id}}",
                        "run_id": "{{job.run_id}}",
                        "workspace_root": "${workspace.file_path}",
                        "wheel_path": wheel_path,
                        **{key: "{{job.parameters." + key + "}}" for key in ("action", "variant", "rows")},
                    },
                },
            }
        ],
        "environments": [
            {
                "environment_key": "Default",
                "spec": {
                    "environment_version": "4",
                    "dependencies": ["dbt-core==1.12.5", "dbt-databricks==1.12.7", wheel_path],
                },
            }
        ],
    }
    config = {
        "bundle": {"name": schema},
        "targets": {target: {"default": True}},
        "resources": {"jobs": {"bootstrap": job}},
    }
    (directory / "databricks.yml").write_text(json.dumps(config, indent=2) + "\n")
    return config


def check_stage(metrics, *, orders_reused=None, expected_models=None, expected_tests=1, failed_test=False):
    expected = {"orders_model", "required_gate_test"} | (set() if failed_test else {"report_model"})
    actual = [metric["task_key"] for metric in metrics]
    assert set(actual) == expected and len(actual) == len(expected), "task metrics missing or duplicated"
    failed = {metric["task_key"] for metric in metrics if not metric["success"]}
    assert failed == ({"required_gate_test"} if failed_test else set()), "unexpected task failure metrics"
    for metric in metrics:
        if metric["reason"] == "REUSED":
            assert not metric["imported_dbt"], "reused model imported dbt"
            assert not any(metric["invocations"].values()), "reused model initialized dbt"
    counts = {key: sum(item["invocations"][key] for item in metrics) for key in ("run", "test", "parse")}
    assert counts["test"] == expected_tests, f"required test invocation missing or duplicated: {counts}"
    if expected_models is not None:
        assert counts["run"] == expected_models, f"unexpected model execution count: {counts}"
    if orders_reused is not None:
        orders = [item for item in metrics if item["task_key"] == "orders_model"]
        assert len(orders) == 1, "orders task metric missing or duplicated"
        assert (orders[0]["reason"] == "REUSED") == orders_reused, orders
    return counts


def executing_query(queries, marker):
    for query in queries:
        if (
            query.status is not None
            and query.status.value == "RUNNING"
            and hashlib.sha256((query.query_text or "").encode()).hexdigest() == marker.get("sha256")
        ):
            return query.query_id
    return None


def completed_repair(run, repair_id):
    return any(item.get("id") == repair_id and item.get("end_time") for item in run.get("repair_history", []))


def read_run(client, run_id, *, deadline):
    try:
        return client.jobs.get_run(run_id, include_history=True)
    except TimeoutError:
        # The SDK already retries a request for five minutes. Allow one recovered connection.
        if time.monotonic() >= deadline:
            raise
        return client.jobs.get_run(run_id, include_history=True)


def run_acceptance(
    profile, catalog, schema, volume, warehouse_id, directory, target, wheel, *, scenario="acceptance", rows=100000000
):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import VolumeType

    try:
        from tests.databricks.reuse_acceptance import publish_probe_wheel
    except ModuleNotFoundError:
        from reuse_acceptance import publish_probe_wheel

    client = WorkspaceClient(profile=profile)
    control_warehouse_id = warehouse_id
    warehouse = client.warehouses.get(warehouse_id)
    if not warehouse.enable_serverless_compute:
        raise ValueError("acceptance requires a serverless SQL warehouse")
    directory = Path(directory).resolve()
    initial = prepare_acceptance_bundle(directory, catalog, schema, volume, warehouse_id, wheel, target)
    marker = "dbt-factory-acceptance:" + uuid.uuid4().hex
    volume_root = f"/Volumes/{catalog}/{schema}/{volume}"
    report = {
        "profile": profile,
        "catalog": catalog,
        "schema": schema,
        "volume": volume,
        "warehouse_id": warehouse_id,
        "target": target,
        "compute": "serverless",
        "scenario": scenario,
        "ownership_marker": marker,
        "runs": [],
        "statements": [],
        "checks": {},
        "billing": {},
        "task10_complete": False,
        "release_gate": "Measured positive net savings are required; production reuse remains disabled",
        "harness_sha256": {
            name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
            for name in (
                "lakeflow_acceptance.py",
                "acceptance_setup.py",
                "acceptance_runner.py",
                "acceptance_metrics.py",
            )
        },
    }
    created = deployed = False
    benchmark_warehouses = {}
    active_run = None
    job_ids = {}

    def save():
        (directory / "result.json").write_text(json.dumps(report, indent=2, default=str) + "\n")

    def bundle(*arguments):
        command = ["databricks", "bundle", *arguments, "--profile", profile, "--target", target]
        result = subprocess.run(command, cwd=directory, text=True, capture_output=True, check=False)
        if result.returncode:
            raise RuntimeError(result.stderr or result.stdout)
        return result.stdout

    def deploy():
        nonlocal deployed, job_ids
        bundle("validate", "--strict")
        deployed = True
        bundle("deploy", "--auto-approve")
        summary = json.loads(bundle("summary", "--output", "json"))
        job_ids = {key: int(value["id"]) for key, value in summary["resources"]["jobs"].items()}

    def sql(statement, *, parameters=None):
        from databricks.sdk.service.sql import StatementParameterListItem

        values = [StatementParameterListItem(name=key, value=str(value)) for key, value in (parameters or {}).items()]
        result = client.statement_execution.execute_statement(
            statement, control_warehouse_id, parameters=values, wait_timeout="0s"
        )
        report["statements"].append({"statement_id": result.statement_id, "kind": statement.split()[0]})
        deadline = time.monotonic() + 600
        while result.status.state.value in {"PENDING", "RUNNING"}:
            if time.monotonic() >= deadline:
                client.statement_execution.cancel_execution(result.statement_id)
                raise TimeoutError("acceptance SQL timed out")
            time.sleep(2)
            result = client.statement_execution.get_statement(result.statement_id)
        assert result.status.state.value == "SUCCEEDED", result.as_dict()
        chunk = result.result
        data = list(chunk.data_array or []) if chunk else []
        while chunk and chunk.next_chunk_index is not None:
            chunk = client.statement_execution.get_statement_result_chunk_n(result.statement_id, chunk.next_chunk_index)
            data.extend(chunk.data_array or [])
        return data

    def table(name):
        return ".".join("`" + part + "`" for part in (catalog, schema, name))

    def wait_run(run_id, stage, repair_id=None):
        nonlocal active_run
        active_run = run_id
        started = time.monotonic()
        deadline = started + 2400
        last = None
        while True:
            run = read_run(client, run_id, deadline=deadline)
            state = run.state.life_cycle_state.value
            if state != last:
                print(f"{stage}: run {run_id}: {state}", flush=True)
                last = state
            if state in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"} and (
                repair_id is None or completed_repair(run.as_dict(), repair_id)
            ):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{stage} exceeded 40 minutes")
            time.sleep(15)
        active_run = None
        result = {
            "stage": stage,
            "run_id": run_id,
            "run": run.as_dict(),
            "metrics": [],
            "outputs": {},
            "observed_wall_seconds": time.monotonic() - started,
        }
        for task in run.tasks or []:
            if task.run_id:
                output = client.jobs.get_run_output(task.run_id).as_dict()
                result["outputs"][task.task_key] = output
        report["runs"].append(result)
        save()
        return result

    def bootstrap(action, variant="base"):
        run_id = client.jobs.run_now(
            job_ids["bootstrap"],
            job_parameters={
                "action": action,
                "variant": variant,
                "rows": str(rows),
            },
        ).run_id
        result = wait_run(run_id, action + "_" + variant)
        assert result["run"]["state"]["result_state"] == "SUCCESS", result["outputs"]
        return json.loads(result["outputs"]["bootstrap"]["notebook_output"]["result"])

    def capture(variant):
        version = "1.12.4" if variant == "engine_previous" else "1.12.5"
        dependencies = initial["resources"]["jobs"]["bootstrap"]["environments"][0]["spec"]["dependencies"]
        dependencies[0] = "dbt-core==" + version
        current = json.loads((directory / "databricks.yml").read_text())
        if current["resources"]["jobs"]["bootstrap"] != initial["resources"]["jobs"]["bootstrap"]:
            current["resources"]["jobs"]["bootstrap"] = initial["resources"]["jobs"]["bootstrap"]
            (directory / "databricks.yml").write_text(json.dumps(current, indent=2) + "\n")
            deploy()
        captured = bootstrap("capture", variant)
        environment_value = "2" if variant == "environment" else "1"
        shipped = resources.files("databricks_dbt_factory") / "notebook" / "prepare_dbt_reuse.py"
        # Harness-only environment setup; the shipped preparation body is unchanged.
        (directory / "prepare_dbt_reuse.py").write_text(
            "# Databricks notebook source\nimport os\nos.environ['FACTORY_ACCEPTANCE_CONTEXT'] = "
            + repr(environment_value)
            + "\n"
            + shipped.read_text()
        )
        with client.files.download(captured["project_zip_path"]).contents as stream:
            content = stream.read()
        for name in ("project", "profiles"):
            shutil.rmtree(directory / name, ignore_errors=True)
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            for entry in archive.infolist():
                destination = (directory / entry.filename).resolve()
                if not destination.is_relative_to(directory):
                    raise ValueError("project archive escaped the bundle directory")
            archive.extractall(directory)
        with client.files.download(captured["generated_path"]).contents as stream:
            generated = json.load(stream)
        generated["targets"] = {target: {"default": True}}
        generated["resources"]["jobs"]["bootstrap"] = initial["resources"]["jobs"]["bootstrap"]
        for key, job in generated["resources"]["jobs"].items():
            if key == "bootstrap":
                continue
            defaults = {
                "probe_stage": "manual",
                "report_dir": volume_root + "/metrics/manual",
                "probe_reuse_enabled": "true",
                "probe_environment": environment_value,
            }
            job.setdefault("parameters", []).extend(
                {"name": name, "default": value} for name, value in defaults.items()
            )
            for task in job["tasks"]:
                if "run_job_task" in task:
                    task["run_job_task"].setdefault("job_parameters", {}).update(
                        {name: "{{job.parameters." + name + "}}" for name in defaults}
                    )
                if "notebook_task" not in task:
                    continue
                task["notebook_task"].setdefault("base_parameters", {}).update(
                    {
                        "probe_job_id": "{{job.id}}",
                        "probe_run_id": "{{job.run_id}}",
                        "probe_task_key": "{{task.name}}",
                        "probe_attempt": "{{task.execution_count}}",
                        **{name: "{{job.parameters." + name + "}}" for name in defaults},
                    }
                )
        (directory / "databricks.yml").write_text(json.dumps(generated, indent=2) + "\n")
        deploy()
        report["deployment"] = captured
        save()
        return generated

    def collect_metrics(result, stage):
        path = volume_root + "/metrics/" + stage
        for item in client.files.list_directory_contents(path):
            if not item.is_directory and item.path.endswith(".json"):
                with client.files.download(item.path).contents as stream:
                    metric = json.load(stream)
                    if "invocations" in metric:
                        assert metric["stage"] == stage, "stale stage metrics"
                        if not any("run_job_task" in task for task in result["run"].get("tasks", [])):
                            assert metric["run_id"] == str(result["run_id"]), "foreign run metrics"
                        result["metrics"].append(metric)
        for output in result["outputs"].values():
            payload = output.get("notebook_output", {}).get("result")
            if payload:
                parsed = json.loads(payload)
                if "preparation" in parsed and parsed["preparation"]:
                    with client.files.download(parsed["preparation"]["path"]).contents as stream:
                        prepared = json.load(stream)
                    result["preparation"] = {
                        "models": prepared["models"],
                        "comparison_count": len(prepared["comparisons"]),
                    }
        save()

    def run(stage, *, key="acceptance", expected="SUCCESS", models=None, reused=None):
        path = volume_root + "/metrics/" + stage
        client.files.create_directory(path)
        run_id = client.jobs.run_now(job_ids[key], job_parameters={"probe_stage": stage, "report_dir": path}).run_id
        result = wait_run(run_id, stage)
        collect_metrics(result, stage)
        assert result["run"]["state"]["result_state"] == expected, result["outputs"]
        if key == "acceptance" and any("dbt_task" in item for item in result["run"].get("tasks", [])):
            return result
        result["invocations"] = check_stage(
            result["metrics"], orders_reused=reused, expected_models=models, failed_test=expected == "FAILED"
        )
        save()
        return result

    def repair(failed, stage, *, prepare):
        path = volume_root + "/metrics/" + stage
        client.files.create_directory(path)
        tasks = ["factory_reuse_prepare_model", "orders_model"] if prepare else ["orders_model"]
        kwargs = {
            "rerun_tasks": tasks,
            "rerun_dependent_tasks": True,
            "job_parameters": {"probe_stage": stage, "report_dir": path},
        }
        previous = failed["run"].get("repair_history") or []
        if previous:
            kwargs["latest_repair_id"] = previous[-1]["id"]
        repair_id = client.jobs.repair_run(failed["run_id"], **kwargs).response.repair_id
        result = wait_run(failed["run_id"], stage, repair_id=repair_id)
        collect_metrics(result, stage)
        assert result["run"]["state"]["result_state"] == "SUCCESS", result["outputs"]
        result["invocations"] = check_stage(result["metrics"], orders_reused=True, expected_models=1)
        save()

    def interrupt_build():
        from databricks.sdk.service.sql import QueryFilter, QueryStatus, TimeRange

        nonlocal active_run
        stage = "interrupted_build"
        path = volume_root + "/metrics/" + stage
        client.files.create_directory(path)
        before = sql(f"SELECT key FROM {table('factory_reuse_builds')} ORDER BY key")
        started_ms = int(time.time() * 1000)
        run_id = client.jobs.run_now(
            job_ids["acceptance"],
            job_parameters={"probe_stage": stage, "report_dir": path, "factory_force_rebuild": "true"},
        ).run_id
        active_run = run_id
        deadline = time.monotonic() + 900
        query_id = None
        while time.monotonic() < deadline:
            try:
                with client.files.download(path + "/executing.json").contents as stream:
                    marker = json.load(stream)
            except Exception:
                marker = {}
            if marker.get("task_key") == "orders_model" and marker.get("stage") == stage:
                queries = client.query_history.list(
                    filter_by=QueryFilter(
                        query_start_time_range=TimeRange(start_time_ms=started_ms),
                        statuses=[QueryStatus.RUNNING],
                        warehouse_ids=[warehouse_id],
                    ),
                    max_results=100,
                )
                query_id = executing_query(queries.res or [], marker)
                if query_id:
                    break
            state = client.jobs.get_run(run_id).state.life_cycle_state.value
            assert state not in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}, "build ended before interruption"
            time.sleep(2)
        assert query_id, "No matching RUNNING warehouse build observed"
        client.jobs.cancel_run(run_id).result()
        result = wait_run(run_id, stage)
        assert result["run"]["state"]["result_state"] == "CANCELED", result["run"]["state"]
        after = sql(f"SELECT key FROM {table('factory_reuse_builds')} ORDER BY key")
        assert before == after, "interrupted build published successful evidence"
        result["interrupted_query_id"] = query_id
        save()

    def undeclared_lineage():
        sql(f"CREATE TABLE {table('hidden_input')} USING DELTA AS SELECT 1001 AS id")
        sql(f"INSERT INTO {table('orders')} (id) SELECT id FROM {table('hidden_input')}")
        deadline = time.monotonic() + 900
        while True:
            observed = sql(
                "SELECT event_time FROM system.access.table_lineage "
                "WHERE event_date >= current_date() - INTERVAL 1 DAY "
                "AND source_table_full_name = :source AND target_table_full_name = :target LIMIT 1",
                parameters={"source": f"{catalog}.{schema}.hidden_input", "target": f"{catalog}.{schema}.orders"},
            )
            if observed:
                report["checks"]["fresh_lineage_event"] = observed
                break
            assert time.monotonic() < deadline, "Fresh undeclared lineage was not ingested within 15 minutes"
            time.sleep(15)
        first = run("undeclared_lineage", models=2, reused=False)
        assert next(m for m in first["metrics"] if m["task_key"] == "orders_model")["reason"] == "UNDECLARED_INPUT"
        from databricks_dbt_factory.reuse import canonical_json

        stored = sql(
            f"SELECT key, evidence FROM {table('factory_reuse_decisions')} "
            "WHERE get_json_object(evidence, '$.kind') = 'lineage_blocker'"
        )
        blockers = []
        for key, content in stored:
            blocker = json.loads(content)
            assert hashlib.sha256(canonical_json(blocker).encode()).hexdigest() == key
            if (
                blocker["input"]
                == {"catalog": catalog.lower(), "schema": schema.lower(), "identifier": "hidden_input", "path": None}
                and blocker["output"] == {"catalog": catalog.lower(), "schema": schema.lower(), "identifier": "orders"}
                and blocker["scope"]["job_id"] == str(job_ids["acceptance"])
                and blocker["scope"]["project_id"] == "factory_acceptance"
                and blocker["scope"]["owner_key"] == "acceptance"
            ):
                blockers.append(blocker)
        assert len(blockers) == 1, "Scoped undeclared lineage was not persisted"
        report["checks"]["persisted_lineage_blocker"] = blockers[0]
        save()
        retained = run("undeclared_lineage_retained", models=2, reused=False)
        assert next(m for m in retained["metrics"] if m["task_key"] == "orders_model")["reason"] == "UNDECLARED_INPUT"

    try:
        bundle("validate", "--strict")
        client.schemas.create(name=schema, catalog_name=catalog, comment=marker)
        created = True
        client.volumes.create(catalog_name=catalog, schema_name=schema, name=volume, volume_type=VolumeType.MANAGED)
        report["wheel_sha256"] = publish_probe_wheel(client, wheel, volume_root)
        deploy()
        bootstrap("setup")
        if scenario == "acceptance":
            capture("base")
            run("initial", models=2, reused=False)
            run("unchanged", models=0, reused=True)
            sql(f"INSERT INTO {table('input_table')} VALUES (2)")
            run("input_append", models=2, reused=False)
            sql(f"INSERT INTO {table('orders')} VALUES (999)")
            run("output_mutation", models=2, reused=False)
            sql(f"DROP TABLE {table('orders')}")
            sql(f"CREATE TABLE {table('orders')} USING DELTA AS SELECT * FROM {table('input_table')}")
            run("output_recreated", models=2, reused=False)
            sql(
                f"ALTER TABLE {table('input_table')} SET TBLPROPERTIES ('factory_probe_maintenance' = 'metadata_commit')"
            )
            run("maintenance_commit", models=2, reused=False)
            capture("unrelated")
            run("unrelated_edit", models=0, reused=True)
            capture("partial")
            run("partial_deployment", models=1, reused=True)
            partial = run("mixed_baselines", models=0, reused=True)
            assert partial["preparation"]["comparison_count"] == 2, "Distinct successful manifests were not compared"
            capture("macro")
            run("transitive_macro", models=2, reused=False)
            capture("vars")
            run("vars_changed", models=2, reused=False)
            capture("environment")
            run("environment_changed", models=2, reused=False)
            for refreshed in (False, True):
                suffix = "with_prepare" if refreshed else "without_prepare"
                sql(f"UPDATE {table('test_control')} SET fail = true")
                failed = run("failed_test_" + suffix, expected="FAILED", models=0, reused=True)
                downstream = next(task for task in failed["run"]["tasks"] if task["task_key"] == "report_model")
                assert downstream["state"]["result_state"] in {"UPSTREAM_FAILED", "EXCLUDED", "SKIPPED"}, downstream
                sql(f"UPDATE {table('test_control')} SET fail = false")
                repair(failed, "repaired_" + suffix, prepare=refreshed)
            builds, saved = table("factory_reuse_builds"), table("saved_builds")
            sql(f"ALTER TABLE {builds} RENAME TO {saved}")
            try:
                run("state_outage", models=2, reused=False)
            finally:
                sql(f"ALTER TABLE {saved} RENAME TO {builds}")
            capture("engine_previous")
            run("previous_engine", models=2, reused=False)
            capture("base")
            run("engine_upgrade", models=2, reused=False)
            capture("expensive")
            run("before_interruption", models=2, reused=False)
            interrupt_build()
            run("interrupted_recovery", models=0, reused=True)
            undeclared_lineage()
            capture("split")
            run("split_initial", models=2, reused=False)
            run("split_unchanged", models=0, reused=True)
            capture("native")
            run("native_task")
            report["checks"]["generated_dag"] = True
        elif scenario == "benchmark":
            from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType

            report["benchmark_cohorts"] = []
            for variant in ("base", "expensive"):
                for arm in ("off", "on"):
                    cohort = {"variant": variant, "arm": arm, "stages": [], "created_ms": int(time.time() * 1000)}
                    name = schema + "_" + variant + "_" + arm + "_" + marker.rsplit(":", 1)[1]
                    warehouse_id = client.warehouses.create(
                        name=name,
                        cluster_size="2X-Small",
                        min_num_clusters=1,
                        max_num_clusters=1,
                        auto_stop_mins=5,
                        enable_serverless_compute=True,
                        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
                    ).response.id
                    benchmark_warehouses[warehouse_id] = name
                    cohort["warehouse_id"] = warehouse_id
                    report["benchmark_cohorts"].append(cohort)
                    save()
                    client.warehouses.stop(warehouse_id).result()
                    initial["resources"]["jobs"]["bootstrap"]["tasks"][0]["notebook_task"]["base_parameters"][
                        "warehouse_id"
                    ] = warehouse_id
                    sql(f"TRUNCATE TABLE {table('input_table')}")
                    sql(f"INSERT INTO {table('input_table')} VALUES (1)")
                    sql(f"UPDATE {table('test_control')} SET fail = false")
                    capture(variant)

                    def measured_run(suffix, **expect):
                        stage = variant + "_" + arm + "_" + suffix
                        cohort["stages"].append(stage)
                        return run(stage, key="baseline" if arm == "off" else "acceptance", **expect)

                    measured_run("first", models=2, reused=False if arm == "on" else None)
                    measured_run("repeat", models=0 if arm == "on" else 2, reused=True if arm == "on" else None)
                    sql(f"INSERT INTO {table('input_table')} VALUES (42)")
                    measured_run("one_input_changed", models=2, reused=False if arm == "on" else None)
                    sql(f"INSERT INTO {table('input_table')} VALUES (43)")
                    sql(f"UPDATE {table('test_control')} SET fail = false")
                    measured_run("many_inputs_changed", models=2, reused=False if arm == "on" else None)
                    client.warehouses.stop(warehouse_id).result()
                    if variant == "base":
                        capture("partial")
                        measured_run(
                            "partial_deployment", models=1 if arm == "on" else 2, reused=True if arm == "on" else None
                        )
                        partial = measured_run(
                            "mixed_baselines", models=0 if arm == "on" else 2, reused=True if arm == "on" else None
                        )
                        if arm == "on":
                            assert partial["preparation"]["comparison_count"] == 2
                        client.warehouses.stop(warehouse_id).result()
                    cohort["stopped_ms"] = int(time.time() * 1000)
                    save()
            report["checks"]["benchmark_runs"] = True
        else:
            raise ValueError("unsupported acceptance scenario")
        report["passed"] = True
    except BaseException as error:
        report.update(passed=False, error=str(error), error_type=type(error).__name__)
        raise
    finally:
        save()
        if active_run is not None:
            client.jobs.cancel_run(active_run).result()
        for cohort in report.get("benchmark_cohorts", []):
            if "stopped_ms" not in cohort:
                client.warehouses.stop(cohort["warehouse_id"]).result()
                cohort["stopped_ms"] = int(time.time() * 1000)
        try:
            from tests.databricks.acceptance_metrics import collect_measurements
        except ModuleNotFoundError:
            from acceptance_metrics import collect_measurements
        report["measurements"] = collect_measurements(sql, report["runs"], control_warehouse_id)
        for cohort in report.get("benchmark_cohorts", []):
            selected = [item for item in report["runs"] if item["stage"] in cohort["stages"]]
            cohort["measurements"] = collect_measurements(
                sql,
                selected,
                cohort["warehouse_id"],
                warehouse_window=(cohort["created_ms"], cohort["stopped_ms"]),
            )
        save()
        if deployed:
            bundle("destroy", "--auto-approve")
            report["jobs_cleaned_up"] = True
        if created:
            if client.schemas.get(catalog + "." + schema).comment != marker:
                raise RuntimeError("scratch schema ownership changed")
            client.schemas.delete(catalog + "." + schema, force=True)
            report["schema_cleaned_up"] = True
        for owned_id, name in benchmark_warehouses.items():
            owned = client.warehouses.get(owned_id)
            if owned.name != name:
                raise RuntimeError("benchmark warehouse ownership changed")
            client.warehouses.delete(owned_id)
        if benchmark_warehouses:
            report["benchmark_warehouses_cleaned_up"] = True
        save()
    return report
