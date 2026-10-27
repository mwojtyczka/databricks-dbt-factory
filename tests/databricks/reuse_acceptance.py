"""Run the disposable serverless metadata probe with an explicit profile/catalog."""

import argparse
import json
import shutil
import subprocess
import time
import uuid
from pathlib import Path


def prepare_bundle(directory, catalog, schema, volume, warehouse_id):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(Path(__file__).with_name("metadata_probe.py"), directory / "metadata_probe.py")
    config = {
        "bundle": {"name": schema},
        "targets": {"probe": {"default": True}},
        "resources": {
            "jobs": {
                "probe": {
                    "name": schema,
                    "max_concurrent_runs": 1,
                    "timeout_seconds": 1200,
                    "tags": {"aidevkit_project": "dbt-factory", "purpose": "reuse-probe"},
                    "tasks": [
                        {
                            "task_key": "serverless",
                            "environment_key": "probe",
                            "notebook_task": {
                                "notebook_path": "./metadata_probe.py",
                                "base_parameters": {
                                    "catalog": catalog,
                                    "schema": schema,
                                    "volume": volume,
                                    "warehouse_id": warehouse_id,
                                    "mode": "serverless",
                                },
                            },
                        }
                    ],
                    "environments": [
                        {
                            "environment_key": "probe",
                            "spec": {
                                "environment_version": "4",
                                "dependencies": ["dbt-core==1.12.5", "dbt-databricks==1.12.7"],
                            },
                        }
                    ],
                },
            },
        },
    }
    (directory / "databricks.yml").write_text(json.dumps(config, indent=2) + "\n")


def collect_result(client, run_id, report, report_path):
    deadline = time.monotonic() + 1500
    while True:
        run = client.jobs.get_run(run_id=run_id)
        state = run.state.life_cycle_state.value
        print(f"Run {run_id}: {state}", flush=True)
        if state in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Probe run {run_id} exceeded 25 minutes")
        time.sleep(20)
    report["run"] = run.as_dict()
    for task in run.tasks or []:
        output = client.jobs.get_run_output(run_id=task.run_id)
        report["output"] = output.as_dict()
        if output.notebook_output and output.notebook_output.result:
            report["probe"] = json.loads(output.notebook_output.result)
    # Failed notebooks persist their detailed report before raising.
    if "probe" not in report:
        with client.files.download(report_path).contents as stream:
            report["probe"] = json.load(stream)
    return run.state.result_state.value


def run_probe(profile, catalog, warehouse_id, directory):
    # One lifecycle keeps ownership and cleanup visible together.
    # pylint: disable=too-many-locals,too-many-try-statements
    # Optional live dependency: importing the offline bundle builder needs only stdlib.
    from databricks.sdk import WorkspaceClient  # pylint: disable=import-outside-toplevel
    from databricks.sdk.service.catalog import VolumeType  # pylint: disable=import-outside-toplevel

    client = WorkspaceClient(profile=profile)
    warehouse = client.warehouses.get(warehouse_id)
    if not warehouse.enable_serverless_compute:
        raise ValueError("The probe requires a serverless SQL warehouse")
    nonce = uuid.uuid4().hex
    schema = f"factory_reuse_probe_{nonce[:12]}"
    directory = Path(directory).resolve()
    prepare_bundle(directory, catalog, schema, "artifacts", warehouse_id)
    report = {"profile": profile, "catalog": catalog, "schema": schema, "compute": "serverless"}
    marker = f"dbt-factory-probe:{nonce}"
    full_schema = f"{catalog}.{schema}"
    created = False
    deployed = False
    run_id = None

    def bundle(*arguments):
        result = subprocess.run(  # noqa: S603
            ["databricks", "bundle", *arguments, "--profile", profile, "--target", "probe"],
            cwd=directory,
            text=True,
            capture_output=True,
            check=True,
        )
        return result.stdout

    def save():
        (directory / "result.json").write_text(json.dumps(report, indent=2, default=str) + "\n")

    try:
        bundle("validate", "--strict")
        client.schemas.create(name=schema, catalog_name=catalog, comment=marker)
        created = True
        client.volumes.create(
            catalog_name=catalog, schema_name=schema, name="artifacts", volume_type=VolumeType.MANAGED
        )
        deployed = True  # Also destroy resources after a partially failed deployment.
        bundle("deploy", "--auto-approve")
        summary = json.loads(bundle("summary", "--output", "json"))
        job_id = int(summary["resources"]["jobs"]["probe"]["id"])
        started = time.monotonic()
        run_id = client.jobs.run_now(job_id=job_id).run_id
        report.update(job_id=job_id, run_id=run_id)
        save()
        print(json.dumps(report), flush=True)
        result_state = collect_result(client, run_id, report, f"/Volumes/{catalog}/{schema}/artifacts/report.json")
        report["wall_seconds"] = time.monotonic() - started
        save()
        if result_state != "SUCCESS":
            raise RuntimeError(f"Probe run {run_id}: {result_state}")
    finally:
        save()
        # Never drop scratch data while a timed-out notebook is still using it.
        if run_id is not None and client.jobs.get_run(run_id).state.life_cycle_state.value not in {
            "TERMINATED",
            "SKIPPED",
            "INTERNAL_ERROR",
        }:
            client.jobs.cancel_run(run_id=run_id).result()
        if deployed:
            bundle("destroy", "--auto-approve")
            report["job_cleaned_up"] = True
        if created:
            if client.schemas.get(full_schema).comment != marker:
                raise RuntimeError(f"Refusing to delete {full_schema}: ownership marker changed")
            client.schemas.delete(full_schema, force=True)
            report["schema_cleaned_up"] = True
        save()
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--directory", required=True, help="Fresh local directory for bundle and results")
    args = parser.parse_args()
    run_probe(args.profile, args.catalog, args.warehouse_id, args.directory)
