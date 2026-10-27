# Contributing

## Local Setup

This section provides a step-by-step guide to set up and start working on the project. These steps will help you set up your project environment and dependencies for efficient development.

To begin, install [Hatch](https://github.com/pypa/hatch), which is our build tool.

On MacOSX, this is achieved using the following:
```shell
brew install hatch
```

Run the following command to create the default environment and install development dependencies, assuming you've already cloned the github repo.
```shell
make dev
```

Before every commit, apply the consistent formatting of the code, as we want our codebase look consistent:
```shell
make fmt
```

Before every commit, run automated bug detector (`make lint`), unit tests (`make test`), and
integration tests (`make integration`) to ensure that automated pull request checks do pass,
before your code is reviewed by others:
```shell
make lint
make test
make integration
```

`make integration` builds the package and drives the installed `databricks_dbt_factory` CLI as a
real subprocess against the fixtures in `tests/test_data`, comparing the generated job spec to the
committed golden files. It also validates every fixture against the Declarative Automation Bundle schema
(generated on the fly with `databricks bundle schema`) to ensure the generated specs are valid DABs,
and runs the [end-to-end example](README.md#end-to-end-example) offline — invoking the CLI with the
documented arguments against the committed manifest and validating the freshly generated spec. It
requires no Databricks workspace and runs in CI alongside the unit tests.

The bundle-schema validation needs the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/)
on your `PATH`. If it is not installed those tests are skipped locally (but they are required and
run in CI, where the CLI is always installed).
## Local installation and execution

```shell
make build

# use hatch directly
hatch run databricks_dbt_factory \
  --dbt-manifest-path tests/test_data/manifest.json \
  --input-job-spec-path tests/test_data/job_definition_template.yaml \
  --target-job-spec-path job_definition_new.yaml \
  --source WORKSPACE \
  --target dev

# or install locally and run
pip install .
databricks_dbt_factory  \
  --dbt-manifest-path tests/test_data/manifest.json \
  --input-job-spec-path tests/test_data/job_definition_template.yaml \
  --target-job-spec-path job_definition_new.yaml \
  --source WORKSPACE \
  --target dev
```

## First contribution

Here are the example steps to submit your first contribution:

1. Make a Fork from the repo (if you really want to contribute)
2. `git clone`
3. `git checkout main` (or `gcm` if you're using [ohmyzsh](https://ohmyz.sh/)).
4. `git pull` (or `gl` if you're using [ohmyzsh](https://ohmyz.sh/)).
5. `git checkout -b FEATURENAME` (or `gcb FEATURENAME` if you're using [ohmyzsh](https://ohmyz.sh/)).
6. .. do the work
7. `make fmt`
8. `make lint`
9. .. fix if any
10. `make test`
11. .. fix if any
12. `make integration`
13. .. fix if any
14. `git commit -a`. Make sure to enter meaningful commit message title.
15. `git push origin FEATURENAME`
16. Go to GitHub UI and create PR. Alternatively, `gh pr create` (if you have [GitHub CLI](https://cli.github.com/) installed). 
    Use a meaningful pull request title because it'll appear in the release notes. Use `Resolves #NUMBER` in pull
    request description to [automatically link it](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/using-keywords-in-issues-and-pull-requests#linking-a-pull-request-to-an-issue)
    to an existing issue. 
17. announce PR for the review

## End-to-end testing on Databricks

Unit tests (`make test`) cover factory logic in isolation and integration tests (`make integration`)
exercise the installed CLI against the test fixtures — both run in CI without a workspace. For
changes that affect how generated tasks actually *run* on Databricks (task type, cluster/environment
config, command shape, the notebook runner, etc.), additionally verify end-to-end against a real
workspace as described below.

The flow is: **generate** a job definition from a manifest → **deploy** it as a Declarative
Automation Bundle (DAB) → **run** it and confirm the tasks execute correctly.

### 0. Prerequisites

- Databricks CLI v0.205+ installed: `databricks --version`
- Authenticated to the target workspace: `databricks auth login --host https://<workspace>`
  (or select an existing profile with `databricks auth profiles`)
- A scratch workspace you don't mind deploying to — DABs are declarative and will overwrite
  or delete resources that share a bundle name on re-deploy.

### 1. Generate the job definition

Run the CLI against one of the test manifests (or your own):

```shell
databricks_dbt_factory \
  --dbt-manifest-path tests/test_data/manifest.json \
  --input-job-spec-path tests/test_data/job_definition_template.yaml \
  --target-job-spec-path job_definition_new.yaml \
  --source WORKSPACE \
  --target dev
```

For **notebook task type**, also pass `--task-type notebook`. The factory writes an immutable
`run_dbt_command_<sha256>.py` runner next to the generated job spec automatically; `databricks
bundle deploy` syncs that workspace-local bundle file in Step 3. Use `--source WORKSPACE` for this
auto-copy mode. Also pass `--project-directory` / `--profiles-directory` pointing at the uploaded
dbt project; see the [Task types](../README.md#task-types) section in the README for full examples.

### 2. Wrap the generated spec in a DAB

Create `databricks.yml` in the same directory as `job_definition_new.yaml`:

```yaml
bundle:
  name: dbt_factory_test_<iteration>
workspace:
  host: https://<your-workspace>.cloud.databricks.com
targets:
  dev:
    mode: development
    default: true
include:
  - job_definition_new.yaml
```

Use a **unique `bundle.name`** per test iteration — DABs are declarative, so anything previously
deployed under the same bundle name but no longer in `include` is **deleted** on the next deploy.

Auto-copied bundle notebooks use `--source WORKSPACE`, as recommended for workspace-local bundle
files. No extra Git configuration is needed, but the dbt project directory and `profiles.yml` must
exist at the paths passed to `--project-directory` / `--profiles-directory` (upload them with
`databricks workspace import-dir` or sync from your repo). Reserve `--source GIT` for an explicit
`--notebook-path` inside the job's configured remote Git source.

### 3. Validate, deploy, run

```shell
databricks bundle validate --strict         # schema + reference check, no write; warnings fail
databricks bundle deploy --target dev       # push resources to the workspace
databricks bundle run <job-resource-key>    # trigger a run (e.g. dbt_sql_job)
```

`<job-resource-key>` is the key under `resources.jobs:` in the generated YAML, **not** the
`name` field. For the default test manifest that's `dbt_sql_job`.

`databricks bundle run` prints the run URL — open it to watch the DAG in the UI.

### 4. Verify

- Task graph matches the expected topology (models, tests, snapshots, seeds in the right order).
- Each task succeeds — or, when intentionally breaking a test:
  - In the default per-test mode, the individual `<test_name>_test` task fails; downstream models
    gated on that test at the first safe downstream frontier are skipped.
  - Under `--bundle-tests`, the `<resource>_test` task fails and downstream models/seeds/snapshots
    gated on that bundle are skipped.
- For notebook tasks, confirm `dbt_commands` / `project_directory` / `profiles_directory`
  parameters render correctly in the task run page.

### 5. Clean up

```shell
databricks bundle destroy --target dev
```

### Serverless model-reuse metadata probe

Before implementing reuse storage, validate the platform primitives with an explicitly selected
profile, managed scratch catalog, and serverless SQL warehouse. Install the current dbt v1
development environment (`make dev`); its Databricks SDK is used by the live driver.

```shell
hatch run python -m tests.databricks.reuse_acceptance \
  --profile fevm-test-jg \
  --catalog test_jg_catalog \
  --warehouse-id 7b8cc32a9eba1da3 \
  --directory /tmp/factory-reuse-probe-unique
```

The local directory must not exist. The probe validates and deploys a unique serverless bundle,
creates a uniquely named schema and managed Volume, then checks Delta identity/version reads,
view definitions, state MERGE idempotency, immutable Volume uploads, lineage access, and the
Spark/SDK/dbt execution principals. The notebook pins dbt Core 1.12.5 and dbt Databricks 1.12.7.
Lineage unavailability is recorded separately; an empty result never proves complete lineage.

This starts billable serverless compute. The driver saves `result.json` locally before destroying
its job and deleting only its ownership-marked schema. It leaves the selected warehouse available.
Keep the local bundle and result for recovery after a process interruption; inspect its recorded
run before manually destroying the bundle or removing scratch data. This is a primitive probe,
not an acceptance test of implemented model reuse or a net-savings benchmark.

### Reuse configuration and parse provenance

`--reuse-config <json>` validates reuse declarations; `--reuse-provenance <json>` binds them to
a captured manifest. Runtime reuse is pending: these flags currently generate ordinary tasks
and print that limitation. Reuse requires serverless notebook tasks. Missing/unreadable provenance,
an unspecified runtime target, or task-local `dbt deps` gives `CONTEXT_UNKNOWN`; a known manifest,
target, or project/environment mismatch fails before publishing output.

The configuration is a strict JSON object:

```json
{
  "format_version": 1,
  "project_id": "sales",
  "environment": "prod",
  "owner_key": "sales-job",
  "state_catalog": "test_jg_catalog",
  "state_schema": "factory_state",
  "artifact_volume": {"catalog": "test_jg_catalog", "schema": "factory_state", "volume": "artifacts"}
}
```

The build pipeline can call `reuse_prepare.capture_parse(snapshot, profiles, identity=identity,
target="prod", deployment_ref=release_id)`. It returns manifest and provenance bytes from a fresh
dbt parse in the calling Python environment. `identity` contains the trusted `project_id`,
`environment`, `workspace_id`, `metastore_id`, and execution `principal`. Runtime verification of
that declaration remains required before any reuse decision.

Use a dedicated immutable deployment snapshot containing only project code/data and already
installed, locked dbt packages. Keep credentials, profiles, generated artifacts, virtual
environments and Git metadata outside it. Symlinks are refused; materialize local packages into
the snapshot. Write the returned artifacts outside the snapshot. `verify_snapshot` checks its
contents against provenance once during preparation; it is not intended for every model task.

Optional `variables`, `environment_inputs` (an allowlist of environment variable names), and
`secret_versions` must describe all result-affecting context. Vars and allowlisted values are
caller-declared non-secret inputs. Never pass credentials or raw/hashed secrets in these fields.
`DBT_ENV_SECRET_*` values are refused; use trustworthy non-secret version identifiers for
result-affecting secrets. Undeclared context prevents safe reuse and remains the author's
responsibility under the explicit model opt-in contract.

Python and installed library versions belong to the context digest. Manifest/deployment digests
are separate, allowing native per-model code comparison without invalidating every model for an
unrelated source edit. SQL table models can be candidates; hooks, Python/custom/incremental
outputs, and unverified view/ephemeral dependencies execute normally. Physical Delta identity,
version and complete-input checks still have to pass at runtime.

### Immutable reuse artifacts and runtime wheel

With valid provenance, an explicit `--target`, and task-local dependency resolution disabled,
generation writes `reuse_artifacts/` beside the output job spec. It contains the exact manifest,
provenance, and a canonical descriptor, each named with its full SHA-256 digest. The descriptor
binds model declarations, reuse configuration and the generating Factory version. Existing files
must match; dry-run writes nothing. Keep this directory outside the captured project snapshot.
Missing context produces ordinary tasks without a reusable deployment bundle.

Reuse runtime work is still pending. The generated tasks execute normally; these artifacts do
not enable skipping. The preparation notebook and its immutable publication belong to the
preparation implementation, so no placeholder notebook is shipped.

Install the **same Factory wheel used for generation** in each reuse-enabled serverless notebook
environment, alongside the existing exact dbt/adapter pins. Build it with `hatch build -t wheel`
and upload it under a unique immutable release path. Add its Volume path to the chosen environment's
`spec.dependencies`, for example:

```yaml
environments:
  - environment_key: Default
    spec:
      environment_version: '5'
      dependencies:
        - /Volumes/test_jg_catalog/factory_state/artifacts/wheels/RELEASE/databricks_dbt_factory-0.4.0-py3-none-any.whl
        - dbt-databricks==1.12.7
        - dbt-core==1.12.5
```

These are the tested dbt v1 versions; retain your other pinned driver dependencies. For a workspace
base environment, add the wheel there instead. The CLI does not modify environments or require dbt
or the SDK as installation dependencies. `read_descriptor` rejects a different Factory runtime
version or unsupported descriptor format before the future reuse path can use it.

Publish the three deployment artifacts **before bundle validation/deployment**, using the exact
descriptor path printed by generation (a directory may retain multiple deployments):

```python
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks_dbt_factory.reuse_runtime import publish_artifacts, read_descriptor

descriptor_path = Path("resources/reuse_artifacts/descriptor_FULL_SHA256.json")
descriptor = read_descriptor(descriptor_path.read_bytes())
local_bundle = tuple(descriptor_path.parent / name for name in (
    descriptor["manifest"], descriptor["provenance"], descriptor_path.name,
))
client = WorkspaceClient(profile="fevm-test-jg")
volume_path = "/Volumes/test_jg_catalog/factory_state/artifacts"
# Provision the configured Volume and directory explicitly during deployment setup.
client.files.create_directory(volume_path)
references = publish_artifacts(client, local_bundle, volume_path)
```

Use the catalog/schema/volume in your validated config, with an explicitly selected profile.
Publication validates all local bytes and references, uploads without overwrite, reads every object
back, and verifies its digest. It uploads the descriptor last and returns references only after
the whole batch succeeds. A failed batch may leave immutable orphan files; it returns no usable
deployment references. Persist/pass `references` only after the call returns, then run
`databricks bundle validate -t TARGET --profile fevm-test-jg` and deploy the bundle.
`download_artifact(client, path, sha256)` rechecks downloaded bytes before comparison use.
No cross-file transaction or artifact garbage collection is assumed. Retain referenced artifacts.

### Delta state storage checkpoint

On serverless notebook compute, call `reuse_runtime.initialize_state(spark, config)` once in
the configured, existing UC schema. It creates `factory_reuse_owners`, `factory_reuse_builds`
and `factory_reuse_decisions`; model tasks never initialize tables. The setup identity needs
`USE CATALOG`, `USE SCHEMA` and `CREATE TABLE`. The job identity needs the corresponding table
read/write privileges and artifact Volume access.

`claim_owner` binds a physical output name to the project/environment, workspace/metastore,
logical owner and deployed job. All participating jobs must use the same registry. This is
a conflict detector for serialized owning jobs, not a lock against concurrent or unregistered
writers. Keep `max_concurrent_runs: 1`; ownership reassignment requires draining the old job.

`snapshot_relation` brackets a Delta version read with UC/Delta identity checks. It returns
separate relation-name components, UC ID, Delta ID and version. Missing/unreadable metadata,
non-Delta relations and mixed recreation observations raise `EvidenceUnavailable` with a reason.

`record_build` accepts a `BuildEvidence` record and an explicit successful-execution result.
It verifies published artifacts, context, declared inputs and output mapping, and records only
stable pre/post input evidence. Failed attempts, missing evidence and changing inputs cannot
supply a reusable baseline. Builds retain their original completion time; decision events have
their own observation times. Identical attempt publication is idempotent; different content
for an existing attempt fails instead of overwriting it. SQL values use bound parameters.

`read_build` selects by logical scope/model and current output identity/version, then verifies
the stored record, attempt key and downloaded artifacts. Its returned `key` and `sha256` identify
the exact baseline for preparation. Context comparison remains separate: a valid older context
can be inspected, but cannot authorize reuse under a changed execution context.
Missing state/artifacts raise `EvidenceUnavailable`; known artifact mismatches and ownership
conflicts fail explicitly. After model success, state-write outages return a fallback reason
from `record_build`/`record_decision`. Callers must report that reason in task output; durable
decision logging is not guaranteed during an outage.

Runtime preparation, identity verification and task skipping are still pending. To test the
actual storage APIs in a disposable serverless job using the matching built wheel:

```sh
hatch build -t wheel
hatch run python -m tests.databricks.reuse_acceptance \
  --profile fevm-test-jg --catalog test_jg_catalog \
  --warehouse-id 7b8cc32a9eba1da3 --scenario state \
  --factory-wheel dist/databricks_dbt_factory-0.4.0-py3-none-any.whl \
  --directory /tmp/factory-state-probe-unique
```

The harness verifies the uploaded wheel bytes, validates/deploys its serverless bundle, saves
the result, and removes its job and ownership-marked scratch schema. The fixture tables are
built with Spark SQL; a real dbt parse supplies their model/source mappings. This tests storage,
not the unimplemented dbt runner integration or measured net savings.

## Troubleshooting

If you encounter any package dependency errors after `git pull`, run `make clean`
