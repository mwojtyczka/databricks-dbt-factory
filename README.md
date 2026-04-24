Databricks dbt factory
===

Databricks dbt Factory is a lightweight library that generates a Databricks Workflow task for each dbt model, based on your dbt manifest.
It creates a DAG of tasks that run each dbt model, test, seed, and snapshot as a separate task in Databricks Workflows.

The tool can create or update tasks directly within an existing job specification such as Databricks Assets Bundle (DAB).

[![PyPI - Version](https://img.shields.io/pypi/v/databricks-dbt-factory.svg)](https://pypi.org/project/databricks-dbt-factory)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/databricks-dbt-factory.svg)](https://pypi.org/project/databricks-dbt-factory)

-----

**Table of Contents**

- [Motivation](#motivation)
- [How it works](#benefits)
- [Installation](#installation)
- [Usage](#usage)
- [Task types](#task-types)
- [Contribution](#contribution)
- [License](#license)

# Motivation

By default, dbt's integration with Databricks Workflows treats an entire dbt project as a single execution unit — a black box.

Databricks dbt Factory changes that by updating Databricks Workflow specs to run dbt objects (models, tests, seeds, snapshots) as individual tasks.

![before](docs/dbt-factory.png?)

### Benefits

✅ Simplified troubleshooting — Quickly pinpoint and fix issues at the model level.

✅ Enhanced logging & notifications — Gain detailed logs and precise error alerts for faster debugging.

✅ Improved retriability — Retry only the failed model tasks without rerunning the full project.

✅ Seamless testing — Automatically run dbt data tests on tables right after each model finishes, enabling faster validation and feedback.

# How it works

![after](docs/arch.png?)

The tool reads the dbt manifest file and the existing DAB workflow definition, and generates a new definition.

# Installation

```shell
pip install databricks-dbt-factory
```

# Usage

The factory reads a **job template** (a minimal DAB-style YAML with an empty tasks list) and
a **dbt manifest**, then outputs a complete job definition with one task per dbt node.

## Job template

Create a minimal job template YAML. This is the skeleton the factory injects tasks into:

```yaml
resources:
  jobs:
    my_dbt_job:
      name: my_dbt_job
      queue:
        enabled: true
      environments:
      - environment_key: Default
        spec:
          client: '1'
          dependencies:
          - dbt-databricks
```

To use a workspace base environment instead of inline dependencies (recommended for
notebook tasks on serverless — requires Databricks CLI >= 0.292.0):

```yaml
      environments:
      - environment_key: Default
        spec:
          base_environment: "/Workspace/Shared/envs/my_base_env.yaml"
```

Note: `client` and `base_environment` are mutually exclusive — use one or the other.

## Generating native dbt tasks

```shell
databricks_dbt_factory  \
  --dbt-manifest-path target/manifest.json \
  --input-job-spec-path job_template.yaml \
  --target-job-spec-path job_definition.yaml \
  --source GIT \
  --target dev
```

This generates `dbt_task` entries — the native Databricks dbt task type.

Note that `--input-job-spec-path` and `--target-job-spec-path` can be the same file, in which case the job spec is updated in place.

## Generating notebook tasks (recommended for Serverless)

Native `dbt_task` on Serverless does not support base environments or environment variables.
To work around this, the factory can generate `notebook_task` entries instead, using a
reusable runner notebook that invokes dbt via the Python `dbtRunner` API.

```shell
databricks_dbt_factory  \
  --dbt-manifest-path target/manifest.json \
  --input-job-spec-path job_template.yaml \
  --target-job-spec-path job_definition.yaml \
  --task-type notebook \
  --notebook-path /Workspace/Users/you@example.com/notebooks/run_dbt_command.py \
  --project-directory /Workspace/Users/you@example.com/my_dbt_project \
  --profiles-directory /Workspace/Users/you@example.com/my_dbt_project \
  --source WORKSPACE \
  --target dev
```

To run the dbt process on a job cluster instead of serverless, use `--job-cluster-key`
and define the cluster in your job template. Note that `--job-cluster-key` controls where
the dbt Python process runs (parsing, compiling, orchestrating). SQL execution still goes
to the SQL warehouse configured in your `profiles.yml` — this is a `dbt-databricks`
limitation.

```yaml
resources:
  jobs:
    my_dbt_job:
      name: my_dbt_job
      job_clusters:
      - job_cluster_key: dbt_cluster
        new_cluster:
          spark_version: 16.2.x-scala2.12
          num_workers: 1
          node_type_id: i3.xlarge
```

```shell
databricks_dbt_factory  \
  --dbt-manifest-path target/manifest.json \
  --input-job-spec-path job_template.yaml \
  --target-job-spec-path job_definition.yaml \
  --task-type notebook \
  --notebook-path /Workspace/Users/you@example.com/notebooks/run_dbt_command.py \
  --project-directory /Workspace/Users/you@example.com/my_dbt_project \
  --profiles-directory /Workspace/Users/you@example.com/my_dbt_project \
  --job-cluster-key dbt_cluster \
  --source WORKSPACE \
  --target dev
```

The runner notebook (`src/databricks_dbt_factory/notebook/run_dbt_command.py`) ships with
this package. Upload it to your Databricks workspace and reference it via `--notebook-path`.

The notebook:
- Accepts `dbt_commands`, `project_directory`, and `profiles_directory` as parameters
- Injects `DATABRICKS_TOKEN` and `DATABRICKS_HOST` from the runtime context (same as native dbt_task)
- Invokes dbt commands via `dbtRunner().invoke()` — no subprocess needed
- Works with existing `profiles.yml` that use `{{ env_var('DATABRICKS_TOKEN') }}`

**Arguments:**
- `--new-job-name` (type: str, optional, default: None): Optional job name. If provided, the existing job name in the job spec is updated.
- `--dbt-manifest-path` (type: str, required): Path to the dbt manifest file.
- `--input-job-spec-path` (type: str, required): Path to the input job spec file (the job template).
- `--target-job-spec-path` (type: str, required): Path to the target job spec file.
- `--target` (type: str, optional): dbt target to use. If not provided, the default target from the dbt profile will be used.
- `--source` (type: str, optional, default: None): Project source (`GIT` or `WORKSPACE`). If not provided, `WORKSPACE` will be used.
- `--task-type` (type: str, optional, default: "dbt"): Task type to generate — `dbt` for native dbt_task, `notebook` for notebook_task wrapper.
- `--notebook-path` (type: str, required when task-type is "notebook"): Workspace path to the dbt runner notebook.
- `--warehouse_id` (type: str, optional): SQL Warehouse ID. Only used with native dbt_task.
- `--schema` (type: str, optional): Metastore schema. Only used with native dbt_task.
- `--catalog` (type: str, optional): Metastore catalog. Only used with native dbt_task.
- `--profiles-directory` (type: str, optional): Path to the profiles directory.
- `--project-directory` (type: str, optional): Path to the dbt project directory.
- `--environment-key` (type: str, optional, default: Default): Key of the serverless environment. Mutually exclusive with `--job-cluster-key`.
- `--job-cluster-key` (type: str, optional): Job cluster key for running tasks on job compute instead of serverless. Mutually exclusive with `--environment-key`.
- `--extra-dbt-command-options` (type: str, optional, default: ""): Additional dbt command options to include.
- `--run-tests` / `--no-run-tests` (flag, default: enabled): Run data tests after each model.
- `--bundle-tests` / `--no-bundle-tests` (flag, default: disabled): Bundle single-model tests per resource into one `dbt test --select <resource> --indirect-selection cautious` task, and gate downstream models/seeds/snapshots on the upstream's `_tests` task so failing tests halt the DAG. Cross-model tests are emitted as their own tasks with multi-resource deps. See [Test handling](#test-handling).
- `--enable-dbt-deps` / `--no-enable-dbt-deps` (flag, default: disabled): Run dbt deps before each task.
- `--dbt-tasks-deps` (type: str, optional, default: None): Comma separated list of tasks for which dbt deps should be run (e.g. "diamonds_prices,second_dbt_model"). Only in effect if `--enable-dbt-deps` is set.
- `--dry-run` / `--no-dry-run` (flag, default: disabled): Print generated tasks without updating the job spec file.

You can also check all input arguments by running `databricks_dbt_factory --help`.

## Test handling

When `--run-tests` is enabled, the factory produces tasks for dbt tests. Two modes are available,
controlled by `--bundle-tests`:

### Per-test (default)

One Databricks task per dbt test node, running `dbt test --select <test_name>`. Each test task's
`depends_on` includes every model/seed/snapshot the test references, so multi-model tests
(e.g. `relationships`) only run after all their endpoints are built. Downstream models are **not**
rewired — they depend on the parent resource task directly.

- **Pros:** per-test parallelism; failing tests are individually visible in the Databricks UI;
  cross-model tests wait for every endpoint they reference.
- **Cons:** larger DAG (one task per test, and dbt projects routinely have many more tests than
  models).

### Bundled (`--bundle-tests`)

The factory classifies each dbt test node into one of two buckets based on its `depends_on`:

- **Single-model tests** (most tests: `unique`, `not_null`, `accepted_values`, column-level
  checks, …) — collapsed into one Databricks task per tested resource, with task key
  `<resource_task_key>_tests` (e.g. `model_my_project_customers_tests`) running
  `dbt test --select <package>.<resource> --indirect-selection cautious`. The `cautious`
  indirect selector tells dbt "only run tests whose referenced resources are entirely within
  this selection" — which makes the bundle correct: cross-model tests that happen to touch
  this resource are filtered out here and handled separately.

- **Cross-model tests** (e.g. `relationships`, custom tests that reference multiple models) —
  emitted as their own tasks, one per test node, with deps on **every** resource the test
  references. These run in parallel with the bundled tasks; they don't fit inside a bundle
  because their correctness requires all their endpoints to be built first.

Downstream models/seeds/snapshots that depend on a tested resource are rewired to depend on
the upstream's `_tests` task, so data only flows downstream after its upstream single-model
tests pass. Cross-model test tasks don't gate downstream execution — they run as leaf
assertions. (If you want tests to report without blocking, use dbt's `severity: warn` on the
individual tests instead.)

- **Pros:** significantly smaller DAG, dbt's native test selection handles the per-test
  execution inside a bundled task.
- **Cons:** per-test failure visibility is lost inside a bundle — a failure shows up as one red
  `<resource>_tests` task rather than a specific red `<test_name>` task in the UI; drill into
  the task logs to see which individual test(s) failed. (Cross-model test tasks retain their
  per-test visibility because they aren't bundled.)

## Task types

The factory supports two task types, controlled by `--task-type`:

### `dbt` (default)

Generates native Databricks `dbt_task` entries. This is the standard approach that
uses Databricks' built-in dbt integration. Works with both classic compute and serverless.

**Limitations on Serverless:** Native dbt tasks do not support workspace base environments
or environment variables. If you need either of these, use the `notebook` task type instead.

### `notebook`

Generates `notebook_task` entries that wrap dbt execution via the `dbtRunner` Python API.
Each task calls a shared runner notebook (`run_dbt_command.py`) with parameterized dbt commands.

**Advantages over native dbt_task:**
- Supports workspace base environments (admin-managed, pre-cached dependencies) — use `base_environment` in the job template instead of `client` + `dependencies` (see [Job template](#job-template))
- Supports environment variables
- Supports running the dbt process on job compute via `--job-cluster-key` (SQL execution still uses the warehouse in `profiles.yml`)
- Uses the same runtime authentication as native dbt_task (`DATABRICKS_TOKEN` injected from notebook context)

# Contribution

See contribution guidance [here](CONTRIBUTING.md).

# License

`databricks-dbt-factory` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
