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

Update tasks in the existing Databricks workflow (job) definition and write new spec to `job_definition_new.yaml`:
```shell
databricks_dbt_factory  \
  --dbt-manifest-path tests/test_data/manifest.json \
  --input-job-spec-path tests/test_data/job_definition_template.yaml \
  --target-job-spec-path job_definition_new.yaml \
  --source GIT \
  --target dev
```

Note that `--input-job-spec-path` and `--target-job-spec-path` can be the same file, in which case the job spec is updated in place.

**Arguments:**
- `--new-job-name` (type: str, optional, default: None): Optional job name. If provided, the existing job name in the job spec is updated.
- `--dbt-manifest-path` (type: str, required): Path to the dbt manifest file.
- `--input-job-spec-path` (type: str, required): Path to the input job spec file.
- `--target-job-spec-path` (type: str, required): Path to the target job spec file.
- `--target` (type: str, required): optional dbt target to use. If not provided, the default target from the dbt profile will be used.
- `--source` (type: str, optional, default: None): Optional dbt project source (`GIT` or `WORKSPACE`). If not provided, `WORKSPACE` will be used.
- `--warehouse_id` (type: str, optional, default: None): Optional SQL Warehouse to run dbt models on.
- `--schema` (type: str, optional, default: None): Optional metastore schema (database) to use in the dbt task.
- `--catalog` (type: str, optional, default: None): Optional metastore catalog to use in the dbt task.
- `--profiles-directory` (type: str, optional, default: None): Optional (relative) path to the job profiles directory to use in the dbt task.
- `--project-directory` (type: str, optional, default: None): Optional (relative) workspace path to the dbt project directory to use in the dbt task.
- `--environment-key` (type: str, optional, default: Default): Optional (relative) key of an environment.
- `--extra-dbt-command-options` (type: str, optional, default: ""): Optional additional dbt command options to include.
- `--run-tests` (type: bool, optional, default: True): Whether to run data tests after the model. Enabled by default.
- `--enable-dbt-deps` (type: bool, optional, default: False): Whether to run dbt deps before each task. Disabled by default.
- `--dbt-tasks-deps` (type: str, optional, default: None): Optional comma separated list of tasks for which dbt deps should be run (e.g. "diamonds_prices,second_dbt_model"). Only in effect if `--enable-dbt-deps` is enabled.
- `--dry-run` (type: bool, optional, default: False): Print generated tasks without updating the job spec file. Disabled by default.

You can also check all input arguments by running `databricks_dbt_factory --help`.

Demo of the tool can be found [here](https://github.com/mwojtyczka/dbt-demo).

# Contribution

See contribution guidance [here](CONTRIBUTING.md).

# License

`databricks-dbt-factory` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
