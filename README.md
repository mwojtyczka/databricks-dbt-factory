Databricks dbt factory
===

Databricks dbt Factory is a lightweight library that generates a Databricks Workflow task for each dbt model, based on your dbt manifest.
It creates a DAG of tasks that run each dbt model, test, seed, and snapshot as a separate task in Databricks Workflows.

The tool can create or update tasks directly within an existing Databricks workflow (in-place updates or additions).

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

Databricks dbt Factory changes that by updating Databricks Workflows to run dbt objects (models, tests, seeds, snapshots) as individual tasks.

![before](docs/dbt-factory.png?)

### Benefits

✅ Simplified troubleshooting — Quickly pinpoint and fix issues at the model level.

✅ Enhanced logging & notifications — Gain detailed logs and precise error alerts for faster debugging.

✅ Improved retriability — Retry only the failed model tasks without rerunning the full project.

✅ Seamless testing — Automatically run dbt data tests on tables right after each model finishes, enabling faster validation and feedback.

# How it works

![after](docs/arch.png?)

The tool reads the dbt manifest file and the existing Databricks workflow definition, then generates a new workflow definition.

# Installation

```shell
pip install databricks-dbt-factory
```

# Usage

Update tasks in the existing Databricks workflow (job) definition and write the results to `job_definition_new.yaml`:
```shell
databricks_dbt_factory  \
  --dbt-manifest-path tests/test_data/manifest.json \
  --input-job-spec-path tests/test_data/job_definition_template.yaml \
  --target-job-spec-path job_definition_new.yaml \
  --source GIT \
  --target dev
```

**Arguments:**
- `--new-job-name` (type: str, optional, default: None): Optional job name. If provided, the existing job name in the job spec is updated.
- `--dbt-manifest-path` (type: str, required): Path to the manifest file.
- `--input-job-spec-path` (type: str, required): Path to the input job spec file.
- `--target-job-spec-path` (type: str, required): Path to the target job spec file.
- `--target` (type: str, required): dbt target to use.
- `--source` (type: str, optional, default: None): Optional project source. If not provided, WORKSPACE will be used.
- `--warehouse_id` (type: str, optional, default: None): Optional SQL Warehouse to run dbt models on.
- `--schema` (type: str, optional, default: None): Optional schema to write to.
- `--catalog` (type: str, optional, default: None): Optional catalog to write to.
- `--profiles-directory` (type: str, optional, default: None): Optional (relative) path to the profiles directory.
- `--project-directory` (type: str, optional, default: None): Optional (relative) path to the project directory.
- `--environment-key` (type: str, optional, default: Default): Optional (relative) key of an environment.
- `--extra-dbt-command-options` (type: str, optional, default: ""): Optional additional dbt command options.
- `--run-tests` (type: bool, optional, default: True): Whether to run data tests after the model. Enabled by default.
- `--dry-run` (type: bool, optional, default: False): Print generated tasks without updating the job spec file. Disabled by default.

You can also check all input arguments by running `databricks_dbt_factory --help`.

Demo of the tool can be found [here](https://github.com/mwojtyczka/dbt-demo).

# Contribution

See contribution guidance [here](CONTRIBUTING.md).

# License

`databricks-dbt-factory` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
