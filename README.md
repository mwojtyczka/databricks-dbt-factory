Databricks dbt factory
===

Databricks dbt factory is a simple library to generate Databricks Job tasks based on dbt manifest.
The tool can overwrite tasks in the existing Databricks job definition (in-place update, or creating new definition).

[![PyPI - Version](https://img.shields.io/pypi/v/databricks-dbt-factory.svg)](https://pypi.org/project/databricks-dbt-factory)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/databricks-dbt-factory.svg)](https://pypi.org/project/databricks-dbt-factory)

-----

**Table of Contents**

- [Motivation](#motivation)
- [Installation](#installation)
- [Usage](#usage)
- [Contribution](#contribution)
- [License](#license)

# Motivation

The current integration of dbt with Databricks Workflows treats the entire dbt project as a single execution unit (black box).

This tool updates Databricks Workflows so that dbt objects within a project (models / tests / seeds / snapshots) are run as separate tasks.

Benefits:
* Simplified Troubleshooting: Isolating tasks makes it easier to identify and resolve issues specific to a single model
* Enhanced Logging and Notifications: Provides more detailed logs and precise error alerts, improving debugging efficiency
* Better Retriability: Enables retrying only the failed model tasks, saving time and resources compared to rerunning the entire project
* Seamless Testing: Allows running dbt data tests on tables immediately after a model completes, ensuring faster validation and feedback

### Databricks Workflows run dbt projects as a single "black box" task:
![before](docs/before.png?)

Example of a workflow:

![dbt_task](docs/dbt_task.png?)

### The tool update Databricks Workflows so that dbt objects within a project are run as individual tasks:
![after](docs/after.png?)

Example of the updated workflow:

![workflow](docs/workflow.png?)

# Installation

```shell
pip install databricks-dbt-factory
```

# Usage

Update tasks in the existing Databricks job definition and write the results to `job_definition_new.yaml`:
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
