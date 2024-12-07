Databricks dbt factory
===

Databricks dbt factory is a simple library to generate Databricks Job tasks based on dbt manifest.
The tool can overwrite tasks in the existing Databricks job definition (in-place update, or creating new definition).

# databricks_dbt_factory

[![PyPI - Version](https://img.shields.io/pypi/v/databricks-dbt-factory.svg)](https://pypi.org/project/databricks-dbt-factory)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/databricks-dbt-factory.svg)](https://pypi.org/project/databricks-dbt-factory)

-----

**Table of Contents**

- [Installation](#installation)
- [Usage](#usage)
- [Contribution](#contribution)
- [License](#license)

# Motivation

The current integration of dbt with Databricks Workflows treats the entire dbt project as a single execution unit (black box), limiting flexibility and debugging options.

This project breaks down each dbt object (seed/snapshot/model/test) into separate Workflow task offering several key benefits:
* Simplified Troubleshooting: Isolating tasks makes it easier to identify and resolve issues specific to a single model
* Enhanced Logging and Notifications: Provides more detailed logs and precise error alerts, improving debugging efficiency
* Better Retriability: Enables retrying only the failed model tasks, saving time and resources compared to rerunning the entire project
* Seamless Testing: Allows running dbt data tests on tables immediately after a model completes, ensuring faster validation and feedback

### Databricks Workflows run all dbt objects at once:
![before](docs/before.png?)

![dbt_task](docs/dbt_task.png?)

### The tool generates workflows where dbt objects are run as individual Databricks Workflow tasks:
![after](docs/after.png?)

![workflow](docs/workflow.png?)

# Installation

```shell
pip install databricks-dbt-factory
```

# Usage

Generate new Databricks job spec in `job_definition_new.yaml` in the current directory:
```shell
databricks_dbt_factory  \
  --dbt-manifest-path tests/test_data/manifest.json \
  --input-job-spec-path tests/test_data/job_definition_template.yaml \
  --target-job-spec-path job_definition_new.yaml \
  --source GIT \
  --target dev
```

To check all input arguments see `databricks_dbt_factory --help`.

Demo of the tool can be found [here](https://github.com/mwojtyczka/dbt-demo).

# Contribution

See contribution guidance [here](CONTRIBUTING.md).

# License

`databricks-dbt-factory` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
