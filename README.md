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

# Contribution

See contribution guidance [here](CONTRIBUTING.md).

# License

`databricks-dbt-factory` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
