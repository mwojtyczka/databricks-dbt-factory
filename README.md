Databricks dbt factory
===

Databricks dbt factory is a simple library to generate Databricks Job definition where individual dbt models are run as separate tasks.

# databricks_dbt_factory

[![PyPI - Version](https://img.shields.io/pypi/v/databricks-dbt-factory.svg)](https://pypi.org/project/databricks-dbt-factory)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/databricks-dbt-factory.svg)](https://pypi.org/project/databricks-dbt-factory)

-----

**Table of Contents**

- [Installation](#installation)
- [Local Setup](#local-setup)
- [License](#license)

## Installation

```console
pip install databricks-dbt-factory
```

## Local Setup

This section provides a step-by-step guide to set up and start working on the project. These steps will help you set up your project environment and dependencies for efficient development.

To begin, install [Hatch](https://github.com/pypa/hatch), which is our build tool.

On MacOSX, this is achieved using the following:
```shell
brew install hatch
```

Run the following command to create the default environment and install development dependencies:
```shell
make dev
```

Before every commit, apply the consistent formatting of the code, as we want our codebase look consistent:
```shell
make fmt
```

Before every commit, run automated bug detector (`make lint`) and unit tests (`make test`) to ensure that automated
pull request checks do pass, before your code is reviewed: 
```shell
make lint
make test
```

## Local installation and execution

```shell
hatch build

# use hatch directly
hatch run databricks_dbt_factory --manifest-path dbt_manifest/manifest.json
# or install locally and run
pip install .
databricks_dbt_factory --manifest-path dbt_manifest/manifest.json
```

## License

`databricks-dbt-factory` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
