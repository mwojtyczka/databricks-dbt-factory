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
committed golden files. It requires no Databricks workspace and runs in CI alongside the unit tests.
## Local installation and execution

```shell
make build

# use hatch directly
hatch run databricks_dbt_factory \
  --dbt-manifest-path tests/test_data/manifest.json \
  --input-job-spec-path tests/test_data/job_definition_template.yaml \
  --target-job-spec-path job_definition_new.yaml \
  --source GIT \
  --target dev

# or install locally and run
pip install .
databricks_dbt_factory  \
  --dbt-manifest-path tests/test_data/manifest.json \
  --input-job-spec-path tests/test_data/job_definition_template.yaml \
  --target-job-spec-path job_definition_new.yaml \
  --source GIT \
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

The flow is: **generate** a job definition from a manifest → **deploy** it as a Databricks Asset
Bundle (DAB) → **run** it and confirm the tasks execute correctly.

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
  --source GIT \
  --target dev
```

For **notebook task type**, also pass `--task-type notebook`. The factory copies the packaged
runner notebook next to the generated job spec automatically — no separate workspace upload
needed; `databricks bundle deploy` will sync it in Step 3. (For `--source WORKSPACE`, also
pass `--project-directory` / `--profiles-directory` pointing at the uploaded dbt project; see
the [Task types](../README.md#task-types) section in the README for full examples.)

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

If the generated spec uses `--source GIT`, it already contains a `git_source` block; the
workspace must be able to reach that repo/branch (public URL or a Git credential configured in
the workspace). For `--source WORKSPACE`, no extra git config is needed, but the dbt project
directory and `profiles.yml` must already exist at the paths passed to `--project-directory` /
`--profiles-directory` (upload them with `databricks workspace import-dir` or sync from your
repo).

### 3. Validate, deploy, run

```shell
databricks bundle validate                  # schema + reference check, no write
databricks bundle deploy --target dev       # push resources to the workspace
databricks bundle run <job-resource-key>    # trigger a run (e.g. dbt_sql_job)
```

`<job-resource-key>` is the key under `resources.jobs:` in the generated YAML, **not** the
`name` field. For the default test manifest that's `dbt_sql_job`.

`databricks bundle run` prints the run URL — open it to watch the DAG in the UI.

### 4. Verify

- Task graph matches the expected topology (models, tests, snapshots, seeds in the right order).
- Each task succeeds — or, when intentionally breaking a test:
  - In the default per-test mode, the individual `test_<name>` task fails; downstream models
    still run (they depend on the parent resource task, not the test).
  - Under `--bundle-tests`, the `tests_<resource>` task fails and downstream models/seeds/snapshots
    gated on it are skipped.
- For notebook tasks, confirm `dbt_commands` / `project_directory` / `profiles_directory`
  parameters render correctly in the task run page.

### 5. Clean up

```shell
databricks bundle destroy --target dev
```

## Troubleshooting

If you encounter any package dependency errors after `git pull`, run `make clean`
