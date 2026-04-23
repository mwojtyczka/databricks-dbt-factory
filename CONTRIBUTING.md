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

Before every commit, run automated bug detector (`make lint`) and unit tests (`make test`) to ensure that automated
pull request checks do pass, before your code is reviewed by others: 
```shell
make lint
make test
```
## Local installation and execution

```shell
hatch build

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

## End-to-end testing on Databricks

Unit tests (`make test`) cover factory logic in isolation. For changes that affect how generated
tasks run on Databricks (task type, cluster/environment config, command shape, the notebook
runner, etc.), verify end-to-end against a real workspace.

The flow is: **generate** a job definition from a manifest → **deploy** it as a Databricks Asset
Bundle → **run** it and confirm the tasks execute correctly.

1. **Generate the job definition** using the CLI against one of the test manifests (or your own):

    ```shell
    databricks_dbt_factory \
      --dbt-manifest-path tests/test_data/manifest.json \
      --input-job-spec-path tests/test_data/job_definition_template.yaml \
      --target-job-spec-path job_definition_new.yaml \
      --source GIT \
      --target dev
    ```

    For notebook-task-type testing, also pass `--task-type notebook --notebook-path <path>`.

2. **Wrap the generated spec in a DAB** (`databricks.yml`). Use a **unique `bundle.name`** per
    test iteration — DABs are declarative, so anything previously deployed under the same bundle
    name but no longer in `include` is **deleted**. Example:

    ```yaml
    bundle:
      name: dbt_factory_test_<iteration>
    workspace:
      host: https://<your-workspace>.cloud.databricks.com
    include:
      - job_definition_new.yaml
    ```

3. **Deploy and run:**

    ```shell
    databricks bundle validate
    databricks bundle deploy
    databricks bundle run <job-resource-name>
    ```

4. **Verify** the job runs in the Databricks UI — task graph matches expectations, each task
    succeeds, and (for test tasks) failures surface the way your change intends.

Tip: run against a scratch workspace so mistakes don't interfere with real jobs.

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
12. `git commit -a`. Make sure to enter meaningful commit message title.
13. `git push origin FEATURENAME`
14. Go to GitHub UI and create PR. Alternatively, `gh pr create` (if you have [GitHub CLI](https://cli.github.com/) installed). 
    Use a meaningful pull request title because it'll appear in the release notes. Use `Resolves #NUMBER` in pull
    request description to [automatically link it](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/using-keywords-in-issues-and-pull-requests#linking-a-pull-request-to-an-issue)
    to an existing issue. 
15. announce PR for the review

## Troubleshooting

If you encounter any package dependency errors after `git pull`, run `make clean`
