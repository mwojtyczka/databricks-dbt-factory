from pathlib import Path

from dbt.cli.main import dbtRunner, dbtRunnerResult

PROFILES = """\
probe:
  target: dev
  outputs:
    dev:
      type: databricks
      host: example.databricks.com
      http_path: /sql/1.0/warehouses/x
      token: dummy
      schema: default
"""


REUSE_PROFILES = PROFILES.replace("token: dummy", "token: \"{{ env_var('DBT_ACCESS_TOKEN') }}\"")


def invoke_dbt(root: Path, *args: str) -> dbtRunnerResult:
    """
    Invokes dbt against the project at `root`.

    Passes the project and profile directories explicitly rather than changing the working
    directory: `os.chdir` in a test leaks into everything else in the process, and under
    `pytest-cov` it makes coverage write its data file to the wrong place.
    """
    return dbtRunner().invoke([*args, "--project-dir", str(root), "--profiles-dir", str(root)])
