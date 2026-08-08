"""Release-artifact checks that run against built wheels, outside the source tree."""

import shutil
import subprocess
import sys
import tarfile
import textwrap
import zipfile
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_SOURCE = REPOSITORY_ROOT / "src" / "databricks_dbt_factory"


def _run_hatch(*args: str, cwd: Path) -> None:
    hatch = shutil.which("hatch")
    assert hatch is not None, "Hatch is required because the release workflow builds artifacts with `hatch build`"
    result = subprocess.run(  # noqa: S603
        [hatch, "build", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"Hatch build failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"


def _only_artifact(directory: Path, pattern: str) -> Path:
    artifacts = list(directory.glob(pattern))
    assert len(artifacts) == 1, f"expected one {pattern} artifact in {directory}, found {artifacts}"
    return artifacts[0]


def _build_direct_wheel(tmp_path: Path) -> Path:
    output = tmp_path / "direct-wheel"
    _run_hatch("-t", "wheel", str(output), cwd=REPOSITORY_ROOT)
    return _only_artifact(output, "*.whl")


def _build_wheel_from_sdist(tmp_path: Path) -> Path:
    sdist_output = tmp_path / "sdist"
    _run_hatch("-t", "sdist", str(sdist_output), cwd=REPOSITORY_ROOT)
    sdist = _only_artifact(sdist_output, "*.tar.gz")

    unpacked = tmp_path / "unpacked-sdist"
    with tarfile.open(sdist, "r:gz") as archive:
        archive.extractall(unpacked)
    project_directories = [path for path in unpacked.iterdir() if path.is_dir()]
    assert len(project_directories) == 1, f"expected one project directory in {sdist}, found {project_directories}"

    output = tmp_path / "sdist-wheel"
    _run_hatch("-t", "wheel", str(output), cwd=project_directories[0])
    return _only_artifact(output, "*.whl")


def _source_package_files() -> set[str]:
    return {
        f"databricks_dbt_factory/{path.relative_to(PACKAGE_SOURCE).as_posix()}"
        for path in PACKAGE_SOURCE.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
    }


def _assert_wheel_executes_in_isolation(wheel: Path, tmp_path: Path) -> None:
    script = textwrap.dedent(
        """
        import importlib.metadata
        import pathlib
        import sys

        wheel = pathlib.Path(sys.argv[1]).resolve()
        sys.path.insert(0, str(wheel))

        import databricks_dbt_factory.file_io
        import databricks_dbt_factory.main

        if not pathlib.Path(databricks_dbt_factory.main.__file__).is_relative_to(wheel):
            raise AssertionError(f"package imported outside the wheel: {databricks_dbt_factory.main.__file__}")

        distribution = importlib.metadata.distribution("databricks-dbt-factory")
        entry_point = next(
            entry
            for entry in distribution.entry_points
            if entry.group == "console_scripts" and entry.name == "databricks_dbt_factory"
        )
        sys.argv = ["databricks_dbt_factory", "--help"]
        entry_point.load()()
        """
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-I", "-c", script, str(wheel)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert (
        result.returncode == 0
    ), f"isolated wheel execution failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "--dbt-manifest-path" in result.stdout


def test_direct_and_sdist_wheels_ship_and_execute_the_complete_package(tmp_path):
    """Both supported Hatch build paths produce a complete, runnable distribution."""
    expected_package_files = _source_package_files()
    assert expected_package_files

    for wheel in (_build_direct_wheel(tmp_path), _build_wheel_from_sdist(tmp_path)):
        with zipfile.ZipFile(wheel) as archive:
            packaged_files = set(archive.namelist())

        assert expected_package_files <= packaged_files
        assert any(name.endswith(".dist-info/entry_points.txt") for name in packaged_files)
        _assert_wheel_executes_in_isolation(wheel, tmp_path)
