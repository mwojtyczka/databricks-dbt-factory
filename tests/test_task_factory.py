import pytest

from databricks_dbt_factory.DbtTask import DbtTaskOptions
from databricks_dbt_factory.TaskFactory import (
    DbtDependencyResolver,
    ModelTaskFactory,
    SeedTaskFactory,
    SnapshotTaskFactory,
    TestTaskFactory as DbtTestTaskFactory,
)


FACTORY_TYPES = (ModelTaskFactory, SnapshotTaskFactory, SeedTaskFactory, DbtTestTaskFactory)


@pytest.mark.parametrize(
    ("dbt_options", "reserved_option"),
    [
        pytest.param("--select beta", "--select", id="select"),
        pytest.param("--select=beta", "--select", id="select-equals"),
        pytest.param("-s beta", "-s", id="select-short"),
        pytest.param("-sbeta", "-s", id="select-short-attached"),
        pytest.param("-s=beta", "-s", id="select-short-equals"),
        pytest.param("-xsbeta", "-s", id="select-short-clustered-after-fail-fast"),
        pytest.param("'-xsbeta'", "-s", id="select-short-clustered-quoted"),
        pytest.param("--models beta", "--models", id="models"),
        pytest.param("--models=beta", "--models", id="models-equals"),
        pytest.param("--model beta", "--model", id="model"),
        pytest.param("--model=beta", "--model", id="model-equals"),
        pytest.param("-m beta", "-m", id="models-short"),
        pytest.param("-mbeta", "-m", id="models-short-attached"),
        pytest.param("-m=beta", "-m", id="models-short-equals"),
        pytest.param("-qmbeta", "-m", id="models-short-clustered-after-quiet"),
        pytest.param("'-qmbeta'", "-m", id="models-short-clustered-quoted"),
        pytest.param("--exclude alpha", "--exclude", id="exclude"),
        pytest.param("--exclude=alpha", "--exclude", id="exclude-equals"),
        pytest.param("--selector nightly", "--selector", id="selector"),
        pytest.param("--selector=nightly", "--selector", id="selector-equals"),
        pytest.param("--resource-type model", "--resource-type", id="resource-type"),
        pytest.param("--resource-type=model", "--resource-type", id="resource-type-equals"),
        pytest.param("--resource-types model", "--resource-types", id="resource-types"),
        pytest.param("--exclude-resource-type test", "--exclude-resource-type", id="exclude-resource-type"),
        pytest.param("--exclude-resource-types test", "--exclude-resource-types", id="exclude-resource-types"),
        pytest.param("--", "--", id="option-delimiter"),
    ],
)
def test_task_factory_rejects_options_that_can_change_selection(dbt_options, reserved_option):
    with pytest.raises(ValueError) as error:
        ModelTaskFactory(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), dbt_options)

    assert reserved_option in str(error.value)
    assert "selection" in str(error.value)


@pytest.mark.parametrize("factory_type", FACTORY_TYPES)
def test_every_task_factory_validates_dbt_options(factory_type):
    with pytest.raises(ValueError, match="--select"):
        factory_type(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), "--select beta")


def test_task_factory_revalidates_dbt_options_when_they_are_reassigned():
    factory = ModelTaskFactory(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), "--target dev")

    with pytest.raises(ValueError, match="--exclude"):
        factory.dbt_options = "--exclude alpha"

    assert factory.dbt_options == "--target dev"


def test_task_factory_accepts_factory_target_and_pinned_indirect_selection():
    dbt_options = "--target 'qa environment' --indirect-selection eager -x -rmodels.json"

    factory = DbtTestTaskFactory(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), dbt_options)

    assert factory.dbt_options == dbt_options


@pytest.mark.parametrize(
    "dbt_options",
    [
        "--target dev",
        "--target=dev",
        "-t dev",
        "-tdev",
        "--target '-sprod'",
        "-t '--project-dir'",
    ],
)
def test_task_factory_accepts_option_value_that_resembles_a_selection_option(dbt_options):
    factory = ModelTaskFactory(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), dbt_options)

    assert factory.dbt_options == dbt_options


@pytest.mark.parametrize("dbt_options", ["--target", "-t", "--target=", "--target ''", "-t ''"])
def test_task_factory_rejects_a_target_without_a_nonempty_value(dbt_options):
    with pytest.raises(ValueError, match="target requires a nonempty value"):
        ModelTaskFactory(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), dbt_options)


def test_task_factory_accepts_unambiguous_option_value_with_a_reserved_short_prefix():
    dbt_options = "--log-path=-models"

    factory = ModelTaskFactory(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), dbt_options)

    assert factory.dbt_options == dbt_options


def test_task_factory_rejects_an_ambiguous_spaced_option_value_with_a_reserved_short_prefix():
    with pytest.raises(ValueError, match="-m"):
        ModelTaskFactory(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), "--log-path -models")


@pytest.mark.parametrize(
    ("dbt_options", "target_option"),
    [
        pytest.param("--target dev --target prod", "--target", id="duplicate-long"),
        pytest.param("--target dev --target=prod", "--target", id="duplicate-long-equals"),
        pytest.param("--target dev -t prod", "-t", id="duplicate-short"),
        pytest.param("--target dev -tprod", "-t", id="duplicate-short-attached"),
        pytest.param("--target=dev --target prod", "--target", id="duplicate-after-leading-equals"),
        pytest.param("-tdev -tprod", "-t", id="duplicate-after-leading-short-attached"),
        pytest.param("--fail-fast --target dev", "--target", id="later-long"),
        pytest.param("--fail-fast --target=dev", "--target", id="later-long-equals"),
        pytest.param("--fail-fast -t dev", "-t", id="later-short"),
        pytest.param("--fail-fast -tdev", "-t", id="later-short-attached"),
    ],
)
def test_task_factory_rejects_a_target_that_is_not_the_single_leading_option(dbt_options, target_option):
    with pytest.raises(ValueError) as error:
        ModelTaskFactory(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), dbt_options)

    assert target_option in str(error.value)
    assert "at most one target" in str(error.value)
    assert "leading" in str(error.value)


@pytest.mark.parametrize(
    ("dbt_options", "parse_context_option"),
    [
        pytest.param("--vars '{enable_alpha: false}'", "--vars", id="vars"),
        pytest.param("--vars='{enable_alpha: false}'", "--vars", id="vars-equals"),
        pytest.param("--profile prod", "--profile", id="profile"),
        pytest.param("--profile=prod", "--profile", id="profile-equals"),
        pytest.param("--profiles-dir profiles", "--profiles-dir", id="profiles-dir"),
        pytest.param("--profiles-dir=profiles", "--profiles-dir", id="profiles-dir-equals"),
        pytest.param("--project-dir project", "--project-dir", id="project-dir"),
        pytest.param("--project-dir=project", "--project-dir", id="project-dir-equals"),
    ],
)
def test_task_factory_rejects_parse_context_that_can_drift_from_the_manifest(dbt_options, parse_context_option):
    with pytest.raises(ValueError) as error:
        ModelTaskFactory(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), dbt_options)

    assert parse_context_option in str(error.value)
    assert "runtime parse context" in str(error.value)
    assert "supplied manifest" in str(error.value)


@pytest.mark.parametrize(
    ("dbt_options", "target_option"),
    [("--log-path --target -sother", "--target"), ("--log-path -t -sother", "-t")],
)
def test_task_factory_rejects_an_ambiguous_later_target(dbt_options, target_option):
    with pytest.raises(ValueError, match=target_option):
        ModelTaskFactory(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), dbt_options)


def test_task_factory_rejects_malformed_dbt_option_quoting():
    with pytest.raises(ValueError, match="parse dbt command options"):
        ModelTaskFactory(DbtDependencyResolver(), DbtTaskOptions(task_type="dbt"), "--vars '{broken: true}")


def test_task_factory_rejects_dynamic_dbt_options_that_can_change_selection_at_runtime():
    with pytest.raises(ValueError, match="dynamic value"):
        ModelTaskFactory(
            DbtDependencyResolver(),
            DbtTaskOptions(task_type="dbt"),
            "{{job.parameters.dbt_options}}",
        )


@pytest.mark.parametrize("factory_type", FACTORY_TYPES)
def test_every_task_factory_rejects_a_dynamic_reference_formed_by_final_command_assembly(factory_type):
    factory = factory_type(
        DbtDependencyResolver(),
        DbtTaskOptions(task_type="dbt"),
        "--log-path=`}}",
    )

    with pytest.raises(ValueError, match="final dbt command.*dynamic value reference"):
        factory.create_task(
            "fqn:pkg.{{job.parameters.`/orders,package:pkg,file:orders.sql,resource_type:model",
            "orders",
            {"depends_on": {"nodes": []}},
            "orders_model",
            {},
        )


def test_bundled_test_factory_rejects_a_dynamic_reference_formed_by_final_command_assembly():
    factory = DbtTestTaskFactory(
        DbtDependencyResolver(),
        DbtTaskOptions(task_type="dbt"),
        "--log-path=`}}",
    )

    with pytest.raises(ValueError, match="final dbt command.*dynamic value reference"):
        factory.create_bundled_task(
            "orders_test",
            {"empty": ["fqn:pkg.{{job.parameters.`/orders,package:pkg,file:orders.sql,resource_type:test"]},
            "orders",
            ["orders_model"],
        )
