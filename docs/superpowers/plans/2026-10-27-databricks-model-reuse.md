# Databricks Model Reuse Implementation Plan

> **For agentic workers:** Use the executing-plans skill to implement this plan task by task in this session. Steps use checkbox syntax for tracking. Delegate only if the user requests it.

**Goal:** Reuse unchanged, explicitly opted-in production SQL table models using Databricks evidence, while rerunning tests and preserving Factory's Lakeflow graph and failure behavior.

**Architecture:** Add a preparation task to each generated job containing eligible models, followed by a reuse check in the existing notebook runner. Preparation uses public dbt artifacts and state selection; model tasks check current UC/Delta evidence before deciding to reuse or invoke dbt. Delta tables hold evidence and decisions, and a UC Volume holds immutable artifacts and preparation results.

**Tech stack:** Python, the existing Factory package and Databricks SDK, native Spark SQL for Delta metadata/state operations, dbt's public invocation/artifact interfaces, Unity Catalog, and Lakeflow Jobs. dbt v1 and v2 run in separate environments.

**Scope:** This is a plan, not authorization to deploy or a claim that reuse is implemented. The source of requirements is [the approved design](../specs/2026-10-23-databricks-model-reuse-design.md). dbt 2.0 compatibility is a first-release acceptance requirement; native `dbt_task` remains available without reuse.

---

## Language and deployment decision

Use Python for orchestration and policy, and Databricks SQL operations for metadata and durable state. The local work is reading JSON, hashing small context records, grouping baseline comparisons, and invoking native engines. Avoid reimplementing dbt parsing, macro dependency analysis, SQL compilation, or Delta log interpretation.

| Option | Benefit | Cost | Decision |
| --- | --- | --- | --- |
| Python + native SQL | Fits the existing package, notebook runner, SDK and tests | Python process/import overhead must be measured | Build this |
| Rust | Can accelerate proven CPU-heavy local processing | Another build, wheel/native platform support, and language boundary | Consider only after profiling identifies Python CPU as material |
| Scala | Direct JVM/Spark access | Additional packaging/runtime surface beside the Python/dbt runner | No measured benefit for this feature |

There is no separate service, queue, database server, parser, or generic backend interface. Use the existing notebook Spark session for metadata/state SQL; do not introduce another SQL warehouse solely for this feature. Confirm that the required metadata commands work on both supported notebook compute modes in Task 2. Record their runner-compute and startup costs rather than assuming metadata access is free.

Reuse-enabled notebooks need the matching Factory wheel installed in their selected job cluster or serverless environment. Today, the auto-copied runner is self-contained; this feature must make its additional runtime-package requirement explicit. Non-reuse execution keeps its existing deployment requirements. Install the dbt v2 `dbt` distribution and its Databricks driver separately from v1's `dbt-core`/`dbt-databricks` environment.

## File map

All paths below are relative to the repository root.

| File | Responsibility |
| --- | --- |
| `src/databricks_dbt_factory/reuse.py` (new) | Pure eligibility, descriptors, canonical context/evidence comparison; no SDK, Spark, or dbt imports |
| `src/databricks_dbt_factory/reuse_runtime.py` (new) | Concrete UC Volume/Delta evidence access and live relation snapshots using passed SDK/Spark objects |
| `src/databricks_dbt_factory/reuse_prepare.py` (new) | Run-scoped preparation, baseline grouping, and public dbt state comparison |
| `src/databricks_dbt_factory/notebook/prepare_dbt_reuse.py` (new) | Thin notebook entry point for preparation |
| `src/databricks_dbt_factory/notebook/run_dbt_command.py` | Check reuse before importing dbt; execute and publish evidence on a miss |
| `src/databricks_dbt_factory/main.py` | Optional config/provenance arguments, immutable local artifacts, and runner publication |
| `src/databricks_dbt_factory/dbt_factory.py` | Exact selectors, added declared-input edges, per-job preparation integration |
| `src/databricks_dbt_factory/dbt_task.py` | Optional model identity/artifact parameters; include them in parameter-size validation |
| `src/databricks_dbt_factory/job_partition.py` | Partition placement and task limits including preparation |
| `src/databricks_dbt_factory/job_spec.py` | Reuse-enabled concurrency validation and force-run parameter propagation |
| `tests/test_reuse.py`, `tests/test_reuse_runtime.py` (new) | Pure contracts and concrete storage/runner failure behavior |
| `tests/integration/test_dbt_state_comparison.py` (new) | Real engine-generated baseline artifacts and native comparisons |
| `tests/integration/test_notebook_reuse.py` (new) | Complete shipped notebook execution with controlled metadata/dbt boundaries |
| Existing selector, CLI, distribution, partition and job-spec tests | Preserve Factory's existing guarantees |
| `tests/databricks/reuse_acceptance.py` (new) | Runnable acceptance/benchmark scenarios against explicitly chosen scratch resources |
| `README.md`, `CONTRIBUTING.md`, `.github/workflows/integration.yml`, `pyproject.toml` | Runtime setup, isolated engine checks, documented support and operating procedure |

Keep modules as functions and small immutable records. Add a class only where it holds concrete shared state; do not introduce a storage-provider or engine-plugin hierarchy. Reuse `atomic_write_bytes`, existing task-key maps, graph gates, notebook publishing, and bundle-schema validation.

## Contracts to establish before the reuse path

The following names are proposed API/config names, not existing functionality.

Model opt-in uses dbt's existing config metadata:

```yaml
models:
  - name: orders
    config:
      materialized: table
      meta:
        dbt_factory:
          reuse:
            enabled: true
            extra_inputs:
              - catalog: raw
                schema: sales
                identifier: exchange_rates
```

Opt-in attests deterministic behavior and complete declarations; a second boolean does not prove determinism. `ref()`/`source()` supply ordinary graph dependencies. Additional inputs use separate identifier components, avoiding unsafe splitting of dotted SQL identifiers.

Generation takes `--reuse-config <file>` plus provenance captured when the supplied manifest was produced. Required config fields are `format_version`, `project_id`, `environment`, `owner_key`, `state_catalog`, `state_schema`, and `artifact_volume` (catalog/schema/volume components). Provenance binds manifest bytes, immutable project/package content, engine/driver versions, effective target/relation mapping, declared non-secret context inputs and execution identity. Generation does not infer provenance from a pre-existing manifest. Missing provenance permits ordinary execution but never reuse.

Capture provenance in the same parse workflow, through a small helper in `reuse_prepare.py`; make it callable from the existing build pipeline. Secret-dependent context requires a trustworthy non-secret version identifier or makes reuse ineligible. Do not store credentials, raw secrets, or unsalted hashes of low-entropy secrets. Mutable project snapshots or task-local dependency resolution cannot authorize reuse: deploy locked packages and a content-verified project, or execute normally.

Persist three Delta tables under the configured state schema:

| Table | Key and content |
| --- | --- |
| `factory_reuse_owners` | Environment/output identity, logical owner, deployed job binding; conflicting claims are configuration errors |
| `factory_reuse_builds` | Successful attempt identity; model/environment, artifact/context references, pre/post input evidence, output evidence and original completion time |
| `factory_reuse_decisions` | Run/task/attempt/model, decision/reason, evidence references, observation time and measured check durations |

Build records are append-only and attempt-idempotent; a test result does not change build evidence. Selection of an applicable build always verifies its output identity/version and artifact/context, rather than trusting a latest timestamp alone. A decision write cannot manufacture build success. No multi-table transaction is assumed: partial state publication loses optimization, never authorizes reuse without complete evidence.

Use an identity/version tuple per relation: fully qualified name, UC identity when available, Delta table identity and current Delta version. If reliable identity cannot be established, execute normally. UC access checks and SQL reads use the job's execution identity. Recheck identity around version observation to reject mixed observations during recreation. Do not read parquet data or decode Delta log files in Python.

Preparation publishes an immutable result containing deployment digest, job/run/preparation attempt, exact baseline build references, comparison outcomes and fallback reasons. Pass only its URI and digest through task values; keep full manifests/evidence out of notebook parameters. Retrieve a fresh pointer on repair, revalidate baseline references, and fall back to execution if preparation evidence is stale or unavailable.

Resolve ownership before enabling reuse, and validate `max_concurrent_runs: 1` for every owning generated job, including split children and their orchestrator. A registry can detect a conflicting declared owner; it is not a distributed lock and cannot prevent unregistered external writers. Live output identity/version checks detect observed external mutations. Do not promise atomic cross-table reads or protection against concurrent mutation after a check.

For definition-backed dependencies, store a separate definition digest and the resolved physical Delta input closure. A dbt-managed view needs its deployed UC definition checked as well as its dbt code; an ephemeral model needs its manifest definition/dependencies checked. Unknown external view/UDF dependencies remain ineligible. Code comparisons for a consumer include changes to these definitions even if every physical input version is unchanged.

## Task 1: Establish real dbt v2 selector and runner compatibility

**Files:** Modify `dbt_factory.py`, `notebook/run_dbt_command.py`, `tests/integration/test_selector_against_dbt.py`, `tests/integration/dbt_helpers.py`, `.github/workflows/integration.yml`; add isolated v2 test dependencies in the workflow.

- [ ] Add a real generic-test regression to the selector integration suite and run it first against dbt 2.0.9. A minimal fixture is:

  ```python
  @pytest.mark.parametrize("bundle_tests", [False, True])
  def test_generic_test_selection_on_current_engine(tmp_path, bundle_tests):
      _write_project(
          tmp_path,
          {"a.sql": "select 1 as id\n"},
          schema_yml="version: 2\nmodels:\n  - name: a\n    columns:\n      - name: id\n        data_tests: [not_null]\n",
      )
      manifest = _parse(tmp_path)
      _assert_each_task_selects_its_own_node(tmp_path, manifest, bundle_tests=bundle_tests)
  ```

- [ ] Set up a separate v2 environment. Do not install it into the existing v1 Hatch environment:

  ```sh
  uv venv --python 3.12 /tmp/dbt-factory-plan-v2
  uv pip install --python /tmp/dbt-factory-plan-v2/bin/python -e . 'dbt==2.0.9' pytest pytest-cov pytest-mock pytest-timeout pytest-xdist jsonschema
  /tmp/dbt-factory-plan-v2/bin/python -m pytest tests/integration/test_selector_against_dbt.py -k generic_test_selection_on_current_engine -v
  ```

  Expected initial failure: empty selection for the emitted generic-test selector. Move v1 msgpack imports out of module scope before collecting engine-neutral selector tests. Only injection-specific tests are v1-only; selector tests must execute on v2.

- [ ] Reproduce the failing `file:schema.yml` term using `dbt ls`. Change the shared selector construction/validation based on observed behavior. Prefer a selector strategy valid on both engines if exactness can be proved; otherwise make the relevant engine distinction explicit from provenance. Re-run collision, package, versioned-model, shared-schema, source, unit-test and bundled-mode fixtures. Refuse layouts whose exactness cannot be established; do not merely delete the file term and trust the FQN.
- [ ] Retain the v1 injection path where already supported. For v2, use the public runner without injecting a manifest. Check successful parse and failed-command result handling through the complete shipped notebook. Disable hosted State management for the Databricks-owned v2 mode.
- [ ] Add a separate Python 3.12/v2 CI job with no v1 adapter packages; keep the existing v1 checks. Run engine-neutral selector/runner tests in both. Report exact engine versions and failures; do not skip a v2 selector failure because it differs from v1.
- [ ] Verify: `make integration` on v1, plus the selector suite in the isolated v2 environment. Commit: `fix: support exact task selection on dbt v2`.

**Exit:** Both engines select intended resources exactly in per-test and bundled modes. The local probe previously established feasibility; it did not establish this exit condition.

## Task 2: Verify Databricks primitives before implementing the state store

**Files:** Create `tests/databricks/reuse_acceptance.py`; document its setup in `CONTRIBUTING.md`.

- [ ] Have the user select a Databricks profile before the first workspace operation. Require explicit scratch catalog/schema/volume and compute identifiers. Local planning needs no profile and performs no deployment.
- [ ] Build a small notebook probe using the actual execution identity:

  ```python
  detail = spark.sql("DESCRIBE DETAIL `scratch`.`factory_probe`.`input_table`").first().asDict()
  version = spark.sql("DESCRIBE HISTORY `scratch`.`factory_probe`.`input_table` LIMIT 1").first().asDict()["version"]
  assert detail["format"] == "delta"
  assert detail["id"] and isinstance(version, int)
  ```

  The named scratch resources are created by the harness in the user-selected workspace. Read the corresponding UC table identity using the SDK, then repeat after append and drop/recreate. Assert that append advances version and recreation cannot be mistaken for the old table at the same version.
- [ ] Probe schema/table/view classification, deployed UC view-definition reads and changes, Volume publish/read integrity, state-table write access, and access to `system.access.table_lineage`. Record empty lineage separately from permission/query failure. Query with date and target filters; absence of records never establishes completeness.
- [ ] Run on the chosen job compute and serverless notebook modes. Record cold/warm latency and query/compute usage. Unsupported metadata access makes the affected workload ineligible; do not introduce a second backend before confirming the supported path.
- [ ] Verify the same job execution principal is used by Spark, SDK and dbt. Check dbt v2 driver installation/download behavior in the actual compute environment.
- [ ] Commit: `test: verify Databricks reuse metadata primitives`.

**Exit:** Identity, Delta versions, storage and execution context have real platform evidence. This is the first platform checkpoint and can block rollout without blocking local contract work.

## Task 3: Define minimal configuration, eligibility and provenance

**Files:** Create `reuse.py`, the provenance-capture portion of `reuse_prepare.py`, and `tests/test_reuse.py`; modify `main.py`, `tests/test_app.py`.

- [ ] Write failing config/eligibility tests: absent opt-in, malformed metadata, SQL table opt-in, incremental/Python/custom materialization, hooks, unknown context, ambiguous relation and undeclared extra input. Verify opt-out generation is unchanged.
- [ ] Define immutable descriptor/context records and `eligibility_reason(model, context)` in `reuse.py`. Return explicit reasons for optimization ineligibility; raise resource-naming `ValueError` for malformed declarations and graph contract errors. Start with table-backed dependencies; unsupported view/UDF closure executes normally.
- [ ] Add validated `--reuse-config` and provenance input handling in `main.py`. Reject requesting reuse with native task mode. Do not add SDK/dbt imports to the offline generation path. Compare canonical context with stdlib `json.dumps(..., sort_keys=True, separators=(",", ":"))` and SHA-256; keep source-artifact identity separate from semantic context so unrelated code edits do not invalidate every model.
- [ ] Capture manifest/project/package/context provenance together in the build helper. Verify project/package content once in preparation against an immutable execution snapshot; model tasks recheck the pinned deployment reference and their current context, without rehashing the whole project per model. A known runtime graph/artifact mismatch fails explicitly. Missing trustworthy context yields `CONTEXT_UNKNOWN` and normal execution. Package macro/model source edits remain subject to selective native comparison; runtime engine/driver/library changes invalidate broadly.
- [ ] Verify: `hatch run pytest tests/test_reuse.py tests/test_app.py -v`. Commit: `feat: define model reuse policy and provenance`.

**Exit:** Eligibility and context comparison are testable without Spark, SDK, or dbt installed. No skip decision is emitted yet.

## Task 4: Publish immutable artifacts and make runtime packaging explicit

**Files:** Create `reuse_runtime.py` and `tests/test_reuse_runtime.py`; modify `main.py`, `tests/test_app.py`, `tests/integration/test_distribution.py`. Reuse `file_io.py` without changing it unless publication checks establish a necessary change.

- [ ] Write failure-first checks for a wrong digest, partial file, existing different content, missing artifact, and a runtime wheel whose version differs from the descriptor. Extend the installed-wheel test to read packaged notebook resources and import the new pure module.
- [ ] Generate content-addressed manifest, provenance and descriptor files locally using `atomic_write_bytes`. Extend the existing runner-publication helper narrowly to publish the preparation notebook as a second immutable resource. Dry-run remains write-free.
- [ ] Add `publish_artifacts(workspace_client, local_bundle, volume_path)` in `reuse_runtime.py`. Upload without overwriting; if content exists, verify its digest. Return artifact references only after complete upload/verification. Do not rely on a cross-object atomic rename. Verify bytes again before using downloaded comparison state.
- [ ] Document installation of the matching wheel in existing cluster/serverless environment dependencies, then artifact publication before bundle deployment. Do not silently replace user environments or move dbt dependencies into the Factory CLI. The reuse path checks its runtime/descriptor compatibility before reuse.
- [ ] Verify: `hatch run pytest tests/test_app.py tests/test_reuse_runtime.py tests/integration/test_distribution.py -v`. Commit: `feat: publish immutable reuse artifacts`.

**Exit:** Jobs and successful-build records can refer only to published, digest-verified artifacts. Publication failure leaves no new reusable evidence.

## Task 5: Implement concrete Delta evidence and relation snapshots

**Files:** Implement `reuse_runtime.py`; test in `tests/test_reuse_runtime.py` and `tests/databricks/reuse_acceptance.py`.

- [ ] Write failing tests for changed input/output version, renamed/recreated table, mixed identity observations, non-Delta input, unreadable metadata, duplicate attempt publication and conflicting declared ownership. Include state-write failure after a successful model execution.
- [ ] Implement `initialize_state`, `read_build`, `record_build`, `record_decision`, `claim_owner`, and `snapshot_relation` against the three fixed Delta tables and UC metadata. Pass the existing SDK/Spark objects; keep a single concrete implementation. Initialization is an explicit setup operation, not table creation on every model task.
- [ ] Quote SQL identifier components by doubling embedded backticks and surrounding each component with backticks. Use SQL parameter bindings for data values; do not concatenate model names, paths or JSON values into SQL literals. Preserve case/quoted identifier semantics in relation mapping.
- [ ] Make attempt publication idempotent and verify an existing attempt's evidence matches. Never overwrite a different attempt or another environment's state. Persist the original build time separately from later reuse decisions. Let Delta write conflicts/unavailable state lose optimization with a visible reason.
- [ ] Reuse eligibility requires both successful model execution and stable input observations before/after it. Record the pre-build input versions as consumed evidence; do not substitute newer post-build observations. Missing output or unstable identity withholds reusable evidence.
- [ ] Verify: `hatch run pytest tests/test_reuse_runtime.py -v`, then the corresponding scratch Delta scenarios from Task 2. Commit: `feat: persist Databricks model build evidence`.

**Exit:** A malformed, partial, stale or unverified record cannot authorize reuse. State-write failure cannot change successful dbt execution into failure.

## Task 6: Add native selective comparison and run-scoped preparation

**Files:** Extend `reuse_prepare.py` and `tests/test_reuse.py`; create `notebook/prepare_dbt_reuse.py`, `tests/integration/test_dbt_state_comparison.py`.

- [ ] Write real-project baseline tests for model SQL, transitive macro, unused macro, unrelated model, dependency mapping, reuse metadata, engine version, declared vars/environment and partial previous failures. Produce baseline/current manifests with the tested engine. Keep them in different directories.
- [ ] Invoke the installed engine with an argument list and separate current output directories:

  ```text
  dbt ls --select state:modified --resource-type model --state BASELINE_DIRECTORY --output json --output-keys unique_id
  ```

  Add the validated project/profile/target settings and v2 `--no-manage-state`. Parse only node JSON records; retain command diagnostics. A selector/configuration/graph error fails; an unavailable optimization artifact or unsupported comparison produces a visible normal-execution decision.
- [ ] Group models by their own successful-build manifest and semantic context. Execute one native comparison per distinct valid baseline group, not per model or only against the previous deployment. An engine/driver upgrade invalidates broadly without cross-engine comparison. Keep model-level code changes selective.
- [ ] Compare Factory policy, relation mapping and execution context separately from native state results. Track dbt-managed view/ephemeral definitions and walk their declared dependency graph to physical Delta leaves; intersect native changed-node results with that definition closure. For views, also snapshot the deployed UC definition using the metadata access verified in Task 2. Add real changed-view/ephemeral fixtures with unchanged physical input versions and assert that consumers rebuild. Unresolved definitions or incomplete physical closures make the consumer ineligible. Never infer SQL dependencies in Python.
- [ ] Publish immutable preparation output bound to the deployment, job/run/attempt and exact build references. Its task value contains a pointer/digest, not a large model map. Missing/state-unavailable preparation succeeds with reuse disabled; unexpected infrastructure errors retain normal failure behavior.
- [ ] Verify: `hatch run pytest tests/integration/test_dbt_state_comparison.py -v` and the same module in the isolated v2 environment. Assert invocation count equals distinct supported baseline groups. Commit: `feat: prepare selective reuse decisions with native dbt state`.

**Exit:** The agreed selective-invalidation USP is present before enabling skipping. Context changes cannot hide behind unchanged SQL-only state results.

## Task 7: Wire preparation, additional inputs and force controls into Lakeflow

**Files:** Modify `dbt_factory.py`, `dbt_task.py`, `job_partition.py`, `job_spec.py`, `main.py`; extend the corresponding existing unit/integration tests.

- [ ] Write failing graph checks for zero opted-in models, one preparation per eligible job, task-key collision, a declared extra input produced in the graph, a dependency cycle, conflicting ownership, split children, and 999/1,000 existing tasks when preparation adds one. Include conflicting existing job-parameter names and concurrency settings.
- [ ] Bind stable model IDs to the existing task-key maps. Add declared-input producer edges before test gates and partitioning are finalized; rerun existing cycle/gate validation. Add preparation inside each resulting eligible job and count it in the final task limit. Keep cross-partition ordering in the existing parent graph.
- [ ] Give eligible model tasks the preparation dependency under normal success gating. Never use `ALL_DONE`. Preparation does not depend on model tasks; live input checks happen later inside each model task. Keep test tasks and their downstream gate edges.
- [ ] Add descriptor/model/preparation pointer parameters only to relevant notebook tasks, retaining the existing serialized parameter-size limit and dynamic-reference validation. Use a collision-safe preparation task key drawn from the same allocation rules.
- [ ] Add an explicit force-run job parameter, `factory_force_rebuild`, defaulting to `false`. Propagate it through generated parent/child jobs and validate its value strictly at the runner. Force/full-refresh executes normally and still publishes fresh evidence when eligible. Validate owning jobs and orchestrator use `max_concurrent_runs: 1`; refuse conflicting explicit concurrency settings.
- [ ] Verify: `hatch run pytest tests/test_dbt_factory.py tests/test_dbt_task.py tests/test_job_partition.py tests/test_job_spec.py tests/test_app.py tests/integration/test_bundle_schema.py tests/integration/test_split_by_tag_against_dbt.py -v`. Regenerate changed golden files through the CLI, never by hand. Commit: `feat: integrate model reuse preparation with Lakeflow jobs`.

**Exit:** Existing test gates, partition semantics, bundle schema, repair structure and task limits hold with preparation enabled. Opt-out jobs preserve existing behavior.

## Task 8: Enable the reuse check before dbt initialization

**Files:** Modify `reuse.py`, `reuse_runtime.py`, `notebook/run_dbt_command.py`; create `tests/integration/test_notebook_reuse.py`.

- [ ] Specify the pure `reuse_reason` contract `(build, current, code_unchanged, context_unchanged, forced) -> reason`. A complete matching evidence record returns `REUSED`; missing state returns `NO_BASELINE`; changed context/code/input/output or force returns a normal-execution reason. Evidence records include explicit identities, versions and completeness, rather than relying on truthiness of partial dictionaries.
- [ ] Write and run the failing pure-policy checks before implementing that function:

  ```python
  assert reuse_reason(None, current, True, True, False) == "NO_BASELINE"
  assert reuse_reason(build, current, True, True, True) == "FORCED"
  assert reuse_reason(build, current, False, True, False) == "CODE_CHANGED"
  assert reuse_reason(build, current, True, False, False) == "CONTEXT_CHANGED"
  assert reuse_reason(build, current, True, True, False) == "REUSED"
  ```

  Construct `build` and `current` as complete matching input/output identity/version records using Task 5's contract; one-field input/output identity/version mutations must change the decision. Implement only the branches covered by these evidence scenarios and the specified fallbacks.
- [ ] Write the central failing check by executing the shipped notebook with matching evidence and an import guard:

  ```python
  import builtins

  original_import = builtins.__import__
  def reject_dbt_import(name, *args, **kwargs):
      if name == "dbt" or name.startswith("dbt."):
          raise AssertionError("reuse imported dbt")
      return original_import(name, *args, **kwargs)
  monkeypatch.setattr(builtins, "__import__", reject_dbt_import)
  ```

  Provide notebook widget/context stubs using the existing shipped-notebook test pattern. Assert successful notebook completion, a `REUSED` decision, and no model execution. This checks the actual import boundary, not just a policy helper.
- [ ] Move the runner's dbt import/injection/initialization after reuse evaluation. Preserve project resolution, authentication and per-task artifact isolation. Keep the SDK/Spark metadata path independent of dbt. On reuse, finish the model task successfully; do not use a skipped status or fabricate `run_results.json`.
- [ ] On a miss, snapshot inputs immediately before dbt invocation and again after successful execution, then snapshot output. Publish eligible evidence only for stable inputs and matching deployment/context. An input changing during the build records `INPUT_CHANGED_DURING_BUILD`, finishes normal execution, and forces the next invocation to rebuild.
- [ ] Publish build evidence before subsequent tests determine their own result; tests remain separate tasks. Failed/cancelled model invocations publish no success. Retain existing exception reporting and credential/temp-directory cleanup on all paths.
- [ ] Verify: `hatch run pytest tests/integration/test_notebook_reuse.py -v`, then repeat on v2 with public result handling. Commit: `feat: reuse unchanged models without initializing dbt`.

**Exit:** A second unchanged invocation can avoid model execution without importing dbt, and a failed data test does not erase valid model evidence.

## Task 9: Add lineage-based ineligibility without changing the dbt graph

**Files:** Extend `reuse_runtime.py`, `reuse_prepare.py`, `tests/test_reuse_runtime.py`, `tests/databricks/reuse_acceptance.py`.

- [ ] Write failing cases for an observed undeclared input, a declared extra input, an old edge needing review, empty lineage, and unavailable lineage metadata. Verify that no discovered edge is silently added to scheduling.
- [ ] Read filtered UC lineage during preparation and compare known inputs with the declared dependency closure. Persist blocking observations in decision evidence; a known blocker cannot disappear merely because the system-table retention window advanced. Clear it when a reviewed deployment explicitly declares the dependency. A historical edge is reviewed before it is declared; v1 adds no automatic expiry or ignore-list bypass.
- [ ] Unavailable optimization metadata disables reuse for affected models and reports the reason. Empty lineage does not establish completeness; model opt-in/declarations remain required. If a declared extra input is produced by this job, Task 7's explicit edge is mandatory.
- [ ] Verify: `hatch run pytest tests/test_reuse_runtime.py -v` and the live hidden-input scenario. Commit: `feat: disable model reuse for undeclared lineage inputs`.

**Exit:** Additional lineage makes reuse more conservative and explainable, without becoming an alternate compiler or scheduler.

## Task 10: Prove retries, repairs and optimization outages preserve execution

**Files:** Extend `tests/integration/test_notebook_reuse.py`, `tests/test_reuse_runtime.py`, `tests/databricks/reuse_acceptance.py`; change only production branches exposed by these checks.

- [ ] Add scenario assertions before fixing failures:

  | Scenario | Required behavior |
  | --- | --- |
  | State/artifact read unavailable | Run normal dbt; visible fallback |
  | State write fails after successful build | Model succeeds; no new reusable evidence |
  | Stale preparation pointer/build reference | Revalidate or execute; never trust a stale skip |
  | Input or output changes before repair | Execute affected model |
  | Failed model attempt | No successful-build evidence |
  | Failed required data test | Test fails and gates downstream; repaired model may reuse, test reruns |
  | Known graph/artifact mismatch, invalid selector or cycle | Fail explicitly |
  | A different environment/owner supplies matching table names | Refuse conflicting ownership or execute without reuse; never reuse its evidence |
  | Force parameter/full-refresh | Execute dbt and rebuild evidence |
  | Upstream input changes during execution | Complete normal build, withhold eligibility |

- [ ] Execute full notebook paths for read/write failures and stale pointers. Use real dbt fixture artifacts for graph/state behavior; use controlled SDK/SQL failures only at those external boundaries.
- [ ] Run repair with and without rerunning preparation. Recheck current input/output evidence and exact successful-build reference in both cases. A changed baseline can conservatively execute if no matching comparison result exists; an old plan cannot authorize reuse.
- [ ] Verify: `make lint`, `make test`, `make integration`, plus the isolated v2 engine checks. Commit: `test: preserve dbt execution guarantees during reuse failures`.

**Exit:** Missing optimization evidence cannot skip work, and reuse never bypasses a required test failure.

## Task 11: Run Databricks acceptance and measure net savings

**Files:** Complete `tests/databricks/reuse_acceptance.py`; record reproducible commands/results in `docs/superpowers/validation/databricks-model-reuse.md`.

- [ ] Deploy only to the explicitly selected scratch profile/resources using the existing generate → publish artifacts → bundle validate → deploy → run workflow. Always pass `--profile` and the intended bundle target. Use separate job/owner identities for v1 and v2 runs.
- [ ] Give the local acceptance driver this required-input interface, also used with `--scenario metadata` for Task 2 and `--scenario benchmark` for performance checks:

  ```sh
  python tests/databricks/reuse_acceptance.py \
    --profile "$FACTORY_PROFILE" \
    --catalog "$FACTORY_CATALOG" --schema "$FACTORY_SCHEMA" --volume "$FACTORY_VOLUME" \
    --bundle-dir "$FACTORY_BUNDLE_DIR" --target "$FACTORY_TARGET" \
    --engine dbt2 --scenario acceptance
  ```

  The operator supplies these values after choosing scratch resources; the bundle defines the chosen compute. The driver generates the probe notebook/job artifacts, invokes the documented bundle workflow with an explicit profile, checks task-level result states, and cleans up only resources it created. Do not delete user input tables or a pre-existing bundle during cleanup.
- [ ] Assert real Lakeflow outcomes for an initial build, unchanged rerun, one input append, one output mutation, drop/recreate, a maintenance commit advancing Delta version, unrelated SQL edit, transitive macro edit, var/environment change, failed test + repair, interrupted build, state outage, undeclared lineage input and split jobs. Maintenance version advances invalidate conservatively. Record model execution count and test execution count, not only green task status.
- [ ] Confirm reused model tasks succeed, dbt never initializes on those paths, tests rerun, downstream gates hold, and engine/driver upgrade forces a rebuild. Verify the native `dbt_task` option still executes normally without reuse.
- [ ] Benchmark with reuse off/on, cold/warm notebook compute, and cold/warm dbt SQL warehouse. Use short and expensive representative models, changing one input and many inputs. Include multiple distinct successful-build baselines after partial deployments/failures.
- [ ] Report warehouse compute, runner/preparation compute, metadata/state requests and write counts, baseline comparison count, retained test cost and wall time separately. Account for warehouse wake-up, repeated task startup, artifact/content verification and contention from small Delta writes. Use run/query identifiers to attribute work; do not estimate savings from skipped model count alone. Compare full runs with tests enabled in both, so retained test cost is included without being counted twice as new overhead; apply the appropriate SKU rates for cost comparisons.
- [ ] Require positive measured net compute savings for the adopted workload. Publish the break-even workload and retained startup latency. If metadata/state writes dominate, first reduce redundant reads and batch preparation work; consider another implementation language only if profiling specifically identifies Python CPU cost.
- [ ] Commit: `test: validate model reuse on Databricks and measure overhead`.

**Exit:** Both supported engines pass platform checks and the target workload saves compute after all checking/recording costs. Local tests alone cannot satisfy this gate.

## Task 12: Document supported operation and release the opt-in feature

**Files:** Modify `README.md`, `CONTRIBUTING.md`, `pyproject.toml`, CI workflow and the acceptance report.

- [ ] Document model metadata/config, provenance capture, wheel/driver setup, explicit state initialization, grants, artifact publication, one-owner/non-overlapping-run contract and decision queries. Include the exact tested engine/runtime versions and supported compute modes.
- [ ] Document normal-execution fallbacks and configuration errors separately. Explain that lineage absence is not proof, table maintenance may invalidate reuse, and the feature does not pin source snapshots or prevent external writers.
- [ ] Provide a rollback procedure: disable the global reuse config or model opt-in, regenerate/redeploy normal tasks, and retain referenced artifacts/evidence. No artifact garbage collector is required for v1. Ownership reassignment must disable/drain the old job before registering the new owner.
- [ ] Run `make lint`, `make test`, `make integration`, the isolated v2 checks, installed-wheel verification and bundle-schema validation. Regenerate any affected golden outputs with the real CLI and inspect the diff.
- [ ] Commit: `docs: document Databricks model reuse deployment and operation`.

**Exit:** Reuse is explicitly opt-in, supported versions/limits are stated, and the normal execution path is the documented rollback.

## Delivery sequence and review checkpoints

1. **Compatibility:** Task 1 is a useful standalone fix. Tasks 2–4 establish platform evidence and artifact contracts without skipping models.
2. **Complete local path:** Tasks 5–8 implement selective code comparison plus live data checks. Keep the feature unadvertised/disabled until lineage and failure checks pass.
3. **Correctness:** Tasks 9–10 complete required lineage/fallback/repair behavior.
4. **Release:** Tasks 11–12 establish actual compatibility and savings, then document the opt-in feature.

The critical path is correctness and platform validation, not choosing a faster local language. Do not replace selective invalidation with a project-wide checksum to shorten delivery. Do not add incremental-model reuse, native-task skip gates, development clones, automatic lineage adoption, distributed locks, freshness windows, column/row change inference or a UI to this plan.

## Plan self-review

- Every design requirement maps to a task: opt-in/context/eligibility (3), immutable artifacts (4), identities/ownership/evidence (2, 5), per-model baselines and native selective invalidation (6), extra edges/splits/task limits/force (7), no-dbt reuse and stable build observations (8), lineage (9), tests/fallbacks/repair (10), and measured acceptance (11–12).
- Whole-project content verification detects deployment/runtime drift; semantic context and native state determine reuse across valid deployments. These are different comparisons.
- Successful model builds and data-test outcomes remain separate. Every reused task succeeds normally and preserves its downstream gates.
- Platform operations require an explicitly selected profile. No resources have been selected or deployed by writing this plan.
- The first-release language choice is Python; Rust/Scala remain profiling-driven alternatives, not implementation work.
