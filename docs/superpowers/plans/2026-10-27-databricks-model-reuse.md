# Databricks Model Reuse Implementation Plan

> **For agentic workers:** Use the executing-plans skill to implement this plan task by task in this session. Steps use checkbox syntax for tracking. Delegate only if the user requests it.

**Goal:** Reuse unchanged, explicitly opted-in production SQL table models using Databricks evidence, while rerunning tests and preserving Factory's Lakeflow graph and failure behavior.

**Architecture:** Add a preparation task to each generated job containing eligible models, followed by a reuse check in the existing notebook runner. Preparation uses public dbt artifacts and state selection; model tasks check current UC/Delta evidence before deciding to reuse or invoke dbt. Delta tables hold evidence and decisions, and a UC Volume holds immutable artifacts and preparation results.

**Tech stack:** Python, the existing Factory package and Databricks SDK, native Spark SQL for Delta metadata/state operations, dbt's public invocation/artifact interfaces, Unity Catalog, and Lakeflow Jobs. Use the existing dbt v1 environment.

**Scope:** This is a plan, not authorization to deploy or a claim that reuse is implemented. The source of requirements is [the approved design](../specs/2026-10-23-databricks-model-reuse-design.md). The first release targets the existing dbt v1 setup. Separate dbt 2.0 compatibility work, environments and CI checks are deferred; native `dbt_task` remains available without reuse.

---

## Language and deployment decision

Use Python for orchestration and policy, and Databricks SQL operations for metadata and durable state. The local work is reading JSON, hashing small context records, grouping baseline comparisons, and invoking native engines. Avoid reimplementing dbt parsing, macro dependency analysis, SQL compilation, or Delta log interpretation.

| Option | Benefit | Cost | Decision |
| --- | --- | --- | --- |
| Python + native SQL | Fits the existing package, notebook runner, SDK and tests | Python process/import overhead must be measured | Build this |
| Rust | Can accelerate proven CPU-heavy local processing | Another build, wheel/native platform support, and language boundary | Consider only after profiling identifies Python CPU as material |
| Scala | Direct JVM/Spark access | Additional packaging/runtime surface beside the Python/dbt runner | No measured benefit for this feature |

There is no separate service, queue, database server, parser, or generic backend interface. Use the existing notebook Spark session for metadata/state SQL; do not introduce another SQL warehouse solely for this feature. Use serverless notebook compute for preparation and model tasks, and a serverless SQL warehouse for dbt queries. Confirm the required metadata commands in Task 1. Record their runner-compute and startup costs rather than assuming metadata access is free.

Reuse-enabled notebooks need the matching Factory wheel installed in their serverless environment. Today, the auto-copied runner is self-contained; this feature must make its additional runtime-package requirement explicit. Non-reuse execution keeps its existing deployment requirements. Keep the existing `dbt-core`/`dbt-databricks` environment.

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
| `README.md`, `CONTRIBUTING.md` | Runtime setup, documented support and operating procedure |

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

## Task 1: Verify Databricks primitives before implementing the state store

**Files:** Create `tests/databricks/reuse_acceptance.py`; document its setup in `CONTRIBUTING.md`.

- [x] Have the user select a Databricks profile before the first workspace operation. Require explicit scratch catalog/schema/volume and compute identifiers. Local planning needs no profile and performs no deployment.
- [x] Build a small notebook probe using the actual execution identity:

  ```python
  detail = spark.sql("DESCRIBE DETAIL `scratch`.`factory_probe`.`input_table`").first().asDict()
  version = spark.sql("DESCRIBE HISTORY `scratch`.`factory_probe`.`input_table` LIMIT 1").first().asDict()["version"]
  assert detail["format"] == "delta"
  assert detail["id"] and isinstance(version, int)
  ```

  The named scratch resources are created by the harness in the user-selected workspace. Read the corresponding UC table identity using the SDK, then repeat after append and drop/recreate. Assert that append advances version and recreation cannot be mistaken for the old table at the same version.
- [x] Probe schema/table/view classification, deployed UC view-definition reads and changes, Volume publish/read integrity, state-table write access, and access to `system.access.table_lineage`. Record empty lineage separately from permission/query failure. Query with date and target filters; absence of records never establishes completeness.
- [x] Run on serverless notebook compute only. Record first/repeated metadata latency, job timing, and available query/compute usage. Label cold/warm and billing evidence gaps explicitly; the representative savings benchmark remains in Task 10. Unsupported metadata access makes the affected workload ineligible; do not introduce a second backend before confirming the supported path.
- [x] Verify the same job execution principal is used by Spark, SDK and dbt. Check the existing dbt/adapter installation in the actual compute environment.
- [x] Commit: `test: verify Databricks reuse metadata primitives`.

**Evidence:** [Serverless feasibility results](../validation/databricks-model-reuse.md). All primitive checks passed; first/repeated observation timings are available, while system query/billing records had not appeared at collection time.

**Exit:** Identity, Delta versions, storage and execution context have real platform evidence. This is the first platform checkpoint and can block rollout without blocking local contract work.

## Task 2: Define minimal configuration, eligibility and provenance

**Files:** Create `reuse.py`, the provenance-capture portion of `reuse_prepare.py`, and `tests/test_reuse.py`; modify `main.py`, `tests/test_app.py`.

- [x] Write failing config/eligibility tests: absent opt-in, malformed metadata, SQL table opt-in, incremental/Python/custom materialization, hooks, unknown context, ambiguous relation and undeclared extra input. Verify opt-out generation is unchanged.
- [x] Define immutable descriptor/context records and `eligibility_reason(model, context)` in `reuse.py`. Return explicit reasons for optimization ineligibility; raise resource-naming `ValueError` for malformed declarations and graph contract errors. Start with table-backed dependencies; unsupported view/UDF closure executes normally.
- [x] Add validated `--reuse-config` and provenance input handling in `main.py`. Reject requesting reuse with native task mode. Do not add SDK/dbt imports to the offline generation path. Compare canonical context with stdlib `json.dumps(..., sort_keys=True, separators=(",", ":"))` and SHA-256; keep source-artifact identity separate from semantic context so unrelated code edits do not invalidate every model.
- [x] Capture manifest/project/package/context provenance together in the build helper. Provide `verify_snapshot` for one-time project/package verification against an immutable execution snapshot. Wire it into preparation in Task 5; model-task deployment/context rechecks belong to Task 6, without rehashing the whole project per model. A known runtime graph/artifact mismatch fails explicitly. Missing trustworthy context yields `CONTEXT_UNKNOWN` and normal execution. Package macro/model source edits remain subject to selective native comparison; runtime engine/driver/library changes invalidate broadly.
- [x] Verify: `hatch run pytest tests/test_reuse.py tests/test_app.py -v`. Commit: `feat: define model reuse policy and provenance`.

**Evidence:** `capture_parse` produced real dbt Core 1.12.5 / dbt Databricks 1.12.7 manifests, including Python/custom/table models, hooks and an installed package. The offline CLI imports neither dbt nor the SDK. Config/provenance flags validate declarations and explicitly retain ordinary execution until runtime integration.

**Exit:** Eligibility and context comparison are testable without Spark, SDK, or dbt installed. No skip decision is emitted yet.

## Task 3: Publish immutable artifacts and make runtime packaging explicit

**Files:** Create `reuse_runtime.py` and `tests/test_reuse_runtime.py`; modify `main.py`, `tests/test_app.py`, `tests/integration/test_distribution.py`. Reuse `file_io.py` without changing it unless publication checks establish a necessary change.

- [x] Write failure-first checks for a wrong digest, partial file, existing different content, missing artifact, and a runtime wheel whose version differs from the descriptor. Extend the installed-wheel test to read packaged notebook resources and import the new pure module.
- [x] Generate content-addressed manifest, provenance and descriptor files locally using `atomic_write_bytes`. Preparation-notebook publication is part of Task 5, alongside its executable implementation; do not ship a placeholder entry point. Dry-run remains write-free.
- [x] Add `publish_artifacts(workspace_client, local_bundle, volume_path)` in `reuse_runtime.py`. Upload without overwriting; if content exists, verify its digest. Return artifact references only after complete upload/verification. Do not rely on a cross-object atomic rename. Verify bytes again before using downloaded comparison state.
- [x] Document installation of the matching wheel in serverless environment dependencies, then artifact publication before bundle deployment. Do not silently replace user environments or move dbt dependencies into the Factory CLI. The reuse path checks its runtime/descriptor compatibility before reuse.
- [x] Verify: `hatch run pytest tests/test_app.py tests/test_reuse_runtime.py tests/integration/test_distribution.py -v`. Commit: `feat: publish immutable reuse artifacts`.

**Validation:** The focused CLI/storage/distribution run passes 90 tests, including direct-wheel and sdist-derived-wheel execution without importing dbt or the SDK offline. The full suite passes 541 tests; `make lint` passes. Runtime reuse remains disabled.

**Exit:** Jobs and successful-build records can refer only to published, digest-verified artifacts. Publication failure leaves no new reusable evidence.

## Task 4: Implement concrete Delta evidence and relation snapshots

**Files:** Implement `reuse_runtime.py`; test in `tests/test_reuse_runtime.py` and `tests/databricks/reuse_acceptance.py`.

- [x] Write failing tests for changed input/output version, renamed/recreated table, mixed identity observations, non-Delta input, unreadable metadata, duplicate attempt publication and conflicting declared ownership. Include state-write failure after a successful model execution.
- [x] Implement `initialize_state`, `read_build`, `record_build`, `record_decision`, `claim_owner`, and `snapshot_relation` against the three fixed Delta tables and UC metadata. Pass the existing SDK/Spark objects; keep a single concrete implementation. Initialization is an explicit setup operation, not table creation on every model task.
- [x] Quote SQL identifier components by doubling embedded backticks and surrounding each component with backticks. Use SQL parameter bindings for data values; do not concatenate model names, paths or JSON values into SQL literals. Preserve case/quoted identifier semantics in relation mapping.
- [x] Make attempt publication idempotent and verify an existing attempt's evidence matches. Never overwrite a different attempt or another environment's state. Persist the original build time separately from later reuse decisions. Let Delta write conflicts/unavailable state lose optimization with a visible reason.
- [x] Reuse eligibility requires both successful model execution and stable input observations before/after it. Record the pre-build input versions as consumed evidence; do not substitute newer post-build observations. Missing output or unstable identity withholds reusable evidence.
- [x] Verify: `hatch run pytest tests/test_reuse_runtime.py -v`, then the corresponding scratch Delta scenarios from Task 1. Commit: `feat: persist Databricks model build evidence`.

**Validation:** 574 unit/integration tests and `make lint` pass. The final matching wheel passes the real serverless state probe; job and marked scratch schema are removed. [Recorded evidence](../validation/databricks-model-reuse.md#verified-behavior) covers immutable attempts, owner conflicts, input/output mutations and state outages. Runtime preparation/skipping remain disabled.

**Exit:** A malformed, partial, stale or unverified record cannot authorize reuse. State-write failure cannot change successful dbt execution into failure.

## Task 5: Add native selective comparison and run-scoped preparation

**Files:** Extend `reuse_prepare.py` and `tests/test_reuse.py`; create `notebook/prepare_dbt_reuse.py`, `tests/integration/test_dbt_state_comparison.py`.

- [x] Write real-project baseline tests for model SQL, transitive macro, unused macro, unrelated model, dependency mapping, reuse metadata, engine version, declared vars/environment and partial previous failures. Produce baseline/current manifests with the tested engine. Keep them in different directories.
- [x] Invoke the installed engine with an argument list and separate current output directories:

  ```text
  dbt ls --select state:modified --resource-type model --state BASELINE_DIRECTORY --output json --output-keys unique_id
  ```

  Add the validated project/profile/target settings. Parse only node JSON records; retain command diagnostics. A selector/configuration/graph error fails; an unavailable optimization artifact or unsupported comparison produces a visible normal-execution decision.
- [x] Group models by their own successful-build manifest and semantic context. Execute one native comparison per distinct valid baseline group, not per model or only against the previous deployment. An engine/driver upgrade invalidates broadly without cross-engine comparison. Keep model-level code changes selective.
- [x] Compare Factory policy, relation mapping and execution context separately from native state results. Track dbt-managed view/ephemeral definitions and walk their declared dependency graph to physical Delta leaves; intersect native changed-node results with that definition closure. For views, also snapshot the deployed UC definition using the metadata access verified in Task 1. Add real changed-view/ephemeral fixtures with unchanged physical input versions and assert that consumers rebuild. Unresolved definitions or incomplete physical closures make the consumer ineligible. Never infer SQL dependencies in Python.
- [x] Ship the functional preparation notebook and extend the existing runner-publication helper to publish it as a second immutable resource. Extend installed-wheel checks to read that notebook; preserve dry-run and immutable-file checks.
- [x] Publish immutable preparation output bound to the deployment, job/run/attempt and exact build references. Its task value contains a pointer/digest, not a large model map. Missing/state-unavailable preparation succeeds with reuse disabled; unexpected infrastructure errors retain normal failure behavior.
- [x] Verify: `hatch run pytest tests/integration/test_dbt_state_comparison.py -v` in the existing dbt environment. Assert invocation count equals distinct supported baseline groups. Commit: `feat: prepare selective reuse decisions with native dbt state`.

**Validation:** 17 native comparison checks pass against dbt Core 1.12.5 / dbt Databricks 1.12.7; the full 600-test suite includes isolated wheel/sdist packaging. Preparation/notebook boundary checks cover exact build binding, deployed view changes, state/artifact outages and unexpected failures. Artifact downloads are shared only within an attempt. [Recorded evidence](../validation/databricks-model-reuse.md#verified-behavior) distinguishes local validation from platform acceptance. Runtime skipping remains disabled.

**Exit:** The agreed selective-invalidation USP is present before enabling skipping. Context changes cannot hide behind unchanged SQL-only state results.

## Task 6: Wire preparation, additional inputs and force controls into Lakeflow

**Files:** Modify `dbt_factory.py`, `dbt_task.py`, `job_spec.py`, `main.py`, reuse contracts/runtime and notebooks; reuse the existing `job_partition.py` output without changing its partition rules. Extend unit/integration tests.

- [x] Write failing graph checks for zero opted-in models, one preparation per eligible job, task-key collision, a declared extra input produced in the graph, a dependency cycle, conflicting ownership, split children, and 999/1,000 existing tasks when preparation adds one. Include conflicting existing job-parameter names and concurrency settings.
- [x] Bind stable model IDs to the existing task-key maps. Add declared-input producer edges before test gates and partitioning are finalized; rerun existing cycle/gate validation. Add preparation inside each resulting eligible job and count it in the final task limit. Keep cross-partition ordering in the existing parent graph.
- [x] Give eligible model tasks the preparation dependency under normal success gating. Never use `ALL_DONE`. Preparation does not depend on model tasks; live input checks happen later inside each model task. Keep test tasks and their downstream gate edges.
- [x] Add descriptor/model/preparation pointer parameters only to relevant notebook tasks, retaining the existing serialized parameter-size limit and dynamic-reference validation. Use a collision-safe preparation task key drawn from the same allocation rules.
- [x] Add an explicit force-run job parameter, `factory_force_rebuild`, defaulting to `false`. Propagate it through generated parent/child jobs and validate its value strictly at the runner. Force/full-refresh executes normally; fresh build-evidence publication follows in Task 7's runner implementation. Validate owning jobs and orchestrator use `max_concurrent_runs: 1`; refuse conflicting explicit concurrency settings.
- [x] Verify: `hatch run pytest tests/test_dbt_factory.py tests/test_dbt_task.py tests/test_job_partition.py tests/test_job_spec.py tests/test_app.py tests/integration/test_bundle_schema.py tests/integration/test_split_by_tag_against_dbt.py -v`. Regenerate changed golden files through the CLI, never by hand. Commit: `feat: integrate model reuse preparation with Lakeflow jobs`.

**Validation:** All 644 tests and `make lint` pass, including native dbt selection/state checks, bundle schemas and isolated wheel/sdist builds. Preparation is scoped before ownership claims. Real parsed fixtures cover extra-input gates, cycles, task-key collisions and 999/1,000-task boundaries; dbt itself rejects conflicting output aliases during capture. Existing golden specs are unchanged. Reuse notebooks remain outside the immutable project, with regressions for relative/absolute placement and missing-context output. [Recorded evidence](../validation/databricks-model-reuse.md#verified-behavior). Runtime skipping and fresh build publication remain Task 7; no live deployment or savings claim is made here.

**Exit:** Existing test gates, partition semantics, bundle schema, repair structure and task limits hold with preparation enabled. Opt-out jobs preserve existing behavior.

## Task 7: Enable the reuse check before dbt initialization

**Files:** Modify `reuse.py`, `reuse_runtime.py`, `reuse_prepare.py`, `dbt_factory.py`, `notebook/run_dbt_command.py` and `tests/integration/test_reuse_jobs.py`; create `tests/integration/test_notebook_reuse.py`.

- [x] Specify the pure `reuse_reason` contract `(build, current, code_unchanged, context_unchanged, forced) -> reason`. A complete matching evidence record returns `REUSED`; missing state returns `NO_BASELINE`; changed context/code/input/output or force returns a normal-execution reason. Evidence records include explicit identities, versions and completeness, rather than relying on truthiness of partial dictionaries.
- [x] Write and run the failing pure-policy checks before implementing that function:

  ```python
  assert reuse_reason(None, current, True, True, False) == "NO_BASELINE"
  assert reuse_reason(build, current, True, True, True) == "FORCED"
  assert reuse_reason(build, current, False, True, False) == "CODE_CHANGED"
  assert reuse_reason(build, current, True, False, False) == "CONTEXT_CHANGED"
  assert reuse_reason(build, current, True, True, False) == "REUSED"
  ```

  Construct `build` and `current` as complete matching input/output identity/version records using Task 4's contract; one-field input/output identity/version mutations must change the decision. Implement only the branches covered by these evidence scenarios and the specified fallbacks.
- [x] Write the central failing check by executing the shipped notebook with matching evidence and an import guard:

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
- [x] Move the runner's dbt import/injection/initialization after reuse evaluation. Preserve project resolution, authentication and per-task artifact isolation. Keep the SDK/Spark metadata path independent of dbt. On reuse, finish the model task successfully; do not use a skipped status or fabricate `run_results.json`.
- [x] On a miss, snapshot inputs immediately before dbt invocation and again after successful execution, then snapshot output. Publish eligible evidence only for stable inputs and matching deployment/context. An input changing during the build records `INPUT_CHANGED_DURING_BUILD`, finishes normal execution, and forces the next invocation to rebuild.
- [x] Publish build evidence before subsequent tests determine their own result; tests remain separate tasks. Failed/cancelled model invocations publish no success. Retain existing exception reporting and credential/temp-directory cleanup on all paths.
- [x] Verify: `hatch run pytest tests/integration/test_notebook_reuse.py -v`. Commit: `feat: reuse unchanged models without initializing dbt`.

**Validation:** All **696 tests** and `make lint` pass, including isolated packaging and installed-engine selection/state comparisons.

**Runtime checkpoint:** The shipped notebook can avoid model execution without importing dbt under controlled SDK/Spark evidence. Fresh native parsing binds successful model evidence to the captured graph; captured vars reach the actual invocation. Only the exact generated model command with optional full-refresh is eligible; other options execute without publishing reusable evidence. Reuse decisions do not write builds, and tests remain separate tasks.

**Release gate:** `reuse_runtime.REUSE_ENABLED` remains `False` until Tasks 8–9 establish lineage and repair/outage correctness. The import-guard test enables this internal gate only within the test. No live reuse deployment, overhead measurement or savings claim is made here. See [runtime evidence](../validation/databricks-model-reuse.md#verified-behavior).

**Exit:** A controlled second unchanged invocation avoids model execution without importing dbt. Model successes are retained independently of subsequent test results; production skipping remains gated.

## Task 8: Add lineage-based ineligibility without changing the dbt graph

**Files:** Extend `reuse.py`, `reuse_runtime.py`, `tests/test_reuse_runtime.py`, `tests/integration/test_notebook_reuse.py` and `tests/databricks/state_probe.py`.

- [x] Write failing cases for an observed undeclared input, a declared extra input, an old edge needing review, empty lineage, and unavailable lineage metadata. Verify that no discovered edge is silently added to scheduling.
- [x] Read filtered UC lineage during preparation and compare known inputs with the declared dependency closure. Persist blocking observations in decision evidence; a known blocker cannot disappear merely because the system-table retention window advanced. Clear it when a reviewed deployment explicitly declares the dependency. A historical edge is reviewed before it is declared; v1 adds no automatic expiry or ignore-list bypass.
- [x] Unavailable optimization metadata disables reuse for affected models and reports the reason. Empty lineage does not establish completeness; model opt-in/declarations remain required. If a declared extra input is produced by this job, Task 6's explicit edge is mandatory.
- [x] Verify: `hatch run pytest tests/test_reuse_runtime.py -v` and the live hidden-input scenario. Commit: `feat: disable model reuse for undeclared lineage inputs`.

**Validation:** All **717 tests** and `make lint` pass. Serverless run `907310529658101` succeeded with the matching wheel; the harness removed its job and marked scratch schema. The live probe checks existing UC hidden-input observations and real Delta persistence, with the empty-event boundary controlled. Fresh-build lineage ingestion and complete Lakeflow skip acceptance remain in Task 10. See [lineage evidence](../validation/databricks-model-reuse.md#verified-behavior). Production reuse remains disabled.

**Exit:** Additional lineage makes reuse more conservative and explainable, without becoming an alternate compiler or scheduler.

## Task 9: Prove retries, repairs and optimization outages preserve execution

- [x] Establish immutable profile deployment/version binding between preparation and model tasks. Verify the captured profile before importing dbt, so an out-of-band edit cannot authorize reuse.

**Files:** Extend `tests/integration/test_notebook_reuse.py`, `tests/test_reuse_runtime.py`, `tests/databricks/reuse_acceptance.py`; change only production branches exposed by these checks.

- [x] Add scenario assertions before fixing failures:

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

- [x] Execute full notebook paths for read/write failures and stale pointers. Use real dbt fixture artifacts for graph/state behavior; use controlled SDK/SQL failures only at those external boundaries.
- [x] Run repair with and without rerunning preparation. Recheck current input/output evidence and exact successful-build reference in both cases. A changed baseline can conservatively execute if no matching comparison result exists; an old plan cannot authorize reuse.
- [x] Verify: `make lint`, `make test`, `make integration`. Commit: `test: preserve dbt execution guarantees during reuse failures`.

**Validation:** All **761 tests** and `make lint` pass. Final-wheel serverless run `225044578979337` succeeded with real dbt model/test execution through the shipped runner. The harness removed its job and marked scratch schema. Repaired attempts were simulated within one notebook; actual Lakeflow repair scheduling and downstream task gates remain Task 10. See [runtime evidence](../validation/databricks-model-reuse.md#profile-binding-and-repair). Production reuse remains disabled.

**Exit:** Missing optimization evidence cannot skip work, and reuse never bypasses a required test failure.

## Task 10: Run Databricks acceptance and measure net savings

**Files:** Complete `tests/databricks/reuse_acceptance.py`; record reproducible commands/results in `docs/superpowers/validation/databricks-model-reuse.md`.

- [ ] Deploy only to the explicitly selected scratch profile/resources using the existing generate → publish artifacts → bundle validate → deploy → run workflow. Always pass `--profile` and the intended bundle target.
- [ ] Give the local acceptance driver this required-input interface, also used with `--scenario metadata` for Task 1 and `--scenario benchmark` for performance checks:

  ```sh
  python tests/databricks/reuse_acceptance.py \
    --profile "$FACTORY_PROFILE" \
    --catalog "$FACTORY_CATALOG" --schema "$FACTORY_SCHEMA" --volume "$FACTORY_VOLUME" \
    --bundle-dir "$FACTORY_BUNDLE_DIR" --target "$FACTORY_TARGET" \
    --scenario acceptance
  ```

  The operator supplies these values after choosing scratch resources; the bundle defines the chosen compute. The driver generates the probe notebook/job artifacts, invokes the documented bundle workflow with an explicit profile, checks task-level result states, and cleans up only resources it created. Do not delete user input tables or a pre-existing bundle during cleanup.
- [ ] Assert real Lakeflow outcomes for an initial build, unchanged rerun, one input append, one output mutation, drop/recreate, a maintenance commit advancing Delta version, unrelated SQL edit, transitive macro edit, var/environment change, failed test + repair, interrupted build, state outage, undeclared lineage input and split jobs. Maintenance version advances invalidate conservatively. Record model execution count and test execution count, not only green task status.
- [ ] Confirm reused model tasks succeed, dbt never initializes on those paths, tests rerun, downstream gates hold, and engine/driver upgrade forces a rebuild. Verify the native `dbt_task` option still executes normally without reuse.
- [ ] Benchmark with reuse off/on, cold/warm notebook compute, and cold/warm dbt SQL warehouse. Use short and expensive representative models, changing one input and many inputs. Include multiple distinct successful-build baselines after partial deployments/failures.
- [ ] Report warehouse compute, runner/preparation compute, metadata/state requests and write counts, baseline comparison count, retained test cost and wall time separately. Account for warehouse wake-up, repeated task startup, artifact/content verification and contention from small Delta writes. Use run/query identifiers to attribute work; do not estimate savings from skipped model count alone. Compare full runs with tests enabled in both, so retained test cost is included without being counted twice as new overhead; apply the appropriate SKU rates for cost comparisons.
- [ ] Require positive measured net compute savings for the adopted workload. Publish the break-even workload and retained startup latency. If metadata/state writes dominate, first reduce redundant reads and batch preparation work; consider another implementation language only if profiling specifically identifies Python CPU cost.
- [ ] Commit: `test: validate model reuse on Databricks and measure overhead`.

**Exit:** The existing dbt v1 setup passes platform checks and the target workload saves compute after all checking/recording costs. Local tests alone cannot satisfy this gate.

## Task 11: Document supported operation and release the opt-in feature

**Files:** Modify `README.md`, `CONTRIBUTING.md` and the acceptance report.

- [ ] Document model metadata/config, provenance capture, wheel/driver setup, explicit state initialization, grants, artifact publication, one-owner/non-overlapping-run contract and decision queries. Include the exact tested engine/runtime versions and serverless compute configuration.
- [ ] Document normal-execution fallbacks and configuration errors separately. Explain that lineage absence is not proof, table maintenance may invalidate reuse, and the feature does not pin source snapshots or prevent external writers.
- [ ] Provide a rollback procedure: disable the global reuse config or model opt-in, regenerate/redeploy normal tasks, and retain referenced artifacts/evidence. No artifact garbage collector is required for v1. Ownership reassignment must disable/drain the old job before registering the new owner.
- [ ] Run `make lint`, `make test`, `make integration`, installed-wheel verification and bundle-schema validation. Regenerate any affected golden outputs with the real CLI and inspect the diff.
- [ ] Commit: `docs: document Databricks model reuse deployment and operation`.

**Exit:** Reuse is explicitly opt-in, supported versions/limits are stated, and the normal execution path is the documented rollback.

## Delivery sequence and review checkpoints

1. **Foundations:** Tasks 1–3 establish platform evidence and artifact contracts without skipping models.
2. **Complete local path:** Tasks 4–7 implement selective code comparison plus live data checks. Keep the feature unadvertised/disabled until lineage and failure checks pass.
3. **Correctness:** Tasks 8–9 complete required lineage/fallback/repair behavior.
4. **Release:** Tasks 10–11 establish platform acceptance and savings, then document the opt-in feature.

The critical path is correctness and platform validation, not choosing a faster local language. Do not replace selective invalidation with a project-wide checksum to shorten delivery. Do not add incremental-model reuse, native-task skip gates, development clones, automatic lineage adoption, distributed locks, freshness windows, column/row change inference or a UI to this plan.

## Plan self-review

- Every in-scope design requirement maps to a task: opt-in/context/eligibility (2), immutable artifacts (3), identities/ownership/evidence (1, 4), per-model baselines and native selective invalidation (5), extra edges/splits/task limits/force (6), no-dbt reuse and stable build observations (7), lineage (8), tests/fallbacks/repair (9), and measured acceptance (10–11).
- Whole-project content verification detects deployment/runtime drift; semantic context and native state determine reuse across valid deployments. These are different comparisons.
- Successful model builds and data-test outcomes remain separate. Every reused task succeeds normally and preserves its downstream gates.
- Platform operations require an explicitly selected profile. No resources have been selected or deployed by writing this plan.
- The first-release language choice is Python; Rust/Scala remain profiling-driven alternatives, not implementation work.
