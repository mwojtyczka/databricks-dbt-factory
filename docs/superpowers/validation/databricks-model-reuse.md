# Serverless model-reuse feasibility

The Task 1 platform primitives passed on 2026-10-27 using profile `fevm-test-jg`
and catalog `test_jg_catalog`. This validates the storage and observation path;
the production reuse feature and its savings acceptance are not implemented.

Job `521647302814502`, run `529906380753320`, task run `233338761216485`
finished with `SUCCESS`. The harness then destroyed the bundle job and removed
its ownership-marked schema `factory_reuse_probe_8c2da23ac12e`, including its
managed Volume. The existing serverless SQL warehouse `7b8cc32a9eba1da3`
(2X-Small) was left available. No classic compute was provisioned.

[Structured evidence](databricks-model-reuse.json) retains the probe results,
identities, timings, and query references. The serverless notebook used
environment version 4, Python 3.12.3, Spark 4.2.0, dbt Core 1.12.5,
dbt Databricks 1.12.7, and Databricks SDK 0.117.0.

| Check | Observed result |
| --- | --- |
| Delta identity/version | Append advanced version 0 to 1 with stable UC/Delta IDs |
| Drop/recreate | New UC and Delta IDs at version 0; version alone would miss replacement |
| Observation consistency | UC identity and Delta identity matched before/after version reads |
| Classification/definitions | SDK distinguished managed table and view; altered view definition changed while input identity/version stayed stable |
| State writes | Two bound-parameter MERGEs for one attempt produced exactly one row; quoted evidence round-tripped |
| Volume artifacts | Uploaded/downloaded SHA-256 matched; duplicate upload without overwrite failed and preserved original bytes |
| Execution identity | Spark, SDK, and the dbt-created table all reported `johannes.gunther@databricks.com` |
| dbt execution | A real SQL table model built successfully on the serverless warehouse |
| Lineage | Date/target-filtered query succeeded with zero rows; completeness remains unproven |

## Measured overhead

One relation snapshot performs two SDK table reads, two `DESCRIBE DETAIL`
commands bracketing one `DESCRIBE HISTORY ... LIMIT 1` command. The first
snapshot took **4.21 s**; repeated snapshots took **1.97, 1.88, and 1.93 s**.
These are sequential per-relation observations after table creation, not a
guaranteed cold/warm compute comparison or a production p95.

The two state MERGEs plus table creation/readback took 6.65 s. Volume integrity
and duplicate-write checks took 1.24 s. The dbt subprocess, connection, tiny
table build, and principal verification took 24.43 s. Jobs reported 4 s setup
and 114 s execution; overall run duration was 118.69 s. These include deliberate
mutation tests and environment/import overhead, not an unchanged-model reuse path.

Both the exact dbt statement lookup in `system.query.history` and job/run-filtered
`system.billing.usage` query succeeded but returned no records at collection time.
Recorded query IDs allow later attribution; no DBU/currency amount or net saving
is established. Billing attribution for the shared SQL warehouse is also separate
from notebook job usage. Task 10 still requires representative cold/warm runs,
retained test costs, and complete billing/query measurements.

The observed seconds per relation justify measuring the complete reuse path
before enabling it for short models. There is no evidence here to justify Rust,
a separate metadata warehouse, or a second backend.

## Verification and limits

The bundle passed `databricks bundle validate --strict`. Offline tests validate
the bundle against the installed CLI schema, require a serverless environment
with no cluster settings, and reject an existing local output directory.
All 495 unit/integration tests passed across the full run and targeted reruns
under the required Hatch environment. `make lint` passed. The distribution check
required access outside the sandbox to create Hatch's build environment/cache.

The probe establishes availability under this user's identity in this workspace.
It does not establish permissions for a deployment service principal, concurrent
snapshot isolation, race-free external writes, complete view dependency closure,
or complete/eventually available lineage. Those remain implementation/acceptance
contracts. Runtime preparation and skip decisions remain pending.

## Offline policy and provenance checkpoint

Task 2 adds immutable configuration/context/model records, conservative eligibility,
declared-input resolution, cycle checks, and fresh-parse provenance. Tests use real
dbt Core 1.12.5 / dbt Databricks 1.12.7 manifests, including an installed package,
Python/custom materializations and project hooks. They verify manifest/snapshot
binding, context/artifact separation, the captured environment used by dbt, normal
execution when provenance is unavailable, and rejection of known mismatches.

All 523 unit/integration tests passed across the full run and the sandbox-exempt
Hatch packaging rerun; `make lint` passed. The generator imports neither dbt nor
the SDK. Runtime preparation and model-task integration are still pending, so
the new CLI flags validate declarations and emit ordinary execution tasks.

## Delta state storage checkpoint

The shipped Factory wheel passed the serverless state probe on 2026-10-27 using
`fevm-test-jg` and `test_jg_catalog`. Job `783766706068862`, run `762048026668262`,
task run `754706383505784` finished with `SUCCESS`. The harness removed the job
and its marked schema `factory_reuse_probe_8b4fa209e722`. It provisioned no classic
compute and left the existing serverless SQL warehouse unchanged.

[State evidence](databricks-model-reuse-state.json) records the exact wheel digest,
baseline key/digest, checks and cleanup. The notebook used serverless environment
4, Python 3.12.3, Spark 4.2.0, Factory 0.4.0, dbt Core 1.12.5 and dbt Databricks
1.12.7. A real dbt parse supplied model/source mappings; Spark SQL built the
fixture tables. This validates storage APIs rather than dbt runner integration.

The probe verified explicit/idempotent table setup, immutable attempt publication,
rejection of conflicting attempt content and owners, separate decision history,
original completion-time retention, and digest-verified artifact references.
Changed inputs withheld evidence; changed/recreated outputs could not retrieve
the old baseline. Renaming an output and using a view as a Delta input returned
`METADATA_UNAVAILABLE`. Temporarily making the builds table unavailable returned
`STATE_UNAVAILABLE` on reads and publication after successful fixture execution.

The checks took **82.71 s** inside the notebook. Jobs reported 4 s setup and 129 s
execution; driver wall time was 142.07 s. These totals include fresh setup, parse,
artifact transport, repeated writes, mutation scenarios and recovery. They do
not measure a per-model reuse check or establish net savings.

All **574 unit/integration tests** and `make lint` pass, including wheel/sdist
packaging. Controlled failure checks additionally reject malformed/partial build
records, incorrect attempt keys, missing artifact bytes, mixed identity reads,
and unavailable write readbacks. The generator still imports neither dbt nor
the SDK. Runtime skipping remains disabled.

## Native preparation checkpoint

Local validation on 2026-10-27 uses dbt Core **1.12.5** and dbt Databricks **1.12.7**.
The native comparison suite passes **17 checks** using fresh real-project artifacts and the
installed engine's `dbt ls`. It covers model SQL, transitive/unused macros, unrelated edits,
view/ephemeral definitions, dependency/reuse-policy changes, driver/engine and declared-context
changes, and different per-model successful baselines after partial builds. Invocation counts
match distinct supported baseline groups. Current output and baseline directories are separate;
runtime profile drift fails the fresh-parse binding check.

The full suite passes **600 tests**, including direct and sdist-derived wheel resources and
execution outside the source tree. `make lint` passes. Controlled SDK/Spark boundaries exercise
run/job/attempt and exact-build binding, deployed-view changes without changed output evidence,
state/artifact outages, immutable publication/readback, and notebook credential cleanup with
unexpected failures propagated. The task value contains a pointer/digest rather than model maps.
An additional regression checks identical descriptors across eight process hash seeds, including
mixed dependency fallback reasons; definition traversal uses deterministic ordering.

One preparation attempt shares verified deployment artifacts. The regression fixture requires
three artifact downloads for a deployment shared by preparation and its successful-build baseline,
rather than repeating downloads per model. This establishes request-count behavior, not compute
savings or a measured latency improvement. Native comparisons still require a fresh dbt process
for each distinct supported baseline group; tests still need to execute.

The preparation API and content-addressed notebook are shipped. Live runner reuse and lineage
enforcement remain pending. This checkpoint has no new Databricks run and establishes no platform
acceptance or net savings. The existing serverless profile/catalog constraints remain in effect for live work.

## Lakeflow wiring checkpoint

Local validation on 2026-10-27 passes **644 tests** and `make lint`, using dbt Core **1.12.5** /
dbt Databricks **1.12.7**. This includes installed-engine selection/state checks, single/split-job
bundle schemas and isolated wheel/sdist builds. The generated graphs add one preparation task
per eligible job, scoped to that child's model IDs before ownership claims. Declared extra-input
edges precede test gating and partitioning; native selectors and the captured manifest are unchanged.

Real parsed fixtures cover per-test/bundled gates, dependency cycles, preparation-key collisions,
zero opt-in and final 1,000-task boundaries. dbt refuses conflicting physical output aliases during
capture. Controlled notebook checks cover strict force values before authentication and validated
preparation scopes. Job templates reject parameter overrides and conflicting concurrency; split
parents forward force controls. Missing context retains ordinary output. Snapshot checks prove
relative/absolute notebook placement cannot silently modify the captured deployment.

Model tasks still execute normal dbt. Fresh build publication and reuse before dbt initialization
remain pending. No new Databricks resources were deployed, and no runtime overhead or savings
were measured at this checkpoint.


## Runtime runner checkpoint

All **696 tests** and `make lint` pass, including native selection/state comparisons and isolated
wheel/sdist builds. The runner/policy suite contributes **48 checks**.

The shipped runner evaluates live reuse evidence before importing dbt or creating its private
artifact/log directory. An import guard on the complete notebook proves the unchanged-model
path finishes successfully without importing dbt. It records a separate decision and leaves the
original successful build intact. Test tasks still execute independently in the generated graph.

Misses use a fresh native dbt parse, verify its graph against the captured manifest, and inject
that parsed manifest into the model runner. Captured non-secret vars reach parse and every generated model/test task through task parameters,
including partitions without reusable models. Artifact outages retain those execution vars.
Only the exact generated run/selector/target command, optionally with full-refresh, can publish
reusable evidence; unsupported flags execute normally. Force and full-refresh always execute.

Controlled SDK/Spark boundaries exercise missing/stale/unavailable preparation, per-model baseline
binding, input and output identities/versions, view definitions, changed deployment files and
profiles, malformed preparation, and preparation values that cannot authorize skipping. Successful
model invocations publish only actual model-result artifacts with stable before/after observations.
Failed/cancelled/wrong-model and unstable-input cases retain the earlier build. Credentials and
private directories are cleaned on success and failure. Native parse and injected-manifest selection
are checked with dbt Core **1.12.5** / dbt Databricks **1.12.7**; warehouse model execution is controlled
in these local tests.

Profiles stay outside the content-verified project. Provenance v2 binds the validated profile template
and captured routing values before the runner can skip dbt. The runner consumes a private profile
snapshot on execution; profile edits after preparation cannot authorize reuse.

`reuse_runtime.REUSE_ENABLED` is **False**. Task 10 must establish complete Lakeflow acceptance and positive measured savings before
production skipping is enabled. No cloud resources were deployed for this
checkpoint; complete-path serverless overhead, billing and savings still require Task 10.

## Lineage veto checkpoint

All **717 tests** and `make lint` pass, including isolated wheel/sdist builds and native dbt
Core **1.12.5** / Databricks **1.12.7** fixtures. Serverless job `666011975098521`, run
`907310529658101`, task run `533652377278530` finished with `SUCCESS` using profile
`fevm-test-jg` and catalog `test_jg_catalog`. The harness removed its job and marked schema
`factory_reuse_probe_1ceec374a070`. No classic compute was provisioned.
[Structured lineage evidence](databricks-model-reuse-lineage.json) records the matching wheel
digest, real blocker, storage checks and cleanup. The notebook used environment version 4,
Python **3.12.3** and Spark **4.2.0**.

Reproduce with a freshly built matching wheel and a new local output directory:

```sh
PYENV_VERSION=3.10.20 hatch build -t wheel
.venv/bin/python tests/databricks/reuse_acceptance.py \
  --profile fevm-test-jg --catalog test_jg_catalog \
  --warehouse-id 7b8cc32a9eba1da3 \
  --directory /tmp/factory-lineage-probe-unique \
  --scenario state --factory-wheel dist/databricks_dbt_factory-0.4.0-py3-none-any.whl
```

Preparation queries `system.access.table_lineage` for each opted-in output and declared
view, using a 365-day date filter and separately bound catalog/schema/table components.
Observed inputs outside the declared closure veto reuse without adding scheduling edges.
Complete table components identify registered tables even when `source_path` contains their
backing storage location. Path-only observations block reuse; v1 has no path declaration or
ignore-list. Empty lineage supplies no completeness guarantee.

Immutable blocker markers share the decisions Delta table. They bind to the execution scope
and normalized physical output rather than the model ID or deployment digest, so a model
rename or new deployment cannot discard known observations. Explicit declarations satisfy
retained markers; removing a declaration blocks reuse again. Preparation avoids repeated
MERGEs for markers it has already verified. Model tasks reread retained blockers before
accepting preparation evidence or publishing a successful build, including forced runs.

Controlled SQL/SDK checks cover undeclared/declared inputs, view dependencies, case and dotted
identifiers, multiple retained blockers, renamed models, changed deployment context, path
observations, partial metadata, corrupt marker content and read/write outages. Notebook checks
prove an old preparation pointer cannot authorize skipping after a blocker is recorded and
that normal dbt execution still occurs. Native UC identifier checks distinguish names that
Unicode case folding would incorrectly merge. No lineage edge changes the generated dbt graph.

Production reuse remains disabled. The live lineage probe uses existing system-table observations
because fresh-event ingestion is asynchronous, with writes confined to the harness-owned scratch schema.
Its empty-event check controls only the system-table boundary and verifies persistence against
real Delta storage. It does not establish a complete fresh-build-to-lineage-to-skip Lakeflow path;
Task 10 retains that acceptance scenario and the full overhead/savings measurement.

## Profile binding and repair checkpoint

Provenance v2 includes the digest of a validated, canonical profile template. Capture,
preparation and the model runner verify the same template. A profile edit after preparation,
including a warehouse-only change invisible to dbt's graph, fails before importing dbt.
Prior provenance must be recaptured; there is no compatibility fallback to an unbound profile.

Reuse currently accepts one statically named profile, static target/output names and basic
Databricks PAT fields (`type`, `host`, `http_path`, `catalog`, `schema`, `threads`, `token`).
Literal credentials, default credentials, nested adapter options and arbitrary Jinja are rejected.
Routing `env_var` references must be declared in captured non-secret environment inputs. Their
values must be nonempty literals; nested Jinja cannot introduce another undeclared dependency.
Capture, comparison and execution use private profile copies with those captured routing values,
so subsequent source-file or process-environment edits cannot change the bound invocation.

Token references remain unresolved in private profiles; resolved credentials are never hashed
or persisted as evidence. Capture/comparison can accept versioned `DBT_ENV_SECRET_*` references.
The runtime reuse path requires `DBT_ACCESS_TOKEN` and the same workspace host as the authenticated
SDK client, so the verified execution identity also authenticates dbt. Other token references run
normally without reusable evidence. This restriction applies only to the opt-in reuse path.

Notebook repair tests cover unchanged, changed-input and changed-output attempts with and without
new preparation. An old preparation binds an exact successful-build reference and cannot authorize
reuse after that baseline changes. Controlled state/artifact read and build/decision write failures
preserve normal model execution; failed/cancelled builds and unstable inputs retain the earlier
successful evidence. Private directories and credentials are cleaned on success and failure.

The runtime probe uses the shipped runner and real dbt model/test execution on serverless. It
simulates repaired attempts inside one notebook task. Actual Lakeflow `jobs.repair_run`, downstream
gating across tasks, fresh-lineage ingestion and measured net savings remain Task 10 acceptance
checks. Production `REUSE_ENABLED` remains **False**.

All **761 tests** and `make lint` pass, including native dbt Core **1.12.5** / dbt Databricks
**1.12.7**, isolated package builds and bundle-schema checks. Spec and code-quality reviews pass.
Final-wheel serverless job `661952856474187`, run `225044578979337`, task run `32239315245072`
finished with `SUCCESS` using `fevm-test-jg` and `test_jg_catalog`. Python was **3.12.3**, Spark
**4.2.0**, serverless environment version **4**. The harness removed its job and marked scratch
schema `factory_reuse_probe_409164cd6a6e`; no classic compute was provisioned.
[Structured runtime evidence](databricks-model-reuse-runtime.json) includes the matching wheel
digest and cleanup confirmations.

The probe executed four real model commands: initial build, changed-input rebuild, stale-baseline
rebuild and a successful forced rebuild during a state-write outage. Three unchanged attempts
reused without importing dbt. Four real test commands include two expected failures and their
successful reruns, covering repaired model attempts with and without refreshed preparation.
A profile edit fails before dbt import. These runner invocation counters exclude capture and
preparation subprocesses. The scenario took **323.0 seconds** including task setup; this is a
correctness probe duration, not an overhead or savings benchmark.

Reproduce with the current matching wheel and a fresh output directory:

```sh
PYENV_VERSION=3.10.20 hatch build -t wheel
.venv/bin/python tests/databricks/reuse_acceptance.py \
  --profile fevm-test-jg --catalog test_jg_catalog \
  --warehouse-id 7b8cc32a9eba1da3 \
  --directory /tmp/factory-runtime-probe-unique \
  --scenario runtime --factory-wheel dist/databricks_dbt_factory-0.4.0-py3-none-any.whl
```
