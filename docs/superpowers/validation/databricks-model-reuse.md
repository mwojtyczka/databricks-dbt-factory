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

The preparation API and content-addressed notebook are shipped. Generated Lakeflow graphs still
contain ordinary dbt tasks; preparation wiring, live runner reuse and lineage enforcement remain
pending. This checkpoint has no new Databricks run and establishes no platform acceptance or
net savings. The existing serverless profile/catalog constraints remain in effect for live work.
