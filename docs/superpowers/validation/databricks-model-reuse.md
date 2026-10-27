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
contracts. The state store and skip decisions are the next implementation work.
