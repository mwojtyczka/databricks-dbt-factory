# Serverless model-reuse validation

Production `reuse_runtime.REUSE_ENABLED` is **False**. Complete generated-job acceptance,
representative workloads and positive measured net compute savings are required before release.
The 813-test local suite and static checks pass; local checks do not satisfy the platform gate.

## Tested environment

Live probes use the explicitly selected profile `fevm-test-jg`, catalog `test_jg_catalog`,
serverless notebooks and serverless SQL warehouse `7b8cc32a9eba1da3` (2X-Small).
The recorded runtime is Python 3.12.3, Spark 4.2.0, serverless environment version 4,
dbt Core 1.12.5, dbt Databricks 1.12.7 and Factory 0.4.0.
Separate dbt 2.0 compatibility work is deferred.

## Verified behavior

| Evidence | Verified scope |
| --- | --- |
| [Metadata primitives](databricks-model-reuse.json) | UC/Delta identities and versions, replacement detection, bracketed observations, view definitions, parameterized state writes, Volume integrity and execution identity |
| [State storage](databricks-model-reuse-state.json) | Immutable/idempotent attempts, conflicting owners/content, exact artifact references, mutations and unavailable state |
| [Runtime runner](databricks-model-reuse-runtime.json) | Real dbt models/tests through the shipped wheel, reuse without dbt import, forced execution and state-write fallback |
| [Lineage veto](databricks-model-reuse-lineage.json) | Persistent undeclared-input blockers, scope binding and normal execution on unavailable lineage evidence |
| [Generated-job discovery](databricks-model-reuse-acceptance-discovery.json) | Two initial model commands and one test, then zero model commands and one required test; both reused tasks avoided dbt import |
| [Paired benchmark](databricks-model-reuse-benchmark.json) | All 20 measured stages, exact invocation counts, four exclusive warehouse cohorts and owned-resource cleanup |

Native comparison tests use fresh dbt-generated artifacts and `dbt ls`, covering selective SQL,
macro, view/ephemeral, dependency, policy and context invalidation. Each model uses its own
successful baseline; preparation shares artifact downloads only within that attempt.
Generated-graph tests cover extra-input edges, required gates, split-job scopes, force controls,
reserved parameters and the 1,000-task limit. Native selectors and the captured graph remain intact.

Full notebook tests cover stale/missing preparation, model/build binding, input/output identity
and version changes, malformed evidence, artifact/state outages, unstable builds and credential
cleanup. Successful builds and test outcomes remain separate. Reuse never bypasses required tests.

## Profile binding and repair

Provenance binds the validated profile template, routing values, immutable deployment, runtime
versions and declared context. Basic Databricks PAT profiles support literal routing fields or
single declared `env_var` references. Private profile copies freeze routing values while leaving
credential references unresolved. Credentials are never hashed or persisted.

The runtime reuse path requires `DBT_ACCESS_TOKEN` and the authenticated SDK workspace host.
Other token references execute normally without reusable evidence. Profile edits fail before
skipping or publishing evidence. Declared non-secret vars reach model and test tasks even when
optimization artifacts are unavailable.

The runtime probe simulated repaired attempts inside one notebook: four real model commands,
three no-dbt reuse attempts and four real test commands including two expected failures.
Those checks do not establish actual Lakeflow repair scheduling. The generated-job suite has
completed 12 stages covering data/code/context invalidation and mixed successful baselines.
Its deliberate failed test ran twice under Lakeflow retry and blocked the downstream report.
The harness counts every observed test attempt and rejects duplicate metrics.
Complete actual repair/interruption, state outage, engine-change, fresh-lineage, split-job and
native-task acceptance remains required.

## Measured overhead and limits

| Warm unchanged workload | Reuse off | Reuse on |
| --- | ---: | ---: |
| Short fixture | 103.4 s | 239.0 s |
| Hash aggregate over 100 million rows | 104.7 s | 150.2 s |

These are actual Lakeflow durations for single observations, including required tests.
The aggregate's ordinary model CREATE calls took only **6.3 s**: it remains a seconds-long
transformation. These fixtures measure checking/startup overhead, not economics for substantial
dbt workloads. Representative minutes-long models and repeated off/on runs must establish break-even.
Warm reuse runner Spark calls occupied 81.5 s and 20.7 s respectively; these exclude preparation
and are not total billed compute. Primitive relation snapshots took 4.21 s initially and
1.88–1.97 s on repeats. These are observations, not production latency guarantees.

Each benchmark arm owns an identical serverless warehouse and records its creation/stop window.
Controls and telemetry use the supplied control warehouse. Cold/warm runs, one/multiple input
changes, partial deployments and mixed baselines retain tests in both arms. Collection separates
warehouse usage, actual job/query evidence and provisional effective list-price billing.
Owned jobs, schemas and warehouses are cleaned up; the supplied warehouse is retained.

Billing queries returned no rows, including a subsequent collection. No DBU/currency savings
are established. Individual runs within a warehouse arm remain unallocated; notebook coldness,
complete Spark query-history coverage and final billing corrections are not established.
Cursor identifier coverage is reported separately from overall completeness.

The probes establish availability for the tested user, not deployment service-principal grants.
They do not establish concurrent snapshot isolation, prevent external writes or prove lineage
completeness. Missing lineage is never proof of complete dependencies. Runtime identity/version
checks and the one-owner/non-overlapping-run contract remain required.

## Reproduce

Build a matching wheel, then use fresh schema and local directory names:

```sh
PYENV_VERSION=3.10.20 hatch build -t wheel
.venv/bin/python tests/databricks/reuse_acceptance.py \
  --profile fevm-test-jg --catalog test_jg_catalog \
  --schema factory_acceptance_jg_unique --volume artifacts \
  --warehouse-id 7b8cc32a9eba1da3 \
  --bundle-dir /tmp/factory-acceptance-unique --target probe \
  --scenario acceptance \
  --factory-wheel dist/databricks_dbt_factory-0.4.0-py3-none-any.whl
```

Use `--scenario benchmark` with different fresh names for paired measurements; `--rows` sets
aggregate work. Metadata/state/runtime probes use `--scenario metadata|state|runtime` and
`--directory /tmp/factory-probe-unique` without schema/volume/target flags. State/runtime probes
require the matching wheel. All live work must explicitly select a profile and use serverless.
Neither probe completion nor the benchmark enables production reuse.
