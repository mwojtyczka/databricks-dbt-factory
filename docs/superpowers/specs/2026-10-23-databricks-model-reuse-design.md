# Databricks model reuse for dbt Factory

Status: Design for review; no implementation changes.

## Purpose and fit

Add optional production model reuse to Factory. Databricks owns the state and
decisions; dbt retains compilation, selection, materializations, and testing;
Lakeflow Jobs retains scheduling, task dependencies, retries, and repair.

Factory already generates individual model and test tasks, supports a notebook
runner, and partitions large projects into child jobs. Reuse belongs around that
runner. It does not require a dbt fork, custom SQL engine, or persistent service.

The value is selective code invalidation combined with live Databricks data
state, inspectable evidence in Unity Catalog, and existing Lakeflow execution.
Selective code comparison alone is already available in dbt.

## Agreed scope

- Production warehouse-compute savings take priority over unchanged-run latency.
  Reused tasks still start and incur Lakeflow and state-check overhead.
- SQL models using table materialization explicitly opt in through model metadata.
  Model authors declare that results depend only on tracked code, context, and
  inputs. Time/random-dependent behavior, hidden reads, external side effects,
  custom materializations, and hooks that must execute make a model ineligible.
- Complete data dependencies must resolve to Unity Catalog Delta tables. Views
  and ephemeral models require tracked definitions and a complete dependency
  closure; otherwise their consumers execute normally. Untracked UDF definitions
  and other opaque dependencies also prevent reuse.
- Any tracked input change forces execution. V1 has no lag-tolerance policy.
- Ordinary model and macro changes invalidate selectively. Engine/adapter upgrades
  and context changes without reliable model-level attribution invalidate broadly.
- Reuse applies to the notebook runner. Native `dbt_task` execution remains
  available without reuse. Seeds, snapshots, incremental models, Python models,
  and tests execute normally.
- Each output table has one owning Factory job with non-overlapping runs. There
  is no distributed locking or coordination between independent writers.
- Tests rerun even when a model is reused. Build evidence and test outcomes are
  separate; required tests retain their downstream gates.

## Architecture

### Generation and immutable artifacts

Factory generates the existing model/test graph, model descriptors, and one
preparation task per generated job that contains reuse-enabled models. In split
mode, preparation belongs inside each eligible child job; the existing parent
orchestrator continues to order child jobs. Every resulting job must remain
within the platform task limit, counting the preparation task.

Descriptors bind stable dbt `unique_id` values to expected output relations,
declared input relations, reuse policy, and immutable deployment artifacts.
State uses logical project/environment/model identity rather than Lakeflow task
keys, which can change when names collide.

Retain immutable manifests in a UC Volume. Publish them before referencing them
from a job or successful-build record. Do not delete artifacts still referenced
by build evidence. V1 can retain all artifacts without a cleanup service.

The build process must supply execution-context provenance alongside the
manifest. A manifest alone cannot establish the target, vars, environment, and
package context that produced it. Record runtime versions, effective target and
relation mapping, declared context inputs, and execution identity relevant to
results. Credentials and raw secrets do not belong in stored evidence.

Generation preserves Factory's exact selectors and test gates. Declared extra
inputs that are produced by another task require the corresponding dependency
edge. A cycle or an unprovable graph contract is a configuration error, not a
reason to silently drop a dependency.

### Preparation: selective code comparison

Use dbt's public state comparison, including `dbt ls --select state:modified`,
against immutable manifests associated with each model's successful build.
Group comparisons by baseline manifest and context; do not invoke dbt separately
for every model. Keep current output artifacts separate from comparison state.

The previous deployment is not a sufficient baseline: a deployment may exclude
models or fail after only some models finish. Each model's successful-build
record references the artifact that actually built that output.

Native comparison supplies code-change evidence, not the complete reuse verdict.
Factory also checks resolved dependency mappings, its own reuse metadata, and
execution context. Changes to metadata controlling reuse must be checked even
when dbt treats them as metadata-only. Vars/environment changes without proven
model-level attribution invalidate broadly. Missing context provenance prevents
reuse. Unsupported comparisons execute normally.

Definitions of views and ephemeral dependencies must participate in invalidation:
they cannot rely on a materialized table-version change to signal changed code.
Opaque or unverifiable definitions make the consumer ineligible.

Preparation produces run-scoped code decisions bound to the current artifact
and baseline records. It does not freeze the input versions for the whole run.
Expected state-access failures produce a successful preparation result with
reuse disabled and a visible fallback reason. Do not use `ALL_DONE` on model
tasks to bypass a failed preparation task: that could also bypass failed data
dependencies or test gates. Unexpected task/infrastructure failures retain normal
Lakeflow failure behavior.

### Runner: authoritative data checks

After its upstream tasks and required tests succeed, each model runner:

1. Checks opt-in policy, force-execution options, current context, code evidence,
   and the latest successful-build record.
2. Reads current input/output identities and Delta versions using the job's
   execution identity. Missing output, changed identity/version, absent evidence,
   or an unverifiable dependency prevents reuse.
3. Reuses only when all required evidence matches. Record `REUSED` and finish the
   Lakeflow task successfully without importing or initializing dbt. Do not emit
   a fictitious dbt success artifact or use a skipped task status that changes
   downstream gating.
4. Otherwise captures input identities/versions immediately before execution,
   invokes the existing dbt command, and captures inputs and output again after
   successful execution.
5. Publishes reusable build evidence only when all declared inputs remained
   stable across those observations, the deployment/context still matches, and
   the output can be identified reliably. If an input changed, record
   `INPUT_CHANGED_DURING_BUILD` and withhold eligibility. Tests continue normally;
   the next model invocation rebuilds.

Input versions are checked against the versions recorded before that successful
build, never against newly observed post-build versions assumed to have been
read by dbt. V1 preserves ordinary dbt consistency semantics; it does not rewrite
queries to pin versions or promise a global cross-table snapshot. Dependencies
whose stability cannot be established remain ineligible.

Delta version advances are conservative invalidations, including maintenance
commits. V1 does not classify commits to prove that logical query results stayed
the same. Skipping more work can follow verified evidence for such a refinement.

Retries and repairs revalidate live state. A cached plan must not authorize reuse
against a different baseline, artifact, input version, or output. Newly successful
builds can supply evidence on repair; failed/cancelled attempts never publish
successful-build evidence. Explicit full-refresh/force execution bypasses reuse.

### Durable state and lineage

Use UC Delta tables for successful-build evidence and decision history. A build
record contains logical model/environment identity, owner, artifact/context
references, declared dependency evidence, output identity/version, and Lakeflow
run/task/attempt identifiers. Record original build time separately from reuse
time. A failed data test does not erase valid build evidence.

Bind state to the correct workspace/metastore and environment; never reuse
another environment's evidence implicitly. Validate declared ownership and
non-overlapping job configuration. Per-job concurrency settings do not enforce
ownership across jobs. External output mutations invalidate reuse through live
identity/version checks; they are not prevented by this feature.

UC lineage is diagnostic evidence. A known input absent from the declared graph
disables reuse until declared through `ref()`/`source()` or Factory metadata.
Historical edges need review; they are not adopted automatically. Absence of a
lineage record never proves dependency completeness. Model authors remain
responsible for the opt-in contract because lineage capture is incomplete.

## Failure behavior and visibility

Missing/unavailable state, unreadable comparison artifacts, or unavailable
optimization metadata cause normal execution with an explanation such as
`NO_BASELINE`, `CONTEXT_UNKNOWN`, or `STATE_UNAVAILABLE`. Code/data changes report
`CODE_CHANGED`, `CONTEXT_CHANGED`, `INPUT_CHANGED`, or `OUTPUT_CHANGED`.

State-write failure after a successful build leaves that build without new reuse
evidence; it does not turn successful dbt execution into a failure. State outages
can also prevent durable decision logging, so the task output/log must report the
fallback. Do not claim a complete audit trail during an outage.

The optimization cannot relax existing execution guarantees. Invalid selectors,
dependency cycles, conflicting ownership, and a detected runtime graph/artifact
mismatch fail explicitly. Ordinary dbt and required-test failures retain their
normal Lakeflow outcomes.

## Verification and performance acceptance

Local feasibility checks using dbt-core 1.12.5 and dbt-databricks 1.12.7 show:

| Fixture change | Observed native comparison |
| --- | --- |
| Transitive macro body changes | Dependent model selected |
| Add downstream expansion (`state:modified+`) | Dependent model and its downstream selected |
| Unused macro changes | No models selected |
| Unrelated model SQL changes | Only that model selected |
| SQL-only var/environment value changes | No models selected in these fixtures |

These are local parse/selection checks, not warehouse or reuse-engine validation.

Implementation must retain runnable tests for selective invalidation, broad
context invalidation, changed dependency mappings, per-model baselines after
partial failure, and unchanged outputs across deployments. Test real dbt-produced
manifests and feed emitted selectors back through current dbt as required by
`AGENTS.md`; do not substitute hand-written manifest assumptions.

Databricks acceptance checks cover unchanged reuse without dbt initialization,
input/output changes and recreation, changes during execution, forced runs,
retry/repair, split jobs, required test gates, undeclared lineage inputs, and
state read/write failures. View/ephemeral support stays disabled wherever its
complete definition/dependency contract cannot pass those checks.

Benchmark unchanged and changed runs on cold and warm compute. Report warehouse
compute, runner compute, state/metadata overhead, and elapsed time separately.
Include task startup, metadata-induced warehouse wake-up, retained test cost,
number of distinct baseline comparisons, and small Delta-write overhead.
Retained startup can dominate short models; no latency or savings percentage is
promised without representative measurements. Adoption requires positive net
compute savings for the opted-in workload, including checking/recording costs.

## Deferred scope

Development cloning/deferral automation, freshness windows, native `dbt_task`
reuse gates, distributed locks, automatic lineage adoption, non-Delta sources,
row/column-level change inference, and a separate UI/service are outside v1.

## References

- [dbt State GA](https://www.getdbt.com/blog/dbt-state-is-ga)
- [dbt state selector methods](https://docs.getdbt.com/reference/node-selection/methods)
- [dbt state comparison caveats](https://docs.getdbt.com/reference/node-selection/state-comparison-caveats)
- [UC lineage system tables](https://docs.databricks.com/aws/en/admin/system-tables/lineage)
- [Databricks table history](https://docs.databricks.com/aws/en/delta/history)
- [Existing Factory runner](../../../src/databricks_dbt_factory/notebook/run_dbt_command.py)
- [Factory graph and context contract](../../../README.md#manifest-and-runtime-graph-contract)
