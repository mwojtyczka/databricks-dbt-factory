# AI agent instructions

This project generates dbt commands that run inside Databricks jobs. Almost every bug that
matters is a mismatch between what we *believe* dbt does and what dbt *actually* does, so the
rules below are about closing that gap rather than about style.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the local workflow (`make lint`, `make test`,
`make integration`).

## Verify every claim about dbt against a live, current dbt

**Do not reason about dbt's behaviour from its source code, from memory, or from a helper
function in isolation.** Install a current dbt, build a throwaway project that reproduces the
shape in question, and observe what dbt really does.

This is not a formality. Every selector and manifest bug found in this repo so far was found
this way, and each had already survived a reading of dbt's own source:

- An fqn selector was believed to match a node's fqn. It does — but `QualifiedNameSelectorMethod`
  *also* matches the fqn with its package stripped, so a fix verified against the inner
  `is_selected_node` helper looked correct and was wrong end-to-end. The layout that exposes it needs
  an *installed package* (`libs/pkg/models/<root-pkg-name>/x.sql`), so a root-only fixture cannot
  reach it — check whether the shape you are testing can actually occur in the project layout.
- A selector believed to isolate one node was confirmed with `dbt ls` to select two, which is
  how a duplicate-build bug surfaced.
- `path:` selectors were assumed to be literal paths. dbt resolves them through `Path.glob`, so a
  path containing `*?[]` silently matches *nothing*.
- Unit tests on versioned models were assumed to carry per-version fqns. dbt clones the fqn
  verbatim, so hand-written fixtures encoding a per-version fqn made tests pass against
  behaviour dbt never produces.

`tests/integration/test_selector_against_dbt.py` does exactly this and is the place to add cases:
dbt is a declared test dependency, so it writes real projects, has dbt parse them, runs the factory
over the real manifest, and feeds every emitted selector back through `dbt ls`. It also generates
randomised layouts, so a shape nobody enumerated still gets checked. Add a regression layout there
whenever you touch selector construction.

A related lesson: selector construction is now *one* rule — intersect every discriminator dbt gives
you, dropping only terms its grammar cannot express — rather than a decision tree per shape. Several
rounds of per-case branching each shipped a new blind spot. If you find yourself adding a branch for
a newly-discovered layout, prefer strengthening the uniform rule.

**And a harder lesson: a rule that keeps needing repair is the wrong rule, however uniform.** dbt has
no `unique_id:` selector, so every selector is a predicate whose exactness must be established. The rule
is "the fqn or the name must survive, or refuse" — deliberately stricter than dbt, because establishing
exactness from `package:`+`file:` instead means modelling dbt's whole matching semantics (which sections
`file:` reaches, that it matches a base name rather than a path, that sources count because
`--indirect-selection` defaults to eager), and every such model has needed repair.

Before adding machinery that models dbt's matching semantics, ask which project shapes it actually buys
and whether refusing them is cheaper. Prefer a guarantee that holds per node over one that depends on the
rest of the project, and accept being stricter than dbt when the alternative cannot be verified by
reading it.

**Fail rather than silently weaken a guarantee.** When a property cannot be established — a selector that
may not be exact, a test gate that cannot be added without creating a cycle — raise, naming the resource
and a remedy. Do not emit the task with the guarantee quietly dropped: the failure then surfaces as wrong
data or an ungated model at run time, and which resource is affected can hinge on something as arbitrary
as a name's sort order. Check the blast radius first (a refusal that fires on ordinary layouts is the
wrong rule), and prefer a global property verified on the real artifact over a local predicate that
approximates it. Read facts like "is this model versioned?" from the manifest field (`version`) rather
than parsing them out of an id, whose spelling is ambiguous (`model.pkg.orders.v1.1` vs
`model.pkg.vendors`).

Practically:

- **Prefer `dbt ls`** (or `dbt parse` plus the resulting `target/manifest.json`) over reading
  dbt's Python. `dbt ls --select '<selector>'` answers "what does this select?" definitively.
- **Feed generated output back through dbt.** For a change to selectors, run each selector the
  factory emits through `dbt ls` and confirm it selects exactly the intended nodes — in per-test
  *and* bundled mode, since `--indirect-selection` changes the answer.
- **Let dbt build the fixture.** When a test needs a manifest shape, have dbt parse a small
  project and read its real `manifest.json` rather than hand-writing node dicts. Hand-written
  fixtures encode assumptions and will happily confirm them.
- **Check the current release**, not the pinned one, when reasoning about upstream behaviour;
  note the version you verified against in the commit or PR.
- **State how you verified.** "Confirmed with `dbt ls` on dbt 1.11.6" is reviewable; "matches
  dbt's selector semantics" is not.

## Every change needs a test

No behaviour change — fix, feature, or refactor — lands without a test covering it.

- **Write the test first and watch it fail** for the expected reason. A test written after the
  code passes immediately and proves nothing about whether it can catch the bug.
- **Assert the real behaviour**, not a restatement of the implementation. If a fix stops a model
  being built twice, assert what the emitted selector resolves to, not that a helper returned a
  particular string.
- **Cover the fallbacks too.** Guard clauses (missing field, unusable value, absent path) are
  where silent failures hide.
- **Document known limitations with a test that pins the current behaviour**, so a partial fix is
  recorded honestly rather than looking complete.

The golden job definitions under `tests/test_data/` are generated output, never hand-edited. If a
change alters them, regenerate each one by running the CLI with the same options as the test that
consumes it (see `tests/conftest.py` and `run_job_spec_test`), then confirm the diff contains only
what you intended. Hand-editing a golden to match new output hides whatever else moved.

## Document the current state, not the history

Docstrings, comments, README and this file describe how the code behaves **now**. Git history records
what changed and why, so "an earlier revision did X", "three rounds of review found Y", or a narration
of approaches already discarded does not belong in them — it grows with every fix, goes stale silently,
and buries the rule a reader came for.

Keep the *constraint* when it stops someone reintroducing a bug, but state it as a rule rather than as a
story: "read the version from the manifest's `version` field; the id's last segment cannot be told from a
model name" rather than "an earlier revision compared ids by substring, which broke on `vendors`". One
clause on the failure mode is usually enough; a test name is a better pointer than a paragraph.

The same applies to error messages: state what the code established and no more. Explaining *why* a
project is shaped the way it is means inferring something the check never verified, and that inference
is what turns out to be wrong.
