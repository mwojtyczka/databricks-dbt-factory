# Guidance for Claude

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
  `is_selected_node` helper looked correct and was wrong end-to-end.
- A selector believed to isolate one node was confirmed with `dbt ls` to select two, which is
  how a duplicate-build bug surfaced.
- `path:` selectors were assumed to be literal paths. dbt resolves them through `Path.glob`, so a
  path containing `*?[]` silently matches *nothing*.
- Unit tests on versioned models were assumed to carry per-version fqns. dbt clones the fqn
  verbatim, so hand-written fixtures encoding a per-version fqn made tests pass against
  behaviour dbt never produces.

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
