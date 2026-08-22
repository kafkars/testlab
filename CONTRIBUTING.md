# Contributing

1. Read `ARCHITECTURE.md` and `AGENTS.md`.
2. Add or update a machine-readable scenario or contract before broadening the
   runner.
3. Keep adapters black-box and process-isolated.
4. Add deterministic evidence for every verifier rule.
5. Run `scripts/check` and `scripts/run-reference-pack`.

A new public protocol or manifest field requires a version bump. A new release
claim must be backed by sealed evidence, not console output.
