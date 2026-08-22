# Repository contract

This repository tests packaged Kafka clients from the outside. It does not gain
privileged access to client internals merely to make a scenario easier.

## Before coding

- Read `ARCHITECTURE.md` and the relevant protocol or evidence document.
- Identify whether a fact comes from the adapter, environment, or harness.
- Preserve uncertainty instead of manufacturing a definite outcome.
- Keep deterministic verdict logic independent from narrative reporting.

## Non-negotiable rules

- Adapters are external processes speaking the versioned JSON Lines protocol.
- Adapter stdout contains protocol messages only; diagnostics go to stderr.
- Every command and terminal operation carries a stable identity.
- After manifests validate and a run identity exists, execution failures seal
  invalid evidence whenever the evidence filesystem remains writable.
- Broker-visible truth is checked independently from adapter-reported success.
- LLM output never decides validity, pass/fail, or release eligibility.
- `unsafe` remains forbidden in the harness workspace.

## Rust source shape

- Every Rust source file begins with a `//!` module contract.
- `lib.rs` and `mod.rs` are declarative facades: module declarations and
  re-exports only.
- Unit tests live in sibling `*_test.rs` files.
- Rust source files remain at or below 300 lines.
- Run `scripts/check` before handing off a change.
