# Evidence model

Console output is for humans. Sealed evidence is the product.

A run is written to `<run-id>.partial` and renamed only after required files and
digests exist.

## Required artifacts

- `manifest.json`
- `scenario.json`
- `subject.json`
- `environment.json`
- `adapter.json` after a successful handshake
- `history.jsonl`
- `broker-observations.jsonl`
- `verdict.json`
- `summary.md`
- `reproduction.sh`
- `digests.json`

Evidence schema v2 records the exact environment identity in `manifest.json`.
Every effectful environment terminal operation carries a stable identity in
`history.jsonl`; retained stdout and stderr are named by that operation.

## Qualification evidence

`testctl qualify` creates one `<qualification-run-id>.partial` tree, executes
every reviewed environment/pack cell into `cells/<cell-id>/<run-id>`, derives
cell and top-level status from the sealed run verdicts, recursively digests the
complete tree, and only then publishes the qualification directory.

At least one cell must be gating. For gating cells, `invalid` outranks `failed`
and `failed` outranks `passed`. Non-gating cells remain visible but cannot make
the release-facing aggregate pass or fail.

## Status

`passed` means valid evidence and no semantic violation.

`failed` means valid evidence with one or more semantic violations.

`invalid` means process, protocol, timeout, capability, environment, broker, or
harness failure prevents a client claim.

## Evidence references

Violations cite stable locations such as `history:12`,
`broker-observation:3`, and `scenario:operation:op-1`.

## Future LLM summary

An LLM receives a deterministic packet containing status, contract IDs, metric
IDs, and bounded excerpts. Its output is derived narrative. It never alters the
verdict or raw evidence.
