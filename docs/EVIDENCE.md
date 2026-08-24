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
- `broker-state-observations.jsonl`
- `verdict.json`
- `summary.md`
- `reproduction.sh`
- `digests.json`

Evidence schema v6 records the exact environment identity in `manifest.json`.
Every effectful environment terminal operation carries a stable identity in
`history.jsonl`; retained stdout and stderr are named by that operation.
Docker environments pull and then inspect the declared digest as separate
operations before Compose receives the image reference. Scenario-owned broker
restarts and their Kafka readiness probes are recorded as distinct operations
without stopping the packaged adapter process.
If a broker process exits before initial readiness, Testlab retains its failed
readiness, process state, and logs, then permits one deadline-bounded start of
the same container. A repeated exit makes the run invalid.
If Docker loses a reserved loopback port before Compose owns it, Testlab retains
the failed start, removes its partial project, assigns a fresh reserved port
set, and permits one deadline-bounded Compose retry. A second collision makes
the run invalid.
Provisioning waits for every harness-created scenario partition to report a
leader and the topology's full in-sync replica count before starting the
packaged client.
Real-Kafka runs also record one `broker_observe` operation. Its librdkafka
snapshot targets only record-bearing adapter commands actually issued in the
recorded harness history; an issued batch, transaction, or fencing command
contributes every contained operation. The snapshot uses broker watermarks and
emits structured observations with exact partition, offset, key, value, and
ordered header bytes.

Read-only Admin claims deliberately reuse those independent record facts. A
topic-description claim covers only declared partitions later exercised by
observed records, and an all-topic listing claim covers only declared required
topics with observed markers. A latest-offset claim covers one isolated
partition whose greatest observed offset is exactly one less than the declared
end offset. The evidence does not imply exhaustive topic discovery, internal
topic classification, replica topology, or untested offset selectors.

`broker-state-observations.jsonl` retains independently queried broker state
that is not a record snapshot. The first state observation is one exact
consumer-group committed offset, queried only when its correlated public admin
command was actually issued. The state-query consumer never joins the group,
subscribes, assigns, or commits. An absent broker offset remains explicit
absence, and observer errors invalidate the run rather than manufacture a
client result. ADMIN-006 requires the public and independent offsets to match
the scenario's declared checkpoint.

## Qualification evidence

`testctl qualify` creates one `<qualification-run-id>.partial` tree, executes
every reviewed environment/pack cell into `cells/<cell-id>/<run-id>`, derives
cell and top-level status from the sealed run verdicts, recursively digests the
complete tree, and only then publishes the qualification directory.
Cells declare a bounded attempt count. Every scenario run records its one-based
attempt ordinal, and any failed or invalid repetition contributes to the cell.

At least one cell must be gating. For gating cells, `invalid` outranks `failed`
and `failed` outranks `passed`. Non-gating cells remain visible but cannot make
the release-facing aggregate pass or fail.

## Status

`passed` means valid evidence and no semantic violation.

`failed` means valid evidence with one or more semantic violations. An
adapter-reported public client API error is such a violation and remains
distinguishable from adapter, protocol, process, or environment invalidity.

`invalid` means process, protocol, timeout, capability, environment, broker, or
harness failure prevents a client claim.

## Evidence references

Violations cite stable locations such as `history:12`,
`broker-observation:3`, `broker-state-observation:0`, and
`scenario:operation:op-1`.

## Future LLM summary

An LLM receives a deterministic packet containing status, contract IDs, metric
IDs, and bounded excerpts. Its output is derived narrative. It never alters the
verdict or raw evidence.
