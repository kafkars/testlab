# Roadmap

## Batch 1 — repository foundation (this zip)

- Versioned schemas and process adapter protocol.
- Owned runner, deadline, model broker, verifier, evidence sealer, and zrail policy.
- Reference adapter and three producer truth scenarios.

## Batch 2 — real kafkars Rust adapter

- Resolve and build packaged `kafka-client` artifacts.
- Map public admission, delivery, flush, close, and shutdown semantics.
- Add backpressure, cancellation, drop, and outstanding-operation scenarios.
- Run the same scenarios against Rust base and head subjects.

## Batch 3 — Kafka protocol adversary

- Add API-version negotiation, partial frames, wrong correlation IDs, stale
  responses, read/write stalls, disconnect points, retriable/fatal errors, and
  leader/coordinator movement.
- Preserve minimized failure scripts as a permanent corpus.

## Batch 4 — real clusters and interoperability

- Pin oldest/current/next Kafka versions and image digests.
- Add independent Java seeding and Fetch-based verification.
- Add leader, controller, group coordinator, transaction coordinator, TLS,
  SCRAM, ACL, quota, and rolling-restart scenarios.
- Add Java and librdkafka interoperability.

## Batch 5 — C ABI gauntlet

- Test C11/C++17, static/dynamic linking, symbol/layout snapshots, older struct
  sizes, copy-in, retained-event backpressure, release order, and shutdown.
- Add sanitizer, Miri-compatible, and stateful FFI fuzz lanes.

## Batch 6 — consumers, groups, transactions, generated histories

- Add consumer, assignment, rebalance, checkpoint, transaction, fencing, and
  atomic offset scenarios.
- Add concurrent actors, barriers, seeded generation, replay, and shrinking.
- Add nightly, weekly-chaos, and release packs.
- Add deterministic analysis packets and optional LLM narrative summaries.
