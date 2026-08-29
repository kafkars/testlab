# Roadmap

## Shipped foundation

- External JSON Lines adapters exercise packaged client APIs.
- `testctl` owns deadlines, process supervision, deterministic verdicts, and
  sealed replayable evidence.
- The model broker self-tests the harness without making Kafka compatibility
  claims.
- Pinned Apache Kafka 3.7.2 through 4.3.1 environments cover single- and
  three-broker clusters, TLS, SASL/PLAIN, and SCRAM-SHA-256/512.
- Real-Kafka scenarios cover producing, assigned, group, and share consumption;
  singleton and ordered partial-batch topic creation, exact duplicate-creation
  rejection, exact unknown-topic and invalid-partition rejection, partition
  expansion and deletion, validate-only topic creation, partition expansion,
  and topic-configuration replacement, scoped topic, cluster, and
  consumer-group discovery, earliest- and
  latest-offset administration, consumer-group offset listing, alteration, and
  deletion, record-prefix deletion, empty-group deletion, and selected
  topic-configuration description, replacement, and restoration; transactions,
  fencing, broker restart, rolling restart, and independently targeted
  partition-leader, controller, classic and KIP-848 group-coordinator, and
  transaction-coordinator recovery.
- A SASL/PLAIN policy environment and targeted scenarios cover topic produce,
  classic-group consume, admin create, and transactional-ID authorization
  denial with observed permission restoration, plus bounded producer and
  consumer progress under independently observed byte-rate quotas.
- Pull-request and release qualifications package Kafkars first and verify
  broker-visible truth independently through librdkafka.
- A versioned external Kafka protocol adversary covers partial frames, wrong
  correlation IDs, stale responses, bounded stalls, selected disconnect points,
  and minimized metadata and producer replay cases.
- A versioned external TCP proxy covers live connection cuts, bidirectional
  blackholes, and one-way latency, with separate adapter, hidden upstream, and
  independent observer routes plus deterministic recovery contracts.
- Deterministic concurrent actor groups cover multiple public producers and
  assigned producer/consumer pipelines with exact start/join boundaries,
  stable actor identities, and independent broker truth.
- Producer, assigned, classic, KIP-848, and Share scenarios preserve null versus
  empty keys and values, tombstones, duplicate nullable binary headers, public
  coordinates, and multi-record receive sets against independent broker records;
  producer scenarios additionally pin sequential and batched partition order.
  Share batches cover ordered multi-record acquisition, record-specific accept,
  release, and reject decisions, and complete dropped-batch redelivery.
- Producer cancellation retains one public delivery observer across two
  stage-aware cancellation requests, preserves `too_late` uncertainty, and
  joins cancellation monotonicity to ordinary independent delivery truth.
- Independent producer-configuration scenarios cover explicit client-wide
  delivery timeout, retry, ownership limits, request concurrency, linger, and
  none, gzip, snappy, LZ4, and zstd public compression selections.
- Public client metrics coverage retains every calls, failures, mailbox,
  latency, and producer snapshot getter after independently verified producer
  work, with exact command ownership and deterministic cross-field invariants.
- Directly assigned consumers cover repeated cursor advance, beginning, end,
  and exact-offset replacement, seek replay, pause/resume partition isolation,
  incremental add/remove with survivor cursors, and independent cursors across
  two public consumers. Repeated lifecycle operations and public controls settle
  against exact command identities rather than aggregate resource counts.
- Classic and KIP-848 group consumers cover public pause/resume partition
  isolation and assignment-fenced seek replay, with committed public outcomes
  joined to positive protocol epochs and independent broker coordinates.
- Classic and KIP-848 configured groups cover latest missing-offset reset and
  read-committed isolation against pre-membership records and independently
  verified aborted transactions.
- Classic and KIP-848 hosted groups cover clone-shared shutdown, repeated
  request idempotence, public event-stream termination, and independently
  queried zero-member broker state.
- Configured Share consumers cover public record ceilings and one-record
  acquisition ranges, retaining exact public acquisition counts while every
  delivered record remains joined to independent broker observations.
- Multi-handle lifecycle scenarios cover repeated client readiness and producer
  flush, sibling and replacement producer progress after close, and independent
  client progress after another client shuts down.
- Multi-record transactions span topics and partitions with the same field and
  header distinctions. Committed sets retain exact independent coordinates and
  per-partition order, aborted sets remain wholly read-committed invisible, and
  successive commit/abort boundaries on one public producer cannot overlap.
- Classic and KIP-848 consume-transform-produce scenarios transfer public
  assignment-fenced checkpoints with `send_offsets`; committed checkpoints are
  independently queried and aborted checkpoints are proved unchanged through
  replacement-member redelivery.
- A pinned composite action gives client repositories one qualification entry
  point while Testlab retains the broker matrix and verdict rules.

## Now — adopt the release boundary

1. Make Kafkars pull requests call the pinned Testlab action and archive its
   sealed evidence.
2. Make scheduled and manual release qualification call the Testlab release
   tier.
3. Remove duplicated broker, scenario, matrix, and aggregation logic from the
   Kafkars repository.
4. Fix client failures exposed by Testlab until every gating release cell
   passes.
5. Derive Kafkars support and release eligibility from archived qualification
   evidence.

## Next — broaden failure coverage

- Add seeded generation, shrinking, soak, and weekly chaos packs without
  weakening deterministic verdicts.

## Later — additional client surfaces

- Add Java and librdkafka reference adapters over shared public semantics.
- Add a C ABI gauntlet when Kafkars exposes a versioned foreign interface.
- Add deterministic analysis packets and optional narrative summaries that
  cannot alter validity, pass/fail, or release eligibility.

## Release rule

A release requires complete passing evidence for every gating cell. A failed or
invalid cell blocks the claim. Model-broker runs and narrative output never
count as Kafka compatibility evidence.
