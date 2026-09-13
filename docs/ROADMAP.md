# Roadmap

## Shipped foundation

- External JSON Lines adapters exercise packaged client APIs.
- `testctl` owns deadlines, process supervision, deterministic verdicts, and
  sealed replayable evidence.
- The model broker self-tests the harness without making Kafka compatibility
  claims.
- Pinned Apache Kafka 3.7.2 through 4.3.1 environments cover single- and
  three-broker clusters, custom-root TLS, SASL/PLAIN, and SCRAM-SHA-256/512,
  including every SASL mechanism over TLS on Kafka 4.3.1.
- Real-Kafka scenarios cover producing, assigned, group, and share consumption;
  configured singleton and ordered partial-batch topic creation with public and
  independent value proof, exact three-broker manual
  topic replica placement and manual partition expansion with exact replica order
  and full-ISR metadata proof, exact duplicate-creation
  rejection, exact unknown-topic rejection, source-preserving routing failure
  for an independently proven absent partition, partition
  expansion and deletion, validate-only topic creation, partition expansion,
  and topic-configuration replacement, metadata-backed plus explicit
  single-page and cursor-followed `DescribeTopicPartitions` topic description,
  caller-ordered
  detailed plural name- and topic-ID descriptions with requested authorization
  bitfields, detailed byte-sorted topic listing with requested authorization
  metadata plus paired exclusion and inclusion of independently present
  canonical `__consumer_offsets`, and name-based deletion with mixed resource outcomes, scoped topic,
  cluster identity with requested authorization metadata and paired
  fenced-broker exclusion/inclusion across one reversible graceful stop,
  consumer-only and generic all-group discovery with exact state and group-type
  filters plus generic protocol-type filtering, singleton earliest-, latest-,
  maximum-timestamp-, and caller-timestamp-offset administration plus
  caller-ordered batch earliest and latest selection, consumer-group offset
  listing, alteration, and
  deletion, caller-ordered mixed classic/KIP-848 group descriptions with
  requested authorization bitfields, static
  classic-member removal, singleton record-prefix deletion and caller-ordered plural record
  deletion with explicit and high-watermark boundaries, singleton and caller-ordered plural
  empty-group deletion, and selected
  topic-configuration description, caller-ordered plural selected-configuration
  description with exact synonym/documentation options and complete public
  entry metadata, exact incremental Set, Delete, Append, and Subtract methods,
  legacy full-snapshot replacement and default restoration, plus
  singleton replacement and restoration;
  caller-ordered
  literal ACL creation, exact description, and exact deletion plus named-user
  producer and consumer quota replacement, description, and removal and
  SCRAM-SHA-256/512 credential upsert, description, and deletion, plus active
  singleton and caller-ordered plural Share-group state, assignment, and
  requested authorization descriptions, singleton and caller-ordered plural
  selected partition offset listings, partition offset alteration/deletion, and
  caller-ordered empty-group deletion, against
  independent Kafka CLI state; stable-baseline transaction discovery with
  exact state, producer-ID, duration, and Kafka 4.3 pattern filtering;
  transactions, replacement and Admin
  force-termination fencing, broker-derived single-partition Admin abort with
  pre-cleanup state proof, reversible stopped-broker unregistration with exact
  remaining and restored cluster identity, broker restart, rolling restart,
  independently targeted ordinary and transactional partition-leader recovery,
  and controller, classic/KIP-848 group-coordinator, and
  transaction-coordinator recovery.
- Public Admin log-directory coverage preserves selected broker and replica
  placement details, then proves caller-ordered two-directory replica moves
  through independently polled Kafka CLI state with no remaining future copy.
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
  independent observer routes plus deterministic single-record, batch,
  transactional-batch, classic/KIP-848 consume-transform-produce, and consumer
  recovery contracts.
- Deterministic concurrent actor groups cover multiple public producers and
  assigned producer/consumer pipelines with exact start/join boundaries,
  stable actor identities, and independent broker truth.
- Direct assigned-consumer records are observed through both the waiting
  `recv` object and repeated immediate `try_take_batch` calls, with the selected
  public method retained in command evidence. Retained direct-consumer failures
  are likewise observed through both `next_event` and `try_take_event`, with
  exact public fences and broker codes joined to independent policy evidence.
- Classic and KIP-848 assignment transitions are observed through both the
  public `next_event` future and immediate `try_take_event` method, with the
  exact selection retained beside stable assignment and broker-visible proof.
- Direct-consumer batches reach owned records through both the owned-batch
  chain and direct `into_owned_records` conversion before transfer through an
  ordinary producer. Exact source coordinates and bytes are read again after
  the destination terminal, while independent observations prove the original
  and transferred records, preserved timestamp and nullable bytes, and ordered
  source headers.
- Producer, assigned, classic, KIP-848, and Share scenarios preserve null versus
  empty keys and values, tombstones, duplicate nullable binary headers, public
  coordinates, and multi-record receive sets against independent broker records;
  producer scenarios additionally pin sequential and batched partition order.
  A dedicated producer scenario carries one explicit timestamp through the
  public delivery receipt, independent Kafka observation, and public assigned
  consumer record.
  A second scenario omits the public explicit partition for a keyed record and
  binds Kafkars's receipt and broker placement to an independent Java-compatible
  positive-Murmur2 partition oracle.
  A UUID-bound ordinary-send scenario preserves the complete public receipt,
  including topic identity, optional leader epoch, and null-versus-empty
  serialized sizes, against prior public and independent topic identity plus
  independent record truth.
  Share batches cover ordered multi-record acquisition, the public `accept_all`
  conversion, record-specific accept, release, and reject decisions, and
  complete dropped-batch redelivery.
- Producer cancellation separately retains public `Delivery` and waiting
  `Send` observers across two stage-aware cancellation requests, preserves
  `too_late` uncertainty, and joins cancellation monotonicity to ordinary
  independent delivery truth.
- Independent producer-configuration scenarios cover explicit client-wide
  delivery timeout, retry, ownership limits, request concurrency, linger, and
  none, gzip, snappy, LZ4, and zstd public compression selections. Successful
  ordinary producer construction records the selected builder timeout, proving
  exact per-handle overrides and configured-client inheritance.
- A dedicated producer scenario selects public `Producer::send` rather than
  `try_send`, retaining its bounded waiting-admission choice in evidence while
  proving the resulting receipt against an independent Kafka record.
- Public client metrics coverage retains every calls, failures, mailbox,
  latency, and producer snapshot getter after independently verified producer
  work, with exact command ownership and deterministic cross-field invariants.
- Dedicated client-metrics resource discovery retains its exact public
  throttle and canonical names against a separately provisioned and immediately
  listed pinned Kafka CLI state.
- Kafka 4.3.1 finalized-feature updates cover the public validation-only path,
  caller-ordered outcomes, and exact unchanged CLI state and epoch.
- Kafka 4.3.1 authenticated delegation-token coverage exercises public create,
  owner-filtered describe, renew, and immediate expire under one deadline,
  retains no HMAC bytes, and proves final absence through a secret-free pinned
  CLI projection on an independently authenticated listener.
- Kafka 4.3.1 modern Streams-group coverage provisions two real bundled
  WordCount applications, then exercises all seven public description, stable
  offset, offset-mutation, and group-deletion methods under one deadline with
  caller-ordered results and an independent pinned CLI final-absence proof.
- Directly assigned consumers cover repeated cursor advance, beginning, end,
  and exact-offset replacement, seek replay, pause/resume partition isolation,
  incremental add/remove with survivor cursors, and read-committed isolation
  after an independently verified aborted transaction. One selected batch also
  exposes its UUID-qualified Fetch offset window, byte charge, and checkpoint
  against independent topic identity, watermarks, and record progress. The read-committed path
  uses non-default public Fetch and retained-delivery capacity policy. Repeated
  lifecycle operations and public controls settle against exact command
  identities rather than aggregate resource counts.
- Classic and KIP-848 group consumers cover public pause/resume partition
  isolation and assignment-fenced seek replay, with committed public outcomes
  joined to positive protocol epochs and independent broker coordinates.
- Classic and KIP-848 group records are observed through both the waiting
  `recv` object and repeated immediate `try_take_batch` calls, with the selected
  public method retained in command evidence. Full-batch commits retain both
  the canonical `checkpoint` conversion and its public `into_checkpoint`
  compatibility alias. Both protocols also acknowledge
  an assignment-fenced checkpoint midway through work that exceeds the original
  processing window, then commit the same batch inside the renewed window. A
  second path marks and commits only an ordered processed prefix, independently
  proves its offset, and resumes the exact suffix with a replacement member.
- Classic and KIP-848 group consumers preserve caller-ordered multi-topic
  subscriptions through returned-handle readback, expose an assignment for
  every subscribed topic, and commit exact records from both topics against
  independent broker observations.
- Share consumers preserve caller-ordered multi-topic subscriptions, expose an
  assignment for every subscribed topic, and acquire and accept exact records
  from both topics against independent broker observations. Returned-handle
  readback retains the subscription and configured rack identity, which is also
  retained by broker-reported singleton and caller-ordered plural Share-group
  descriptions.
- Classic and KIP-848 configured groups cover fail-closed, earliest, and latest
  missing-offset reset behavior plus read-committed isolation against
  pre-membership records and independently
  verified aborted transactions; classic membership also proves explicit
  cooperative-sticky selection through a broker-reported public description.
  Classic configuration additionally carries non-default public session,
  rebalance, heartbeat, and rejoin timing through live single-broker operation
  and three-broker recovery. Both protocols run seek replay with non-default
  Fetch and retained-delivery capacity policy plus processing,
  membership-start, seek, and close deadlines selected through both the
  individual setters and aggregate public group operation configuration.
- Classic and KIP-848 hosted groups cover clone-shared shutdown, repeated
  request idempotence, public event-stream termination, and independently
  queried zero-member broker state.
- Configured Share consumers cover every public long-poll, byte, record,
  acquisition-range, attempt-timeout, membership-start, and close setting at
  non-default values while every delivered record remains joined to independent
  broker observations.
- Lifecycle scenarios cover repeated client readiness and producer flush plus
  independent client progress after another client shuts down. Every successful
  client creation also reads back and records the exact public client ID,
  caller-ordered bootstrap servers, and optional expected cluster ID.
- Explicit child-handle ownership preserves the original shared client path
  while qualifying private producer close and replacement owners and two
  directly assigned consumers with independent cursor state from one client
  configuration.
- Multi-record transactions span topics and partitions through individual
  sends, while dedicated homogeneous sets exercise one public `send_batch` for
  both commit and abort with the same field and header distinctions. A
  successful original or replacement initialization records the public
  transactional ID, broker-issued producer ID and epoch, and active-owner
  state. A dedicated commit resolves caller-ordered public topic UUIDs, binds every
  staged record to its UUID, and seals the current transaction revision through
  fresh public validation before commit. Committed sets retain exact independent coordinates and
  per-partition order, aborted sets remain wholly read-committed invisible, and
  successive commit/abort boundaries on one public producer cannot overlap. A
  separate singleton scenario derives the active producer and coordinator
  identity through public Admin, aborts that exact partition transaction, proves
  the public open-to-cleared transition before token cleanup, and confirms the
  cleared identity through Kafka's pinned CLI.
- Classic and KIP-848 consume-transform-produce scenarios transfer public
  assignment-fenced checkpoints with `send_offsets`; committed checkpoints are
  independently queried and aborted checkpoints are proved unchanged through
  replacement-member redelivery.
- A pinned composite action gives client repositories one qualification entry
  point while Testlab retains the broker matrix and verdict rules.

## Now — complete the stable boundary

1. Extend black-box coverage across high-use public group configuration and
   Admin operations that currently have only client-repository evidence.
2. Fix client failures exposed by Testlab until every gating release cell
   passes.
3. Derive Kafkars support and release eligibility from archived qualification
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
