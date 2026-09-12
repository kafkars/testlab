# kafkars Rust adapter handoff

This adapter resolves `kafkars = 0.0.2-rc.1` from public revision
`e556a2f04df855c242b5befb7feceb74e0ed4426` and uses only its curated public
module facade. It implements producer, assigned/group/share consumer, admin,
transaction, and lifecycle commands; Kafka environment control and broker
observation remain testlab-owned.

The checked-in dependency remains the public RC baseline. Candidate builds
replace it with the packaged checkout, advertise `independent_handles`, and use
the public independent producer and assigned-consumer builders; baseline builds
do not advertise or compile those newer calls.

It:

1. depends only on the packaged public `kafkars` surface;
2. implements protocol v85 over stdin/stdout;
3. configures exact expected cluster identity through the public client builder,
   verifies that the returned public handle retains it, and exercises both
   fail-closed mismatch and repeated readiness checks against independent
   cluster metadata; preserves caller-selected record timestamps and returned
   partitions through public delivery receipts, omits explicit partitions for Java-keyed sends,
   preserves consumer records, and passes caller-ordered multi-topic classic,
   KIP-848, and Share subscriptions plus complete Share Fetch, runtime, and rack
   configuration through the public builder; group registrations also preserve
   caller-selected Fetch policy, retained-delivery capacities, and processing,
   membership-start, seek, and close deadlines, while configured assigned
   clients preserve the same Fetch and capacity envelope;
4. preserves admission rejection separately from accepted delivery;
5. maps client outcomes to acknowledged, definitely-not-sent, or possibly-sent
   without inventing certainty;
6. preserves caller order and exact per-resource public outcomes for admin
   batches, including mixed success and failure;
7. maps singleton public admin errors to stable normalized `command_failed`
   events without receiving scenario expectations;
8. forwards validate-only topic creation, partition increase, and incremental
   configuration changes through the packaged public builders;
9. executes metadata-backed and explicit `DescribeTopicPartitions` singleton
   topic descriptions, detailed caller-ordered plural topic descriptions and
   caller-ordered plural name-based topic deletion with mixed outcomes,
   caller-ordered plural selected topic-configuration descriptions and
   incremental and legacy full-snapshot replacements through both
   topic-convenience and generic resource APIs, plus filtered canonical generic
   topic-resource and dedicated client-metrics resource listings,
   caller-ordered topic-partition and consumer-group offset batches, singleton
   and caller-ordered plural record deletion with explicit and high-watermark
   boundaries, cluster feature discovery and validation-only finalized-feature
   updates with caller-ordered outcomes, exact partition active-producer
   state, canonical metadata-quorum discovery, canonical transaction listing,
   caller-ordered transaction descriptions, and caller-ordered producer
   fencing with independently matched post-fence identities, selected-replica
   log-directory description and caller-ordered alteration outcomes,
   classic static membership plus complete session, rebalance, heartbeat, and
   rejoin timing, and caller-ordered static-member removal after owner
   abandonment,
   consumer-only and generic all-group listings, plural offset
   mutations, dedicated classic-group descriptions, caller-ordered plural
   empty classic-group deletion, singleton and
   caller-ordered plural active Share-group state, rack, and assignment
   descriptions,
   singleton and caller-ordered plural selected Share-group offset listings,
   empty-group alteration, topic-wide offset
   deletion, and caller-ordered empty-group deletion, literal ACL
   create/describe/delete operations, named-user client-quota
   set/describe/remove operations, and named-user SCRAM-SHA-256/512
   upsert/describe/delete operations as one bounded public call with exact
   non-secret outcomes;
10. explicitly settles flush, close, client shutdown, exact group-owner
   abandonment, and clone-shared hosted group shutdown through public
   event-stream termination;
11. exposes the exact packaged version in its descriptor and subject metadata.

Do not copy the model-broker client into the production adapter. The reference
adapter is a harness fixture, not a Kafka implementation template.
