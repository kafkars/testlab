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
2. implements protocol v114 over stdin/stdout;
3. configures exact expected cluster identity through the public client builder,
   verifies that the returned public handle retains it, and exercises both
   fail-closed mismatch and repeated readiness checks against independent
   cluster metadata; preserves caller-selected record timestamps, returned
   partitions, topics, UUIDs, optional leader epochs, and nullable serialized
   sizes through public delivery receipts, omits explicit partitions for Java-keyed sends,
   preserves consumer records, and passes caller-ordered multi-topic classic,
   KIP-848, and Share subscriptions plus complete Share Fetch, runtime, and rack
   configuration through the public builder; group registrations also preserve
   caller-selected Fetch policy, retained-delivery capacities, and processing,
   membership-start, seek, and close deadlines through either the exact
   individual setters or one public aggregate operation configuration, while
   configured assigned clients preserve the same Fetch and capacity envelope;
   direct receives map
   default `recv` and selected repeated `try_take_batch` to their exact public
   methods, retain public Fetch UUID, offset-window, byte-charge, and checkpoint
   evidence for independent comparison, and an owned direct-consumer record
   reaches an ordinary producer through either `into_owned().into_records()` or
   direct `into_owned_records()` while its source lease stays readable after
   delivery; a selected
   bounded delay publicly acknowledges the retained
   group checkpoint before committing that same batch; complete batches convert
   through either exact public `checkpoint()` or compatibility
   `into_checkpoint()`, and a selected partial
   checkpoint marks only the declared ordered prefix through the public builder;
   retained failure events map default `next_event` and selected repeated
   `try_take_event` with complete public fences and failure kinds; group
   assignment transitions likewise preserve selected public `next_event`
   or `try_take_event` observation beside stable assignment snapshots; Share
   acknowledgements preserve either explicit record decisions or the public
   `accept_all` batch conversion;
4. preserves immediate `try_send` rejection separately from accepted delivery,
   and maps an explicit `send` selection to Kafkars's bounded FIFO waiting
   operation without an adapter retry loop; cancellation retains that method
   and invokes either `Delivery::cancel` or `Send::cancel` twice on its original
   observer;
5. maps client outcomes to acknowledged, definitely-not-sent, or possibly-sent
   without inventing certainty;
6. resolves selected ordinary and transactional record topics through public
   Admin and applies each public `expected_topic_uuid` guard; ordinary sends
   return that UUID in their receipt, while transactions wait for a fresh
   `validate_for_commit` seal before committing the exact current revision;
7. preserves caller order and exact per-resource public outcomes for admin
   batches, including mixed success and failure;
8. maps singleton public admin errors to stable normalized `command_failed`
   events without receiving scenario expectations;
9. forwards validate-only topic creation, partition increase, and incremental
   configuration changes through the packaged public builders;
10. executes metadata-backed and explicit `DescribeTopicPartitions` singleton
   topic descriptions, detailed caller-ordered plural topic descriptions and
   caller-ordered plural name-based topic deletion with mixed outcomes,
   caller-ordered plural selected topic-configuration descriptions and
   exact synonym/documentation options with complete public entry metadata,
   exact incremental Set, Delete, Append, and Subtract methods, plus legacy
   full-snapshot replacements through both
   topic-convenience and generic resource APIs, including legacy default
   restoration with no expected value on the wire, plus filtered canonical
   generic topic-resource and dedicated client-metrics resource listings,
   caller-ordered topic-partition and consumer-group offset batches; singleton
   earliest, latest, maximum-timestamp, and exact-timestamp offset selection
   with returned public timestamps; and singleton and caller-ordered plural record deletion with explicit and high-watermark
   boundaries, cluster feature discovery and validation-only finalized-feature
   updates with caller-ordered outcomes, exact partition active-producer
   state, canonical metadata-quorum discovery, canonical transaction listing
   with exact state, signed producer-ID, duration, and transactional-ID-pattern
   selection against a stable unfiltered baseline,
   caller-ordered transaction descriptions, caller-ordered producer fencing
   with independently matched post-fence identities, and singleton Admin force
   termination of an active transaction before replacement initialization,
   plus broker-derived singleton Admin partition abort with public
   open-to-cleared producer state captured before transaction-token cleanup,
   selected-replica log-directory description and caller-ordered alteration outcomes,
   classic static membership plus complete session, rebalance, heartbeat, and
   rejoin timing, and caller-ordered static-member removal after owner
   abandonment,
   consumer-only and generic all-group listings with exact state and group-type
   filters plus generic protocol-type filtering, plural offset mutations,
   dedicated classic-group descriptions with requested authorization bitfields,
   caller-ordered plural empty classic-group deletion, singleton and
   caller-ordered plural active Share-group state, rack, assignment, and
   requested authorization descriptions,
   singleton and caller-ordered plural selected Share-group offset listings,
   empty-group alteration, topic-wide offset
   deletion, and caller-ordered empty-group deletion, literal ACL
   create/describe/delete operations, named-user client-quota
   set/describe/remove operations, and named-user SCRAM-SHA-256/512
   upsert/describe/delete operations as one bounded public call with exact
   non-secret outcomes;
11. explicitly settles flush, close, client shutdown, exact group-owner
   abandonment, and clone-shared hosted group shutdown through public
   event-stream termination;
12. exposes the exact packaged version in its descriptor and subject metadata.

Do not copy the model-broker client into the production adapter. The reference
adapter is a harness fixture, not a Kafka implementation template.
