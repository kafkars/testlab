# kafkars Rust adapter handoff

This adapter resolves `kafkars = 0.0.2-rc.1` from public revision
`e556a2f04df855c242b5befb7feceb74e0ed4426` and uses only its curated public
module facade. It implements producer, assigned/group/share consumer, admin,
transaction, and lifecycle commands; Kafka environment control and broker
observation remain testlab-owned.

It:

1. depends only on the packaged public `kafkars` surface;
2. implements protocol v34 over stdin/stdout;
3. preserves admission rejection separately from accepted delivery;
4. maps client outcomes to acknowledged, definitely-not-sent, or possibly-sent
   without inventing certainty;
5. preserves caller order and exact per-resource public outcomes for admin
   batches, including mixed success and failure;
6. maps singleton public admin errors to stable normalized `command_failed`
   events without receiving scenario expectations;
7. forwards validate-only topic creation, partition increase, and incremental
   configuration changes through the packaged public builders;
8. executes selected multi-partition and multi-group offset reads, plural offset
   mutation batches, and dedicated classic-group descriptions as one bounded
   public call with caller-ordered per-resource outcomes;
9. explicitly settles flush, close, client shutdown, and clone-shared hosted
   group shutdown through public event-stream termination;
10. exposes the exact packaged version in its descriptor and subject metadata.

Do not copy the model-broker client into the production adapter. The reference
adapter is a harness fixture, not a Kafka implementation template.
