# kafkars Rust adapter handoff

This directory is reserved for the first real subject adapter.

It should:

1. depend only on the packaged public `kafka-client` surface;
2. implement protocol v1 over stdin/stdout;
3. preserve admission rejection separately from accepted delivery;
4. map client outcomes to acknowledged, definitely-not-sent, or possibly-sent
   without inventing certainty;
5. explicitly settle flush, close, and client shutdown;
6. expose exact build identity in its descriptor or subject metadata;
7. pass the existing reference pack before adding kafkars-specific scenarios.

Do not copy the model-broker client into the production adapter. The reference
adapter is a harness fixture, not a Kafka implementation template.
