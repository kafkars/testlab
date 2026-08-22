# kafkars Rust adapter handoff

This adapter depends on the exact published `kafkars = 0.0.1` package and uses
only its public facade. It implements producer and lifecycle protocol commands;
Kafka environment control and broker observation remain testlab-owned.

It:

1. depends only on the packaged public `kafkars` surface;
2. implements protocol v7 over stdin/stdout;
3. preserves admission rejection separately from accepted delivery;
4. maps client outcomes to acknowledged, definitely-not-sent, or possibly-sent
   without inventing certainty;
5. explicitly settles flush, close, and client shutdown;
6. exposes the exact packaged version in its descriptor and subject metadata.

Do not copy the model-broker client into the production adapter. The reference
adapter is a harness fixture, not a Kafka implementation template.
