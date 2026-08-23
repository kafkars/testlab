# Roadmap

## Shipped foundation

- External JSON Lines adapters exercise packaged client APIs.
- `testctl` owns deadlines, process supervision, deterministic verdicts, and
  sealed replayable evidence.
- The model broker self-tests the harness without making Kafka compatibility
  claims.
- Pinned Apache Kafka 3.7.2 through 4.3.1 environments cover single- and
  three-broker clusters, TLS, SASL/PLAIN, and SCRAM-SHA-256/512.
- Real-Kafka scenarios cover producing, assigned and group consumption, topic
  administration, transactions, fencing, broker restart, and rolling restart.
- Pull-request and release qualifications package Kafkars first and verify
  broker-visible truth independently through librdkafka.
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

- Add a scripted Kafka protocol adversary for partial frames, wrong correlation
  IDs, stale responses, stalls, disconnect points, and minimized replay cases.
- Add targeted leader, controller, group-coordinator, transaction-coordinator,
  ACL, quota, and network-fault scenarios.
- Add concurrent actors, seeded generation, shrinking, soak, and weekly chaos
  packs without weakening deterministic verdicts.

## Later — additional client surfaces

- Add Java and librdkafka reference adapters over shared public semantics.
- Add a C ABI gauntlet when Kafkars exposes a versioned foreign interface.
- Add deterministic analysis packets and optional narrative summaries that
  cannot alter validity, pass/fail, or release eligibility.

## Release rule

A release requires complete passing evidence for every gating cell. A failed or
invalid cell blocks the claim. Model-broker runs and narrative output never
count as Kafka compatibility evidence.
