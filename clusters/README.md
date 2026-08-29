# Cluster environments

This directory will contain pinned Kafka topologies and fault controllers.
Cluster manifests must record Kafka version, image digest, broker configuration,
security mode, topology, and readiness evidence. Floating tags are not release
evidence.

`model-broker.toml` declares the in-process harness self-test environment. It is
not Kafka compatibility evidence.

`protocol-adversary.toml` declares the external scripted Kafka peer. It is
failure-semantics evidence for packaged clients, not evidence of compatibility
with a Kafka broker release. Its independently recorded wire observations and
terminal process operation are required for a valid run.

`apache-kafka/` contains real Apache Kafka environments. Every manifest selects
an immutable image digest and a reviewed Compose topology.

`packs/kafkars-broker-role-failover.toml` targets independently discovered
partition leaders, controllers, classic and KIP-848 group coordinators, and
transaction coordinators on the three-broker plaintext topology. Partition
leader discovery uses librdkafka and remains security-profile aware. The
bounded Metadata and FindCoordinator probes used for controller and coordinator
selection are intentionally plaintext-only; those scenarios do not claim TLS
or SASL role-targeting coverage.

`apache-kafka/4.3.1/single-sasl-plain-policy.toml` enables the standard Kafka
authorizer on a SASL/PLAIN external listener. The packaged client remains the
fixed `User:kafkars` principal while environment controls use the internal
anonymous superuser listener. `packs/kafkars-broker-policy.toml` pairs each
literal deny ACL or user quota with an independently queried removal.

`apache-kafka/4.3.1/single-plaintext-network.toml` separates the packaged
client's proxy listener, a hidden broker upstream listener, and the direct
observer listener. `packs/kafkars-network-faults.toml` covers a live connection
cut, a bidirectional blackhole, and both one-way delay directions without
granting the proxy or adapter ownership of broker-visible truth.
