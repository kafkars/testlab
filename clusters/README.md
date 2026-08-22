# Cluster environments

This directory will contain pinned Kafka topologies and fault controllers.
Cluster manifests must record Kafka version, image digest, broker configuration,
security mode, topology, and readiness evidence. Floating tags are not release
evidence.

`model-broker.toml` declares the in-process harness self-test environment. It is
not Kafka compatibility evidence.

`apache-kafka/` contains real Apache Kafka environments. Every manifest selects
an immutable image digest and a reviewed Compose topology.
