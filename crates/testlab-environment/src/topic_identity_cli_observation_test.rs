//! Tests for the topic identity cli observation contract.

use super::*;

const OUTPUT: &str = "\
Topic: orders\tTopicId: AAECAwQFBgcICQoLDA0ODw\tPartitionCount: 2\tReplicationFactor: 1\tConfigs: min.insync.replicas=1\n\
\tTopic: orders\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\n\
\tTopic: orders\tPartition: 1\tLeader: 1\tReplicas: 1\tIsr: 1\n";

#[test]
fn exact_topic_identity_and_partition_rows_normalize() {
    let state = normalize(7, &operation(), "orders", OUTPUT.as_bytes())
        .unwrap_or_else(|error| panic!("normalize topic identity: {error}"));
    let BrokerStateObservation::TopicIdentity(state) = state else {
        panic!("topic identity state");
    };
    assert_eq!(state.observation, 7);
    assert_eq!(
        state.topic_id,
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
    );
    assert_eq!(state.partitions, vec![0, 1]);
}

#[test]
fn mismatched_topic_zero_id_and_duplicate_partition_fail() {
    assert!(normalize(7, &operation(), "audit", OUTPUT.as_bytes()).is_err());
    let zero = OUTPUT.replace("AAECAwQFBgcICQoLDA0ODw", "AAAAAAAAAAAAAAAAAAAAAA");
    assert!(normalize(7, &operation(), "orders", zero.as_bytes()).is_err());
    let duplicate = OUTPUT.replace("Partition: 1", "Partition: 0");
    assert!(normalize(7, &operation(), "orders", duplicate.as_bytes()).is_err());
}

fn operation() -> OperationId {
    OperationId::new("describe-topics-by-id").unwrap_or_else(|error| panic!("operation: {error}"))
}
