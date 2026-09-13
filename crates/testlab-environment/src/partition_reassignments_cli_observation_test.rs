//! Partition-reassignment observation tests pin accepted Kafka CLI output.

use testlab_schema::{BrokerStateObservation, OperationId};

#[test]
fn empty_and_active_cli_snapshots_are_exact() {
    let BrokerStateObservation::PartitionReassignments(empty) =
        super::normalize(1, &operation(), b"No partition reassignments found.\n")
            .unwrap_or_else(|error| panic!("normalize empty listing: {error}"))
    else {
        panic!("expected partition reassignment state");
    };
    assert!(empty.reassignments.is_empty());

    let output = b"Current partition reassignments:\nalpha-topic-0: replicas: 1,2. adding: 2. removing: 3.\nzeta-1: replicas: 3,1.\n";
    let BrokerStateObservation::PartitionReassignments(active) =
        super::normalize(2, &operation(), output)
            .unwrap_or_else(|error| panic!("normalize active listing: {error}"))
    else {
        panic!("expected partition reassignment state");
    };
    assert_eq!(active.reassignments.len(), 2);
    assert_eq!(active.reassignments[0].topic, "alpha-topic");
    assert_eq!(active.reassignments[0].partition, 0);
    assert_eq!(active.reassignments[0].replicas, [1, 2]);
    assert_eq!(active.reassignments[0].adding_replicas, [2]);
    assert_eq!(active.reassignments[0].removing_replicas, [3]);
}

#[test]
fn malformed_or_noncanonical_cli_output_is_rejected() {
    assert!(super::normalize(1, &operation(), b"No reassignments\n").is_err());
    assert!(
        super::normalize(
            1,
            &operation(),
            b"Current partition reassignments:\nzeta-0: replicas: 1,2.\nalpha-0: replicas: 2,3.\n",
        )
        .is_err()
    );
}

fn operation() -> OperationId {
    OperationId::new("admin-list-reassignments")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
