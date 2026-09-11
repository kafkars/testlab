//! Broker-role controls fail before effects when topology ownership is insufficient.

use std::time::Duration;

use testlab_schema::BrokerRoleTarget;

use crate::compose_test_fixture::Fixture;

#[test]
fn single_broker_cannot_claim_a_replacement_leader() {
    let fixture = Fixture::new(false);
    let mut environment = fixture.environment();

    let phase = environment.stop_broker_role(
        &BrokerRoleTarget::PartitionLeader {
            topic: "records".to_owned(),
            partition: 0,
        },
        Duration::from_secs(1),
    );

    assert_eq!(
        phase.failure.as_ref().map(crate::ComposeFailure::code),
        Some("environment_role_disruption_invalid")
    );
    assert!(phase.operations.is_empty());
}

#[test]
fn restore_requires_an_exact_retained_partition_owner() {
    let fixture = Fixture::new(false);
    let mut environment = fixture.environment();

    let phase = environment.restore_broker_role(
        &BrokerRoleTarget::PartitionLeader {
            topic: "records".to_owned(),
            partition: 0,
        },
        Duration::from_secs(1),
    );

    assert_eq!(
        phase.failure.as_ref().map(crate::ComposeFailure::code),
        Some("environment_role_disruption_invalid")
    );
    assert!(phase.operations.is_empty());
}

#[test]
fn restored_partition_requires_a_member_leader_and_full_isr() {
    use crate::compose_broker_role_readiness::partition_replica_ready;

    assert!(partition_replica_ready(2, &[1, 2, 3], &[3, 1, 2], 1));
    assert!(!partition_replica_ready(2, &[1, 2, 3], &[2, 3], 1));
    assert!(!partition_replica_ready(4, &[1, 2, 3], &[1, 2, 3], 1));
    assert!(!partition_replica_ready(2, &[1, 2, 3], &[1, 2, 3], 4));
}
