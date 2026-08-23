//! Compose disruption tests pin broker targeting, operation identity, and recovery evidence.

use std::time::Duration;

use testlab_schema::{EnvironmentOperation, EnvironmentOperationKind};

use crate::compose_test_fixture::Fixture;

#[test]
fn broker_restart_is_followed_by_distinct_readiness_evidence() {
    let fixture = Fixture::new(false);
    let mut environment = fixture.environment();
    let setup = environment.start(Duration::from_secs(2));

    let restart = environment.restart_broker(1, Duration::from_secs(2));
    let cleanup = environment.finish(Duration::from_secs(2));

    assert!(setup.succeeded(), "setup failure: {:?}", setup.failure);
    assert!(
        restart.succeeded(),
        "restart failure: {:?}",
        restart.failure
    );
    assert!(
        cleanup.succeeded(),
        "cleanup failure: {:?}",
        cleanup.failure
    );
    assert_eq!(
        restart
            .operations
            .iter()
            .map(|operation| operation.kind)
            .collect::<Vec<_>>(),
        vec![
            EnvironmentOperationKind::BrokerRestart,
            EnvironmentOperationKind::Readiness,
        ]
    );
    let all = setup
        .operations
        .iter()
        .chain(&restart.operations)
        .chain(&cleanup.operations)
        .collect::<Vec<_>>();
    assert_unique_operation_ids(&all);
    assert!(
        restart
            .artifacts
            .iter()
            .all(|artifact| artifact.name.starts_with("broker-restart-"))
    );
    assert!(fixture.log().contains("restart --no-deps broker"));
}

#[test]
fn undeclared_broker_ordinal_fails_without_a_terminal_operation() {
    let fixture = Fixture::new(false);
    let mut environment = fixture.environment();

    let restart = environment.restart_broker(2, Duration::from_secs(1));

    assert_eq!(
        restart.failure.as_ref().map(crate::ComposeFailure::code),
        Some("environment_broker_target_invalid")
    );
    assert!(restart.operations.is_empty());
}

fn assert_unique_operation_ids(operations: &[&EnvironmentOperation]) {
    for (index, operation) in operations.iter().enumerate() {
        assert!(
            operations[..index]
                .iter()
                .all(|prior| prior.id != operation.id)
        );
    }
}
