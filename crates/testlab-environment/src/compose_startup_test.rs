//! Compose startup tests prove bounded port reassignment without hiding failed effects.

use std::time::Duration;

use testlab_schema::{EnvironmentOperationKind, EnvironmentOperationStatus};

use crate::compose_test_fixture::Fixture;

#[test]
fn host_port_collision_is_retained_then_reassigned_once() {
    let fixture = Fixture::with_port_collision(false);
    let mut environment = fixture.environment();
    let original_endpoint = environment.endpoint();

    let setup = environment.start(Duration::from_secs(2));
    let reassigned_endpoint = environment.endpoint();
    let _cleanup = environment.finish(Duration::from_secs(2));

    assert!(setup.succeeded(), "setup failure: {:?}", setup.failure);
    assert_ne!(original_endpoint, reassigned_endpoint);
    let compose_up = setup
        .operations
        .iter()
        .filter(|operation| operation.kind == EnvironmentOperationKind::ComposeUp)
        .collect::<Vec<_>>();
    assert_eq!(compose_up.len(), 2);
    assert_eq!(compose_up[0].status, EnvironmentOperationStatus::Failed);
    assert_eq!(compose_up[1].status, EnvironmentOperationStatus::Succeeded);
    assert_eq!(
        setup
            .operations
            .iter()
            .filter(|operation| operation.kind == EnvironmentOperationKind::ComposeDown)
            .count(),
        1
    );
    assert!(setup.artifacts.iter().any(|artifact| {
        artifact.name == "compose-port-recovery-config-001.txt" && !artifact.bytes.is_empty()
    }));
}

#[test]
fn repeated_host_port_collision_fails_closed_after_one_reassignment() {
    let fixture = Fixture::with_port_collision(true);
    let mut environment = fixture.environment();

    let setup = environment.start(Duration::from_secs(2));
    let _cleanup = environment.finish(Duration::from_secs(2));

    assert_eq!(
        setup.failure.as_ref().map(crate::ComposeFailure::code),
        Some("environment_compose_up_failed"),
        "setup failure: {:?}; operations: {:?}",
        setup.failure,
        setup.operations,
    );
    assert_eq!(
        setup
            .operations
            .iter()
            .filter(|operation| operation.kind == EnvironmentOperationKind::ComposeUp)
            .count(),
        2
    );
}
