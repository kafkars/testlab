//! Compose image acquisition tests pin cache use, bounded retry, and invalidity identity.

use std::time::Duration;

use testlab_schema::{EnvironmentOperationKind, EnvironmentOperationStatus};

use crate::compose_test_fixture::Fixture;

#[test]
fn image_pull_retries_twice_then_reuses_the_verified_cache() {
    let fixture = Fixture::with_image_pull_failures(2);
    let mut first = fixture.environment();

    let setup = first.start(Duration::from_secs(2));
    let _cleanup = first.finish(Duration::from_secs(2));

    assert!(setup.succeeded(), "setup failure: {:?}", setup.failure);
    let pulls = setup
        .operations
        .iter()
        .filter(|operation| operation.kind == EnvironmentOperationKind::ImagePull)
        .collect::<Vec<_>>();
    assert_eq!(pulls.len(), 3);
    assert_eq!(pulls[0].status, EnvironmentOperationStatus::Failed);
    assert_eq!(pulls[1].status, EnvironmentOperationStatus::Failed);
    assert_eq!(pulls[2].status, EnvironmentOperationStatus::Succeeded);
    for name in [
        "image-pull.txt",
        "image-pull-002.txt",
        "image-pull-003.txt",
        "image-inspect-after-pull.json",
    ] {
        assert!(setup.artifacts.iter().any(|artifact| artifact.name == name));
    }

    let mut cached = fixture.environment();
    let cached_setup = cached.start(Duration::from_secs(2));
    let _cleanup = cached.finish(Duration::from_secs(2));
    assert!(cached_setup.succeeded());
    assert!(
        cached_setup
            .operations
            .iter()
            .all(|operation| operation.kind != EnvironmentOperationKind::ImagePull)
    );
}

#[test]
fn exhausted_image_pull_is_an_environment_failure_before_compose_start() {
    let fixture = Fixture::with_image_pull_failures(3);
    let mut environment = fixture.environment();

    let setup = environment.start(Duration::from_secs(2));

    assert_eq!(
        setup.failure.as_ref().map(crate::ComposeFailure::code),
        Some("environment_image_pull_failed")
    );
    assert_eq!(
        setup
            .operations
            .iter()
            .filter(|operation| operation.kind == EnvironmentOperationKind::ImagePull)
            .count(),
        3
    );
    assert!(
        setup
            .operations
            .iter()
            .all(|operation| operation.kind != EnvironmentOperationKind::ComposeUp)
    );
}
