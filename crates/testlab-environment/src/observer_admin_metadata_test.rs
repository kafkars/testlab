//! Metadata readiness tests keep fenced expectations out of adapter evidence.

use super::active_cluster_matches;

#[test]
fn full_cluster_requires_the_declared_broker_count() {
    assert!(active_cluster_matches(&[1, 2, 3], 3, &[]));
    assert!(!active_cluster_matches(&[1, 2], 3, &[]));
}

#[test]
fn fenced_expectation_selects_the_active_broker_snapshot() {
    assert!(active_cluster_matches(&[1, 2], 2, &[3]));
    assert!(!active_cluster_matches(&[1, 3], 2, &[3]));
    assert!(!active_cluster_matches(&[1], 2, &[3]));
}
