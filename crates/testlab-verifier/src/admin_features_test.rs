//! Feature verdict tests pin completeness, ordering, flags, and independent CLI equality.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminFeaturesDescription, BrokerFeatureState,
    BrokerFeaturesState, BrokerStateObservation, DescribeFeaturesAction, DescribeFeaturesCommand,
    FeatureVersionRange, HistoryEntry, HistoryPayload, OperationId, ScenarioAction, TerminalStatus,
    VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn complete_and_legacy_incomplete_feature_snapshots_pass() {
    assert!(violations(history(true, true, 7, false), false).is_empty());
    assert!(violations(history(false, false, 7, false), false).is_empty());
}

#[test]
fn completeness_epoch_and_migration_mismatches_fail() {
    for (history, expected_migration) in [
        (history(false, true, 7, false), false),
        (history(true, true, 8, false), false),
        (history(true, true, 7, true), false),
    ] {
        assert_contract(&violations(history, expected_migration));
    }
}

#[test]
fn noncanonical_or_incorrect_feature_levels_fail() {
    let mut wrong = history(true, true, 7, false);
    let HistoryPayload::AdapterEvent { event } = &mut wrong[1].payload else {
        panic!("public feature event");
    };
    let AdapterEvent::FeaturesDescribed(public) = &mut event.event else {
        panic!("public feature description");
    };
    public.supported_features[1].max_version_level = 29;
    assert_contract(&violations(wrong, false));

    let mut reversed = history(true, true, 7, false);
    let HistoryPayload::BrokerStateObservation { observation } = &mut reversed[2].payload else {
        panic!("independent feature state");
    };
    let BrokerStateObservation::Features(independent) = observation else {
        panic!("independent feature description");
    };
    independent.features.reverse();
    assert_contract(&violations(reversed, false));
}

#[test]
fn independent_snapshot_must_immediately_follow_the_public_result() {
    let mut delayed = history(true, true, 7, false);
    delayed.insert(2, command(2, AdapterCommand::Finish));
    delayed[3].sequence = 3;
    delayed[3].observed_unix_ms = 3;
    assert_contract(&violations(delayed, false));
}

fn history(
    include_zero_minimum: bool,
    complete: bool,
    public_epoch: i64,
    public_migration: bool,
) -> Vec<HistoryEntry> {
    let operation_id = operation();
    let mut supported = vec![range("metadata.version", 4, 30)];
    if include_zero_minimum {
        supported.insert(0, range("eligible.leader.replicas.version", 0, 1));
    }
    vec![
        command(
            0,
            AdapterCommand::DescribeFeatures(DescribeFeaturesCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::FeaturesDescribed(AdminFeaturesDescription {
                operation_id: operation_id.clone(),
                supported_features: supported,
                supported_features_complete: complete,
                finalized_features_epoch: Some(public_epoch),
                finalized_features: vec![range("metadata.version", 30, 30)],
                zk_migration_ready: public_migration,
            }),
        ),
        state(
            2,
            BrokerStateObservation::Features(BrokerFeaturesState {
                observation: 4,
                operation_id,
                features: vec![
                    BrokerFeatureState {
                        name: "eligible.leader.replicas.version".to_owned(),
                        supported_min_version_level: 0,
                        supported_max_version_level: 1,
                        finalized_version_level: 0,
                    },
                    BrokerFeatureState {
                        name: "metadata.version".to_owned(),
                        supported_min_version_level: 4,
                        supported_max_version_level: 30,
                        finalized_version_level: 30,
                    },
                ],
                finalized_features_epoch: Some(7),
            }),
        ),
    ]
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the test helper owns each constructed history fixture"
)]
fn violations(
    history: Vec<HistoryEntry>,
    expected_migration: bool,
) -> Vec<testlab_schema::Violation> {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps.insert(
        2,
        step(
            "describe-features",
            ScenarioAction::DescribeFeatures(DescribeFeaturesAction {
                client_id: client(),
                operation_id: operation(),
                expected_zk_migration_ready: expected_migration,
                timeout_ms: 1_000,
            }),
        ),
    );
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
}

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn range(name: &str, min_version_level: i16, max_version_level: i16) -> FeatureVersionRange {
    FeatureVersionRange {
        name: name.to_owned(),
        min_version_level,
        max_version_level,
    }
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-describe-features-1")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-050"),
        "{violations:?}"
    );
}
