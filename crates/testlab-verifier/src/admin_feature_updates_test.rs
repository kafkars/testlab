//! Feature-update verification pins ordered success and immutable CLI state.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminFeatureUpdateOutcome, AdminFeatureUpdatesValidation,
    BrokerFeatureState, BrokerFeaturesState, BrokerStateObservation, FeatureUpdateKind,
    FeatureUpdateSpec, HistoryEntry, HistoryPayload, OperationId, ScenarioAction,
    ValidateFeatureUpdatesAction, ValidateFeatureUpdatesCommand,
};

use crate::admin_feature_updates::verify;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_validation_only_update_and_unchanged_state_pass() {
    assert!(violations(history()).is_empty());
}

#[test]
fn per_feature_failure_or_feature_level_change_fails() {
    let mut failed = history();
    let HistoryPayload::AdapterEvent { event } = &mut failed[2].payload else {
        panic!("public update event");
    };
    let AdapterEvent::FeatureUpdatesValidated(public) = &mut event.event else {
        panic!("public feature validation");
    };
    public.outcomes[0].error_code = Some("invalid_update_version".to_owned());
    assert_contract(&violations(failed));

    let mut changed = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut changed[3].payload else {
        panic!("post-update state");
    };
    let BrokerStateObservation::Features(after) = observation else {
        panic!("post-update features");
    };
    after.features[0].finalized_version_level = 29;
    assert_contract(&violations(changed));
}

#[test]
fn unrelated_metadata_epoch_advance_does_not_invent_a_feature_change() {
    let mut advanced = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut advanced[3].payload else {
        panic!("post-update state");
    };
    let BrokerStateObservation::Features(after) = observation else {
        panic!("post-update features");
    };
    after.finalized_features_epoch = Some(8);

    assert!(violations(advanced).is_empty());
}

#[test]
fn independent_state_must_immediately_follow_public_completion() {
    let mut delayed = history();
    delayed.insert(3, command(3, AdapterCommand::Finish));
    delayed[4].sequence = 4;
    delayed[4].observed_unix_ms = 4;
    assert_contract(&violations(delayed));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        state(0, baseline_operation()),
        command(
            1,
            AdapterCommand::ValidateFeatureUpdates(ValidateFeatureUpdatesCommand {
                client_id: client(),
                operation_id: operation(),
                updates: updates(),
                timeout_ms: 1_000,
            }),
        ),
        event(
            2,
            AdapterEvent::FeatureUpdatesValidated(AdminFeatureUpdatesValidation {
                operation_id: operation(),
                throttle_time_ms: 0,
                outcomes: updates()
                    .into_iter()
                    .map(|update| AdminFeatureUpdateOutcome {
                        name: update.name,
                        error_code: None,
                    })
                    .collect(),
            }),
        ),
        state(3, operation()),
    ]
}

fn state(sequence: u64, operation_id: OperationId) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::Features(BrokerFeaturesState {
                observation: sequence + 10,
                operation_id,
                features: vec![BrokerFeatureState {
                    name: "metadata.version".to_owned(),
                    supported_min_version_level: 4,
                    supported_max_version_level: 30,
                    finalized_version_level: 30,
                }],
                finalized_features_epoch: Some(7),
            }),
        },
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the test helper owns each constructed history fixture"
)]
fn violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let action = ScenarioAction::ValidateFeatureUpdates(ValidateFeatureUpdatesAction {
        client_id: client(),
        operation_id: operation(),
        baseline_operation_id: baseline_operation(),
        updates: updates(),
        timeout_ms: 1_000,
    });
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    assert!(verify(&action, &index, &mut violations));
    violations
}

fn updates() -> Vec<FeatureUpdateSpec> {
    vec![FeatureUpdateSpec {
        name: "metadata.version".to_owned(),
        max_version_level: 30,
        kind: FeatureUpdateKind::Upgrade,
    }]
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-validate-feature-updates")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn baseline_operation() -> OperationId {
    OperationId::new("admin-describe-features-before")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-072"),
        "{violations:?}"
    );
}
