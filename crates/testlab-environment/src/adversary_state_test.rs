//! Adversary state tests cover ordered selection, bounded replay, and fail-closed controls.

use testlab_schema::{EnvironmentOperationId, KafkaApi, ProtocolFault, ProtocolFaultAction};

use crate::adversary_state::AdversaryState;

#[test]
fn applications_are_consumed_only_by_matching_api_in_order() {
    let mut state = AdversaryState::default();
    state
        .arm(control("metadata-fault", KafkaApi::Metadata, 2))
        .unwrap_or_else(|error| panic!("arm control: {error}"));

    assert!(state.select(KafkaApi::Produce).is_none());
    assert_eq!(
        state
            .select(KafkaApi::Metadata)
            .map(|selected| selected.operation_id.to_string()),
        Some("metadata-fault".to_owned())
    );
    assert!(state.select(KafkaApi::Metadata).is_some());
    assert!(state.select(KafkaApi::Metadata).is_none());
    assert!(state.unconsumed_controls().is_empty());
}

#[test]
fn stale_replay_selects_a_different_api_and_retention_is_bounded() {
    let mut state = AdversaryState::default();
    state.retain_response(KafkaApi::ApiVersions, vec![1]);
    state.retain_response(KafkaApi::Metadata, vec![2]);

    assert_eq!(state.stale_response(KafkaApi::Metadata), Some(vec![1]));
    assert_eq!(state.stale_response(KafkaApi::ApiVersions), Some(vec![2]));

    for byte in 3..=11 {
        state.retain_response(KafkaApi::Metadata, vec![byte]);
    }
    assert_eq!(state.stale_response(KafkaApi::Metadata), None);
}

#[test]
fn duplicate_identity_and_unconsumed_control_fail_closed() {
    let mut state = AdversaryState::default();
    let control = control("duplicate", KafkaApi::Produce, 1);
    state
        .arm(control.clone())
        .unwrap_or_else(|error| panic!("arm first control: {error}"));
    assert!(state.arm(control).is_err());
    assert_eq!(
        state
            .unconsumed_controls()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec!["duplicate"]
    );
}

fn control(id: &str, api: KafkaApi, applications: u16) -> ProtocolFaultAction {
    ProtocolFaultAction {
        operation_id: EnvironmentOperationId::new(id)
            .unwrap_or_else(|error| panic!("operation id: {error}")),
        api,
        applications,
        fault: ProtocolFault::StaleResponse,
    }
}
