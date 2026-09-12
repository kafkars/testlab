//! Share acknowledgement method tests reject adapter substitution.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, Capability, ConsumerId, OperationId, SCENARIO_SCHEMA_VERSION,
    Scenario, ScenarioAction, ScenarioId, ShareAcknowledgementMethod, ShareDisposition,
};

use crate::index::HistoryIndex;
use crate::share::verify_share;
use crate::verify_fixture::{command, event, step};

#[test]
fn accept_all_requires_the_exact_public_batch_conversion() {
    let (scenario, history) = fixture(ShareAcknowledgementMethod::IntoAcknowledgement);
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify_share(&scenario, &index, &mut violations);

    assert_eq!(violations.len(), 1, "violations: {violations:?}");
    assert_eq!(violations[0].contract_id.as_str(), "SHARE-011");
}

#[test]
fn exact_accept_all_conversion_is_valid() {
    let (scenario, history) = fixture(ShareAcknowledgementMethod::AcceptAll);
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify_share(&scenario, &index, &mut violations);

    assert!(violations.is_empty(), "violations: {violations:?}");
}

fn fixture(
    command_method: ShareAcknowledgementMethod,
) -> (Scenario, Vec<testlab_schema::HistoryEntry>) {
    let receive = id(OperationId::new("receive-all"));
    let acknowledgement = id(OperationId::new("ack-all"));
    let action = ScenarioAction::ShareAcknowledge {
        consumer_id: id(ConsumerId::new("share-1")),
        receive_id: receive.clone(),
        acknowledgement_id: acknowledgement.clone(),
        method: ShareAcknowledgementMethod::AcceptAll,
        dispositions: vec![ShareDisposition::Accept],
        timeout_ms: 500,
    };
    let scenario = Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: id(ScenarioId::new("share.accept-all-method")),
        title: "accept all method".to_owned(),
        description: "accept_all command fixture".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::from([Capability::ShareConsumer]),
        steps: vec![step("ack", action)],
        assertions: Vec::new(),
    };
    let issued = AdapterCommand::ShareAcknowledge {
        consumer_id: id(ConsumerId::new("share-1")),
        receive_id: receive.clone(),
        acknowledgement_id: acknowledgement.clone(),
        method: command_method,
        dispositions: vec![ShareDisposition::Accept],
        timeout_ms: 500,
    };
    let completed = AdapterEvent::ShareAcknowledgementCompleted {
        acknowledgement_id: acknowledgement,
        receive_id: receive,
        dispositions: vec![ShareDisposition::Accept],
        success: true,
        delivery: None,
        code: None,
    };
    (scenario, vec![command(0, issued), event(1, completed)])
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
