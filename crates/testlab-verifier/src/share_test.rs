//! Share verifier tests pin success and failed acknowledgement certainty.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, Capability, ConsumerId, OperationId, SCENARIO_SCHEMA_VERSION,
    Scenario, ScenarioAction, ScenarioId, ShareDisposition, StepId, TerminalStatus,
};

use crate::index::HistoryIndex;
use crate::share::verify_share;
use crate::verify_fixture::{command, event, step};

#[test]
fn share_acknowledgement_requires_a_successful_certain_terminal() {
    let receive = id(OperationId::new("receive-1"));
    let acknowledgement = id(OperationId::new("ack-1"));
    let consumer = id(ConsumerId::new("share-1"));
    let scenario = Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: id(ScenarioId::new("share.verifier")),
        title: "share verifier".to_owned(),
        description: "share acknowledgement fixture".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::from([Capability::ShareConsumer]),
        steps: vec![step(
            "ack",
            ScenarioAction::ShareAcknowledge {
                consumer_id: consumer,
                receive_id: receive.clone(),
                acknowledgement_id: acknowledgement.clone(),
                disposition: ShareDisposition::Accept,
                timeout_ms: 500,
            },
        )],
        assertions: Vec::new(),
    };
    let issued = AdapterCommand::ShareAcknowledge {
        consumer_id: id(ConsumerId::new("share-1")),
        receive_id: receive.clone(),
        acknowledgement_id: acknowledgement.clone(),
        disposition: ShareDisposition::Accept,
        timeout_ms: 500,
    };
    let failed = AdapterEvent::ShareAcknowledgementCompleted {
        acknowledgement_id: acknowledgement,
        receive_id: receive,
        disposition: ShareDisposition::Accept,
        success: false,
        delivery: Some(TerminalStatus::PossiblySent),
        code: Some("transport".to_owned()),
    };
    let index = HistoryIndex::build(&[command(0, issued), event(1, failed)]);
    let mut violations = Vec::new();

    verify_share(&scenario, &index, &mut violations);

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].contract_id.as_str(), "SHARE-003");
}

#[test]
fn successful_share_acknowledgement_is_coherent() {
    let receive = id(OperationId::new("receive-1"));
    let acknowledgement = id(OperationId::new("ack-1"));
    let consumer = id(ConsumerId::new("share-1"));
    let action = ScenarioAction::ShareAcknowledge {
        consumer_id: consumer.clone(),
        receive_id: receive.clone(),
        acknowledgement_id: acknowledgement.clone(),
        disposition: ShareDisposition::Release,
        timeout_ms: 500,
    };
    let scenario = Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: id(ScenarioId::new("share.verifier-success")),
        title: "share verifier".to_owned(),
        description: "successful share acknowledgement fixture".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::from([Capability::ShareConsumer]),
        steps: vec![testlab_schema::ScenarioStep {
            id: id(StepId::new("ack")),
            action: action.clone(),
        }],
        assertions: Vec::new(),
    };
    let consumer_id = consumer;
    let receive_id = receive;
    let acknowledgement_id = acknowledgement;
    let disposition = ShareDisposition::Release;
    let timeout_ms = 500;
    let history = [
        command(
            0,
            AdapterCommand::ShareAcknowledge {
                consumer_id,
                receive_id: receive_id.clone(),
                acknowledgement_id: acknowledgement_id.clone(),
                disposition,
                timeout_ms,
            },
        ),
        event(
            1,
            AdapterEvent::ShareAcknowledgementCompleted {
                acknowledgement_id,
                receive_id,
                disposition,
                success: true,
                delivery: None,
                code: None,
            },
        ),
    ];
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify_share(&scenario, &index, &mut violations);

    assert!(violations.is_empty(), "violations: {violations:?}");
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
