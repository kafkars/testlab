//! Share verifier tests pin success and failed acknowledgement certainty.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, Capability, ConsumedRecord, ConsumerId, OperationId,
    SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId, ShareConsumedRecord,
    ShareDisposition, StepId, TerminalStatus,
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
                dispositions: vec![ShareDisposition::Accept],
                timeout_ms: 500,
            },
        )],
        assertions: Vec::new(),
    };
    let issued = AdapterCommand::ShareAcknowledge {
        consumer_id: id(ConsumerId::new("share-1")),
        receive_id: receive.clone(),
        acknowledgement_id: acknowledgement.clone(),
        dispositions: vec![ShareDisposition::Accept],
        timeout_ms: 500,
    };
    let failed = AdapterEvent::ShareAcknowledgementCompleted {
        acknowledgement_id: acknowledgement,
        receive_id: receive,
        dispositions: vec![ShareDisposition::Accept],
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
        dispositions: vec![ShareDisposition::Release],
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
    let dispositions = vec![ShareDisposition::Release];
    let timeout_ms = 500;
    let history = [
        command(
            0,
            AdapterCommand::ShareAcknowledge {
                consumer_id,
                receive_id: receive_id.clone(),
                acknowledgement_id: acknowledgement_id.clone(),
                dispositions: dispositions.clone(),
                timeout_ms,
            },
        ),
        event(
            1,
            AdapterEvent::ShareAcknowledgementCompleted {
                acknowledgement_id,
                receive_id,
                dispositions,
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

#[test]
fn mixed_share_acknowledgement_preserves_record_order() {
    let receive = id(OperationId::new("receive-mixed"));
    let acknowledgement = id(OperationId::new("ack-mixed"));
    let consumer = id(ConsumerId::new("share-1"));
    let expected = vec![
        ShareDisposition::Accept,
        ShareDisposition::Release,
        ShareDisposition::Reject,
    ];
    let action = ScenarioAction::ShareAcknowledge {
        consumer_id: consumer.clone(),
        receive_id: receive.clone(),
        acknowledgement_id: acknowledgement.clone(),
        dispositions: expected.clone(),
        timeout_ms: 500,
    };
    let scenario = Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: id(ScenarioId::new("share.mixed-order")),
        title: "mixed order".to_owned(),
        description: "mixed Share decision fixture".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::from([Capability::ShareConsumer]),
        steps: vec![step("ack", action)],
        assertions: Vec::new(),
    };
    let issued = AdapterCommand::ShareAcknowledge {
        consumer_id: consumer,
        receive_id: receive.clone(),
        acknowledgement_id: acknowledgement.clone(),
        dispositions: expected,
        timeout_ms: 500,
    };
    let adapter_event = AdapterEvent::ShareAcknowledgementCompleted {
        acknowledgement_id: acknowledgement,
        receive_id: receive,
        dispositions: vec![
            ShareDisposition::Release,
            ShareDisposition::Accept,
            ShareDisposition::Reject,
        ],
        success: true,
        delivery: None,
        code: None,
    };
    let index = HistoryIndex::build(&[command(0, issued), event(1, adapter_event)]);
    let mut violations = Vec::new();

    verify_share(&scenario, &index, &mut violations);

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].contract_id.as_str(), "SHARE-008");
}

#[test]
fn configured_share_receive_requires_the_exact_public_acquisition_count() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/share-group-fetch-max-records.toml"
    ))
    .unwrap_or_else(|error| panic!("parse configured Share scenario: {error}"));
    let (consumer_id, receive_id, timeout_ms) = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::ShareReceive {
                consumer_id,
                receive_id,
                timeout_ms,
                ..
            } if receive_id.as_str() == "receive-first" => {
                Some((consumer_id.clone(), receive_id.clone(), *timeout_ms))
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("first configured Share receive missing"));
    let record = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::Send {
                operation_id,
                record,
                ..
            } if operation_id.as_str() == "op-first" => Some(record.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("first configured Share record missing"));
    let history = [
        command(
            0,
            AdapterCommand::ShareReceive {
                consumer_id: consumer_id.clone(),
                receive_id: receive_id.clone(),
                timeout_ms,
            },
        ),
        event(
            1,
            AdapterEvent::ShareReceiveCompleted {
                consumer_id,
                receive_id,
                records: vec![ShareConsumedRecord {
                    record: ConsumedRecord {
                        topic: record.topic,
                        partition: record.partition,
                        offset: 0,
                        timestamp_millis: None,
                        key: record.key,
                        value: record.value,
                        headers: record.headers,
                    },
                    delivery_count: 1,
                }],
                acquisition_count: 2,
                member_epoch: Some(1),
                assignment_epoch: Some(1),
            },
        ),
    ];
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify_share(&scenario, &index, &mut violations);

    assert_eq!(violations.len(), 1, "violations: {violations:?}");
    assert_eq!(violations[0].contract_id.as_str(), "SHARE-010");
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
