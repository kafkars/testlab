//! Share step tests distinguish missing deliveries from invalid harness execution.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterEvent, ByteString, Capability, ConsumedRecord, ConsumerId, OperationId, RecordSpec,
    Scenario, ScenarioAction, ScenarioId, ScenarioStep, ShareConsumedRecord, StepId,
};

#[test]
fn empty_or_wrong_share_receive_stops_before_dependent_acknowledgement() {
    let (scenario, action, receive, consumer, expected) = fixture();
    let empty = AdapterEvent::ShareReceiveCompleted {
        consumer_id: consumer.clone(),
        receive_id: receive.clone(),
        records: Vec::new(),
        acquisition_count: 0,
        member_epoch: Some(1),
        assignment_epoch: Some(1),
    };
    assert_eq!(
        super::session_share::receive_succeeded(&scenario, &action, &empty),
        Some(false)
    );
    let wrong = AdapterEvent::ShareReceiveCompleted {
        consumer_id: consumer,
        receive_id: receive,
        records: vec![ShareConsumedRecord {
            record: ConsumedRecord {
                topic: expected.topic,
                partition: expected.partition,
                offset: 0,
                timestamp_millis: None,
                key: expected.key,
                value: Some(ByteString::utf8("wrong")),
                headers: expected.headers,
            },
            delivery_count: 2,
        }],
        acquisition_count: 1,
        member_epoch: Some(1),
        assignment_epoch: Some(1),
    };
    assert_eq!(
        super::session_share::receive_succeeded(&scenario, &action, &wrong),
        Some(false)
    );
}

#[test]
fn one_exact_share_receive_with_minimum_delivery_count_continues() {
    let (scenario, action, receive, consumer, expected) = fixture();
    let event = AdapterEvent::ShareReceiveCompleted {
        consumer_id: consumer,
        receive_id: receive,
        records: vec![ShareConsumedRecord {
            record: ConsumedRecord {
                topic: expected.topic,
                partition: expected.partition,
                offset: 9,
                timestamp_millis: None,
                key: expected.key,
                value: expected.value,
                headers: expected.headers,
            },
            delivery_count: 2,
        }],
        acquisition_count: 1,
        member_epoch: Some(3),
        assignment_epoch: Some(4),
    };
    assert_eq!(
        super::session_share::receive_succeeded(&scenario, &action, &event),
        Some(true)
    );
}

#[test]
fn share_receive_stops_on_wrong_public_acquisition_count() {
    let (scenario, mut action, receive, consumer, expected) = fixture();
    let ScenarioAction::ShareReceive {
        expected_acquisition_count,
        ..
    } = &mut action
    else {
        panic!("share receive fixture missing");
    };
    *expected_acquisition_count = Some(1);
    let event = AdapterEvent::ShareReceiveCompleted {
        consumer_id: consumer,
        receive_id: receive,
        records: vec![ShareConsumedRecord {
            record: ConsumedRecord {
                topic: expected.topic,
                partition: expected.partition,
                offset: 9,
                timestamp_millis: None,
                key: expected.key,
                value: expected.value,
                headers: expected.headers,
            },
            delivery_count: 2,
        }],
        acquisition_count: 2,
        member_epoch: Some(3),
        assignment_epoch: Some(4),
    };

    assert_eq!(
        super::session_share::receive_succeeded(&scenario, &action, &event),
        Some(false)
    );
}

#[test]
fn ordered_multi_record_share_receive_continues_only_for_the_exact_batch() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/share-group-mixed-release.toml"
    ))
    .unwrap_or_else(|error| panic!("parse mixed Share scenario: {error}"));
    let action = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            action @ ScenarioAction::ShareReceive { receive_id, .. }
                if receive_id.as_str() == "receive-initial" =>
            {
                Some(action.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("initial Share receive missing"));
    let operations = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::SendBatch { operations, .. } => Some(operations),
            _ => None,
        })
        .unwrap_or_else(|| panic!("Share batch send missing"));
    let mut records = operations
        .iter()
        .enumerate()
        .map(|(offset, operation)| ShareConsumedRecord {
            record: ConsumedRecord {
                topic: operation.record.topic.clone(),
                partition: operation.record.partition,
                offset: i64::try_from(offset).unwrap_or_else(|error| panic!("offset: {error}")),
                timestamp_millis: None,
                key: operation.record.key.clone(),
                value: operation.record.value.clone(),
                headers: operation.record.headers.clone(),
            },
            delivery_count: 1,
        })
        .collect::<Vec<_>>();
    let event = |records| AdapterEvent::ShareReceiveCompleted {
        consumer_id: id(ConsumerId::new("share-1")),
        receive_id: id(OperationId::new("receive-initial")),
        records,
        acquisition_count: 1,
        member_epoch: Some(1),
        assignment_epoch: Some(1),
    };

    assert_eq!(
        super::session_share::receive_succeeded(&scenario, &action, &event(records.clone())),
        Some(true)
    );
    records.swap(0, 1);
    assert_eq!(
        super::session_share::receive_succeeded(&scenario, &action, &event(records)),
        Some(false)
    );
}

fn fixture() -> (
    Scenario,
    ScenarioAction,
    OperationId,
    ConsumerId,
    RecordSpec,
) {
    let operation = id(OperationId::new("op-held"));
    let receive = id(OperationId::new("receive-held"));
    let consumer = id(ConsumerId::new("share-1"));
    let expected = RecordSpec {
        topic: "jobs".to_owned(),
        partition: 0,
        sequence: 1,
        key: Some(ByteString::utf8("key")),
        value: Some(ByteString::utf8("value")),
        headers: Vec::new(),
    };
    let send = ScenarioAction::Send {
        producer_id: id(testlab_schema::ProducerId::new("producer-1")),
        operation_id: operation.clone(),
        record: expected.clone(),
    };
    let action = ScenarioAction::ShareReceive {
        consumer_id: consumer.clone(),
        receive_id: receive.clone(),
        expected_operation_ids: vec![operation],
        minimum_delivery_count: 2,
        expected_acquisition_count: None,
        timeout_ms: 30_000,
    };
    let scenario = Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: id(ScenarioId::new("test.share-receive")),
        title: "share receive".to_owned(),
        description: "fixture".to_owned(),
        timeout_ms: 60_000,
        requires: BTreeSet::<Capability>::new(),
        steps: vec![
            ScenarioStep {
                id: id(StepId::new("send")),
                action: send,
            },
            ScenarioStep {
                id: id(StepId::new("receive")),
                action: action.clone(),
            },
        ],
        assertions: Vec::new(),
    };
    (scenario, action, receive, consumer, expected)
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
