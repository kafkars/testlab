//! Fetch-evidence verdict tests distinguish complete broker binding from facade claims.

use testlab_schema::{
    AdapterEvent, AssignedConsumerFetchEvidence, BrokerObservation, BrokerPartitionOffsets,
    BrokerStateObservation, BrokerTopicIdentityState, ConsumedRecord, HistoryEntry, HistoryPayload,
    OperationId, Scenario, ScenarioAction,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::event;

#[test]
fn complete_fetch_evidence_matches_independent_broker_facts() {
    let scenario = scenario();
    let (history, observations) = evidence(&scenario);

    let violations = verify(&scenario, &history, &observations);

    assert!(!violates(&violations));
}

#[test]
fn unrelated_facts_from_the_same_batches_are_ignored() {
    let scenario = scenario();
    let (mut history, observations) = evidence(&scenario);
    history.push(state(
        0,
        BrokerStateObservation::TopicIdentity(BrokerTopicIdentityState {
            observation: 11,
            operation_id: operation("admin-fetch-topic-id"),
            topic: "testlab-fetch-control".to_owned(),
            topic_id: [8; 16],
            partitions: vec![0],
        }),
    ));
    history.push(state(
        1,
        BrokerStateObservation::PartitionOffsets(BrokerPartitionOffsets {
            observation: 21,
            operation_id: operation("admin-fetch-watermarks"),
            topic: "testlab-fetch-control".to_owned(),
            partition: 0,
            low_watermark: 0,
            high_watermark: 1,
        }),
    ));

    let violations = verify(&scenario, &history, &observations);

    assert!(!violates(&violations));
}

#[test]
fn changed_fetch_topic_uuid_fails_consumer_contract() {
    let scenario = scenario();
    let (mut history, observations) = evidence(&scenario);
    let fetch = fetch_evidence(&mut history);
    fetch.topic_uuid = [9; 16];

    let violations = verify(&scenario, &history, &observations);

    assert!(violates(&violations));
}

#[test]
fn changed_fetch_watermark_fails_consumer_contract() {
    let scenario = scenario();
    let (mut history, observations) = evidence(&scenario);
    let fetch = fetch_evidence(&mut history);
    fetch.high_watermark = Some(2);

    let violations = verify(&scenario, &history, &observations);

    assert!(violates(&violations));
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-fetch-evidence.toml"
    ))
    .unwrap_or_else(|error| panic!("parse Fetch-evidence scenario: {error}"))
}

fn evidence(scenario: &Scenario) -> (Vec<HistoryEntry>, Vec<BrokerObservation>) {
    let record = scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::Send {
            operation_id,
            record,
            ..
        } if operation_id.as_str() == "op-fetch-evidence" => Some(record.clone()),
        _ => None,
    });
    let record = record.unwrap_or_else(|| panic!("source record missing"));
    let observation = BrokerObservation {
        observation: 30,
        offset: 0,
        operation_id: operation("op-fetch-evidence"),
        digest: record
            .digest()
            .unwrap_or_else(|error| panic!("record digest: {error}")),
        record: record.clone(),
    };
    let history = vec![
        state(
            0,
            BrokerStateObservation::TopicIdentity(BrokerTopicIdentityState {
                observation: 10,
                operation_id: operation("admin-fetch-topic-id"),
                topic: record.topic.clone(),
                topic_id: [7; 16],
                partitions: vec![0],
            }),
        ),
        state(
            1,
            BrokerStateObservation::PartitionOffsets(BrokerPartitionOffsets {
                observation: 20,
                operation_id: operation("admin-fetch-watermarks"),
                topic: record.topic.clone(),
                partition: 0,
                low_watermark: 0,
                high_watermark: 1,
            }),
        ),
        event(
            2,
            AdapterEvent::ReceiveCompleted {
                receive_id: operation("receive-fetch-evidence"),
                records: vec![ConsumedRecord {
                    topic: record.topic.clone(),
                    partition: record.partition,
                    offset: 0,
                    timestamp_millis: record.timestamp_millis,
                    key: record.key.clone(),
                    value: record.value.clone(),
                    headers: record.headers.clone(),
                }],
                fetch_evidence: Some(AssignedConsumerFetchEvidence {
                    topic: record.topic,
                    topic_uuid: [7; 16],
                    partition: 0,
                    requested_offset: 0,
                    next_offset: 1,
                    checkpoint_next_offset: 1,
                    log_start_offset: Some(0),
                    last_stable_offset: Some(1),
                    high_watermark: Some(1),
                    retained_bytes: 256,
                }),
            },
        ),
    ];
    (history, vec![observation])
}

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn fetch_evidence(history: &mut [HistoryEntry]) -> &mut AssignedConsumerFetchEvidence {
    let HistoryPayload::AdapterEvent { event } = &mut history[2].payload else {
        panic!("receive event missing");
    };
    let AdapterEvent::ReceiveCompleted {
        fetch_evidence: Some(evidence),
        ..
    } = &mut event.event
    else {
        panic!("Fetch evidence missing");
    };
    evidence
}

fn verify(
    scenario: &Scenario,
    history: &[HistoryEntry],
    observations: &[BrokerObservation],
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::record_offsets::verify(scenario, &index, observations, &mut violations);
    violations
}

fn violates(violations: &[testlab_schema::Violation]) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == "CONS-022")
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
