//! Assigned-record transfer tests pin ownership and delivery invariants.
use testlab_schema::{
    AdapterCommand, AdapterDescriptor, AdapterEvent, AdapterEventEnvelope, AdapterId,
    AssignedRecordTransferAction, AssignedRecordTransferCommand, AssignedRecordTransferCompletion,
    BrokerObservation, CommandEnvelope, CommandId, ConsumedRecord, HistoryEntry, HistoryPayload,
    OperationId, ProducerReceipt, RecordSpec, Scenario, ScenarioAction, TerminalStatus,
};

#[test]
fn changed_retained_source_or_destination_fails() {
    let (scenario, _, mut history, observations) = fixture();
    assert!(violations(&scenario, &history, &observations).is_empty());
    completion(&mut history).retained_source_record.value = None;
    assert_contract(&scenario, &history, &observations);

    let (scenario, _, history, mut observations) = fixture();
    observations[1].record.headers.pop();
    observations[1].digest = digest(&observations[1].record);
    assert_contract(&scenario, &history, &observations);
}

fn fixture() -> (
    Scenario,
    AdapterDescriptor,
    Vec<HistoryEntry>,
    Vec<BrokerObservation>,
) {
    let scenario = scenario();
    let adapter = adapter();
    let (source_operation, source) = source(&scenario);
    let action = transfer(&scenario);
    let target = testlab_schema::transferred_record(
        source,
        &action.operation_id,
        &action.target_topic,
        action.target_partition,
    );
    let source_receipt = receipt(source);
    let target_receipt = receipt(&target);
    let source_public = consumed(source, 0);
    let history = vec![
        event(
            0,
            "hello-command",
            AdapterEvent::Ready {
                descriptor: adapter.clone(),
            },
        ),
        command(1, "source-command", send_command(&scenario)),
        event(
            2,
            "source-command",
            AdapterEvent::OperationAccepted {
                operation_id: source_operation.clone(),
            },
        ),
        event(
            3,
            "source-command",
            terminal(source_operation.clone(), source, 0, source_receipt),
        ),
        command(
            4,
            "transfer-command",
            AdapterCommand::TransferAssignedRecord(transfer_command(action)),
        ),
        event(
            5,
            "transfer-command",
            AdapterEvent::OperationAccepted {
                operation_id: action.operation_id.clone(),
            },
        ),
        event(
            6,
            "transfer-command",
            terminal(
                action.operation_id.clone(),
                &target,
                0,
                target_receipt.clone(),
            ),
        ),
        event(
            7,
            "transfer-command",
            AdapterEvent::AssignedRecordTransferCompleted(AssignedRecordTransferCompletion {
                operation_id: action.operation_id.clone(),
                source_record: source_public.clone(),
                retained_source_record: source_public,
                status: TerminalStatus::Acknowledged,
                code: None,
                partition: Some(target.partition),
                offset: Some(0),
                timestamp_millis: target.timestamp_millis,
                receipt: Some(target_receipt),
            }),
        ),
    ];
    let observations = vec![
        observation(0, source_operation, source.clone()),
        observation(1, action.operation_id.clone(), target),
    ];
    (scenario, adapter, history, observations)
}

fn terminal(
    operation_id: OperationId,
    record: &RecordSpec,
    offset: i64,
    receipt: ProducerReceipt,
) -> AdapterEvent {
    AdapterEvent::OperationTerminal {
        operation_id,
        status: TerminalStatus::Acknowledged,
        code: None,
        partition: Some(record.partition),
        offset: Some(offset),
        timestamp_millis: record.timestamp_millis,
        receipt: Some(receipt),
    }
}

fn receipt(record: &RecordSpec) -> ProducerReceipt {
    ProducerReceipt {
        topic: record.topic.clone(),
        topic_uuid: None,
        leader_epoch: Some(3),
        serialized_key_size: decoded_len(record.key.as_ref()),
        serialized_value_size: decoded_len(record.value.as_ref()),
    }
}

fn decoded_len(value: Option<&testlab_schema::ByteString>) -> Option<usize> {
    value.map(|value| {
        value
            .decode()
            .unwrap_or_else(|error| panic!("fixture bytes: {error}"))
            .len()
    })
}

fn consumed(record: &RecordSpec, offset: i64) -> ConsumedRecord {
    ConsumedRecord {
        topic: record.topic.clone(),
        partition: record.partition,
        offset,
        timestamp_millis: record.timestamp_millis,
        key: record.key.clone(),
        value: record.value.clone(),
        headers: record.headers.clone(),
    }
}

fn observation(index: u64, operation_id: OperationId, record: RecordSpec) -> BrokerObservation {
    BrokerObservation {
        observation: index,
        offset: 0,
        operation_id,
        digest: digest(&record),
        record,
    }
}

fn send_command(scenario: &Scenario) -> AdapterCommand {
    let ScenarioAction::Send {
        producer_id,
        operation_id,
        method,
        partitioning,
        record,
        ..
    } = &scenario.steps[3].action
    else {
        panic!("source send missing");
    };
    AdapterCommand::Send {
        producer_id: producer_id.clone(),
        operation_id: operation_id.clone(),
        method: *method,
        partitioning: *partitioning,
        validate_topic_uuid: false,
        record: record.clone(),
    }
}

fn transfer_command(action: &AssignedRecordTransferAction) -> AssignedRecordTransferCommand {
    AssignedRecordTransferCommand {
        consumer_id: action.consumer_id.clone(),
        producer_id: action.producer_id.clone(),
        operation_id: action.operation_id.clone(),
        method: action.method,
        target_topic: action.target_topic.clone(),
        target_partition: action.target_partition,
        timeout_ms: action.timeout_ms,
    }
}

fn source(scenario: &Scenario) -> (OperationId, &RecordSpec) {
    let ScenarioAction::Send {
        operation_id,
        record,
        ..
    } = &scenario.steps[3].action
    else {
        panic!("source send missing");
    };
    (operation_id.clone(), record)
}

fn transfer(scenario: &Scenario) -> &AssignedRecordTransferAction {
    let ScenarioAction::TransferAssignedRecord(action) = &scenario.steps[7].action else {
        panic!("transfer action missing");
    };
    action
}

fn completion(history: &mut [HistoryEntry]) -> &mut AssignedRecordTransferCompletion {
    let HistoryPayload::AdapterEvent {
        event:
            AdapterEventEnvelope {
                event: AdapterEvent::AssignedRecordTransferCompleted(completion),
                ..
            },
    } = &mut history[7].payload
    else {
        panic!("transfer completion missing");
    };
    completion
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-owned-record-transfer.toml"
    ))
    .unwrap_or_else(|error| panic!("parse owned transfer: {error}"))
}

fn adapter() -> AdapterDescriptor {
    AdapterDescriptor {
        id: id(AdapterId::new("kafkars-rust")),
        implementation: "fixture".to_owned(),
        version: "fixture".to_owned(),
        protocol_version: testlab_schema::PROTOCOL_VERSION,
        capabilities: std::collections::BTreeSet::new(),
    }
}

fn command(sequence: u64, command_id: &str, command: AdapterCommand) -> HistoryEntry {
    entry(
        sequence,
        HistoryPayload::HarnessCommand {
            command: CommandEnvelope::new(id(CommandId::new(command_id)), command),
        },
    )
}

fn event(sequence: u64, command_id: &str, event: AdapterEvent) -> HistoryEntry {
    entry(
        sequence,
        HistoryPayload::AdapterEvent {
            event: AdapterEventEnvelope::new(id(CommandId::new(command_id)), event),
        },
    )
}

fn entry(sequence: u64, payload: HistoryPayload) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload,
    }
}

fn digest(record: &RecordSpec) -> String {
    record
        .digest()
        .unwrap_or_else(|error| panic!("fixture digest: {error}"))
}

fn assert_contract(
    scenario: &Scenario,
    history: &[HistoryEntry],
    observations: &[BrokerObservation],
) {
    let violated = violations(scenario, history, observations)
        .iter()
        .any(|violation| violation.contract_id.as_str() == "CONS-021");
    assert!(violated);
}

fn violations(
    scenario: &Scenario,
    history: &[HistoryEntry],
    observations: &[BrokerObservation],
) -> Vec<testlab_schema::Violation> {
    let mut violations = Vec::new();
    super::verify(
        scenario,
        &crate::index::HistoryIndex::build(history),
        observations,
        &mut violations,
    );
    violations
}

fn id<T>(result: Result<T, impl std::fmt::Display>) -> T {
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
