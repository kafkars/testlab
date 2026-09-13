//! Partition-leader consumer tests pin exact public progress inside the outage window.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, BrokerRoleTarget, ConsumedRecord, ConsumerId, HistoryEntry,
    OperationId, ProducerId, Scenario, ScenarioAction, ScenarioId, ShareConsumedRecord,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, record, step};

#[test]
fn every_consumer_surface_progresses_on_the_target_partition() {
    for kind in [
        ReceiveKind::Assigned,
        ReceiveKind::Group,
        ReceiveKind::Share,
    ] {
        let violations = violations(kind, history(kind, true, 0, false, 2));
        assert!(!has(&violations, "FAULT-004"), "{kind:?}: {violations:?}");
    }
}

#[test]
fn uncommitted_group_progress_fails() {
    let violations = violations(
        ReceiveKind::Group,
        history(ReceiveKind::Group, false, 0, false, 2),
    );
    assert!(has(&violations, "FAULT-004"), "{violations:?}");
}

#[test]
fn progress_on_another_partition_fails() {
    let violations = violations(
        ReceiveKind::Assigned,
        history(ReceiveKind::Assigned, true, 1, false, 2),
    );
    assert!(has(&violations, "FAULT-004"), "{violations:?}");
}

#[test]
fn duplicate_receive_commands_fail() {
    let violations = violations(
        ReceiveKind::Share,
        history(ReceiveKind::Share, true, 0, true, 2),
    );
    assert!(has(&violations, "FAULT-004"), "{violations:?}");
}

#[test]
fn receive_command_before_replacement_election_fails() {
    let violations = violations(
        ReceiveKind::Assigned,
        history(ReceiveKind::Assigned, true, 0, false, 1),
    );
    assert!(has(&violations, "FAULT-004"), "{violations:?}");
}

#[derive(Clone, Copy, Debug)]
enum ReceiveKind {
    Assigned,
    Group,
    Share,
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the test helper owns each constructed history fixture"
)]
fn violations(kind: ReceiveKind, history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let scenario = scenario(kind);
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    super::consumer_progress::verify(&scenario, 1, &target(), &index, 1, 5, &mut violations);
    violations
}

fn scenario(kind: ReceiveKind) -> Scenario {
    Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("fault.consumer-leader-recovery")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "consumer leader recovery".to_owned(),
        description: "consumer leader recovery fixture".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::new(),
        steps: vec![
            step(
                "send",
                ScenarioAction::Send {
                    producer_id: producer(),
                    operation_id: operation("send-after-election"),
                    method: testlab_schema::ProducerSendMethod::default(),
                    partitioning: testlab_schema::ProducerPartitioning::default(),
                    topic_identity_operation_id: None,
                    record: record("after-election"),
                },
            ),
            step(
                "stop",
                ScenarioAction::StopBrokerRole {
                    target: target(),
                    timeout_ms: 1_000,
                },
            ),
            step("receive", receive_action(kind)),
            step(
                "restore",
                ScenarioAction::RestoreBrokerRole {
                    target: target(),
                    timeout_ms: 1_000,
                },
            ),
        ],
        assertions: Vec::new(),
    }
}

fn receive_action(kind: ReceiveKind) -> ScenarioAction {
    match kind {
        ReceiveKind::Assigned => ScenarioAction::Receive {
            consumer_id: consumer(),
            method: testlab_schema::AssignedConsumerReceiveMethod::default(),
            observe_fetch_evidence: false,
            receive_id: operation("receive-after-election"),
            expected_operation_id: operation("send-after-election"),
            timeout_ms: 1_000,
        },
        ReceiveKind::Group => ScenarioAction::GroupReceive {
            consumer_id: consumer(),
            method: testlab_schema::GroupConsumerReceiveMethod::default(),
            checkpoint_method: testlab_schema::GroupCheckpointMethod::default(),
            receive_id: operation("receive-after-election"),
            expected_operation_id: operation("send-after-election"),
            additional_expected_operation_ids: Vec::new(),
            processed_record_count: None,
            processing_acknowledgement_delay_ms: 0,
            expected_error_code: None,
            timeout_ms: 1_000,
        },
        ReceiveKind::Share => ScenarioAction::ShareReceive {
            consumer_id: consumer(),
            receive_id: operation("receive-after-election"),
            expected_operation_ids: vec![operation("send-after-election")],
            minimum_delivery_count: 1,
            expected_acquisition_count: None,
            timeout_ms: 1_000,
        },
    }
}

fn history(
    kind: ReceiveKind,
    committed: bool,
    partition: i32,
    duplicate: bool,
    command_sequence: u64,
) -> Vec<HistoryEntry> {
    let mut history = vec![command(command_sequence, receive_command(kind))];
    if duplicate {
        history.push(command(command_sequence + 1, receive_command(kind)));
    }
    let duplicate_offset = u64::from(duplicate);
    history.push(event(
        command_sequence + duplicate_offset + 1,
        receive_event(kind, committed, partition),
    ));
    history
}

fn receive_command(kind: ReceiveKind) -> AdapterCommand {
    match kind {
        ReceiveKind::Assigned => AdapterCommand::Receive {
            consumer_id: consumer(),
            method: testlab_schema::AssignedConsumerReceiveMethod::default(),
            receive_id: operation("receive-after-election"),
            timeout_ms: 1_000,
        },
        ReceiveKind::Group => AdapterCommand::GroupReceive {
            consumer_id: consumer(),
            method: testlab_schema::GroupConsumerReceiveMethod::default(),
            checkpoint_method: testlab_schema::GroupCheckpointMethod::default(),
            receive_id: operation("receive-after-election"),
            processing_acknowledgement_delay_ms: 0,
            processed_record_count: None,
            timeout_ms: 1_000,
        },
        ReceiveKind::Share => AdapterCommand::ShareReceive {
            consumer_id: consumer(),
            receive_id: operation("receive-after-election"),
            timeout_ms: 1_000,
        },
    }
}

fn receive_event(kind: ReceiveKind, committed: bool, partition: i32) -> AdapterEvent {
    let records = vec![consumed(partition)];
    match kind {
        ReceiveKind::Assigned => AdapterEvent::ReceiveCompleted {
            receive_id: operation("receive-after-election"),
            records,
            fetch_evidence: None,
        },
        ReceiveKind::Group => AdapterEvent::GroupReceiveCompleted {
            receive_id: operation("receive-after-election"),
            records,
            committed,
            group_epoch: None,
        },
        ReceiveKind::Share => AdapterEvent::ShareReceiveCompleted {
            consumer_id: consumer(),
            receive_id: operation("receive-after-election"),
            records: records
                .into_iter()
                .map(|record| ShareConsumedRecord {
                    record,
                    delivery_count: 1,
                })
                .collect(),
            acquisition_count: 1,
            member_epoch: Some(1),
            assignment_epoch: Some(1),
        },
    }
}

fn consumed(partition: i32) -> ConsumedRecord {
    ConsumedRecord {
        topic: "records".to_owned(),
        partition,
        offset: 1,
        timestamp_millis: None,
        key: None,
        value: None,
        headers: Vec::new(),
    }
}

fn target() -> BrokerRoleTarget {
    BrokerRoleTarget::PartitionLeader {
        topic: "records".to_owned(),
        partition: 0,
    }
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn consumer() -> ConsumerId {
    ConsumerId::new("consumer-1").unwrap_or_else(|error| panic!("consumer id: {error}"))
}

fn producer() -> ProducerId {
    ProducerId::new("producer-1").unwrap_or_else(|error| panic!("producer id: {error}"))
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
