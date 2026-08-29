//! Lifecycle command tests pin repeated-resource operations to exact correlation identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandEnvelope, CommandId, ConsumerId,
    HistoryEntry, HistoryPayload, ProducerId, TerminalStatus, VisibilityExpectation,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::scenario;

#[test]
fn repeated_assignments_and_flushes_are_independently_correlated() {
    let consumer = consumer("consumer-1");
    let producer = producer("producer-1");
    let history = vec![
        command(
            0,
            "assign-1",
            AdapterCommand::AssignBeginning {
                consumer_id: consumer.clone(),
                topic: "first".to_owned(),
                partition: 0,
            },
        ),
        event(
            1,
            "assign-1",
            AdapterEvent::AssignmentCompleted {
                consumer_id: consumer.clone(),
            },
        ),
        command(
            2,
            "assign-2",
            AdapterCommand::AssignBeginning {
                consumer_id: consumer.clone(),
                topic: "second".to_owned(),
                partition: 1,
            },
        ),
        event(
            3,
            "assign-2",
            AdapterEvent::AssignmentCompleted {
                consumer_id: consumer,
            },
        ),
        command(
            4,
            "flush-1",
            AdapterCommand::Flush {
                producer_id: producer.clone(),
            },
        ),
        event(
            5,
            "flush-1",
            AdapterEvent::FlushCompleted {
                producer_id: producer.clone(),
            },
        ),
        command(
            6,
            "flush-2",
            AdapterCommand::Flush {
                producer_id: producer.clone(),
            },
        ),
        event(
            7,
            "flush-2",
            AdapterEvent::FlushCompleted {
                producer_id: producer,
            },
        ),
    ];

    let violations = verify(&history);

    assert!(violations.is_empty(), "violations: {violations:?}");
}

#[test]
fn duplicate_or_cross_correlated_assignment_events_fail() {
    let consumer = consumer("consumer-1");
    let history = vec![
        command(
            0,
            "assign-1",
            AdapterCommand::AssignBeginning {
                consumer_id: consumer.clone(),
                topic: "first".to_owned(),
                partition: 0,
            },
        ),
        event(
            1,
            "assign-1",
            AdapterEvent::AssignmentCompleted {
                consumer_id: consumer.clone(),
            },
        ),
        event(
            2,
            "assign-1",
            AdapterEvent::AssignmentCompleted {
                consumer_id: consumer.clone(),
            },
        ),
        command(
            3,
            "assign-2",
            AdapterCommand::AssignBeginning {
                consumer_id: consumer,
                topic: "second".to_owned(),
                partition: 1,
            },
        ),
    ];

    let violations = verify(&history);

    assert_eq!(
        violations
            .iter()
            .filter(|violation| violation.contract_id.as_str() == "LIFE-009")
            .count(),
        2
    );
}

#[test]
fn completion_before_its_command_cannot_satisfy_lifecycle() {
    let consumer = consumer("consumer-1");
    let history = vec![
        event(
            0,
            "assign-1",
            AdapterEvent::AssignmentCompleted {
                consumer_id: consumer.clone(),
            },
        ),
        command(
            1,
            "assign-1",
            AdapterCommand::AssignBeginning {
                consumer_id: consumer,
                topic: "first".to_owned(),
                partition: 0,
            },
        ),
    ];

    let violations = verify(&history);

    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "LIFE-009")
    );
}

fn verify(history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::lifecycle_commands::verify(&scenario, &index, &mut violations);
    violations
}

fn command(sequence: u64, command_id: &str, value: AdapterCommand) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::HarnessCommand {
            command: CommandEnvelope::new(id(command_id), value),
        },
    }
}

fn event(sequence: u64, command_id: &str, value: AdapterEvent) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::AdapterEvent {
            event: AdapterEventEnvelope::new(id(command_id), value),
        },
    }
}

fn id(value: &str) -> CommandId {
    CommandId::new(value).unwrap_or_else(|error| panic!("command id: {error}"))
}

fn consumer(value: &str) -> ConsumerId {
    ConsumerId::new(value).unwrap_or_else(|error| panic!("consumer id: {error}"))
}

fn producer(value: &str) -> ProducerId {
    ProducerId::new(value).unwrap_or_else(|error| panic!("producer id: {error}"))
}
