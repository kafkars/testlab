//! Producer receipt verification joins public metadata to independent topic and record truth.

use testlab_schema::{
    AdapterCommand, BrokerObservation, ByteString, DescribeTopicsAction, OperationId, RecordSpec,
    Scenario, ScenarioAction, TerminalStatus, TopicSelection, Violation,
};

use crate::index::{HistoryIndex, IndexedAdminTopicsDescription, IndexedTopicIdentityObservation};
use crate::support::violation;

pub(crate) fn verify(
    scenario: &Scenario,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        let ScenarioAction::Send {
            producer_id,
            operation_id,
            method,
            partitioning,
            topic_identity_operation_id: Some(identity_operation_id),
            record,
        } = &step.action
        else {
            continue;
        };
        if !index.action_issued(&step.action) {
            continue;
        }
        let description = scenario.steps.iter().find_map(|step| match &step.action {
            ScenarioAction::DescribeTopics(action)
                if action.operation_id == *identity_operation_id =>
            {
                Some(action)
            }
            _ => None,
        });
        let expected_command = AdapterCommand::Send {
            producer_id: producer_id.clone(),
            operation_id: operation_id.clone(),
            method: *method,
            partitioning: *partitioning,
            validate_topic_uuid: true,
            record: record.clone(),
        };
        verify_one(
            operation_id,
            identity_operation_id,
            record,
            description,
            &expected_command,
            index,
            observations,
            violations,
        );
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one complete producer receipt contract"
)]
fn verify_one(
    operation_id: &OperationId,
    identity_operation_id: &OperationId,
    record: &RecordSpec,
    description: Option<&DescribeTopicsAction>,
    expected_command: &AdapterCommand,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let commands = index
        .commands
        .iter()
        .filter(|(_, _, command)| {
            matches!(command, AdapterCommand::Send {
                operation_id: actual, ..
            } if actual == operation_id)
        })
        .collect::<Vec<_>>();
    let public = one(index.topics_batch_described.get(identity_operation_id));
    let independent = one(index.topic_identities_observed.get(identity_operation_id));
    let terminal = one(index.terminals.get(operation_id));
    let observed = observations
        .iter()
        .filter(|value| value.operation_id == *operation_id)
        .collect::<Vec<_>>();
    let command_sequence =
        matches!(commands.as_slice(), [(_, _, actual)] if actual == expected_command)
            .then(|| commands[0].0);
    let public_id = public.and_then(|value| public_topic_id(value, record));
    let independent_id = independent.and_then(|value| independent_topic_id(value, record));
    let scenario_exact = description.is_some_and(|action| {
        action.selection == TopicSelection::TopicId
            && action.operation_id == *identity_operation_id
            && {
                let mut topics = action
                    .topics
                    .iter()
                    .filter(|topic| topic.topic == record.topic);
                let topic = topics.next();
                topics.next().is_none()
                    && topic.is_some_and(|topic| {
                        topic.expected_error_code.is_none()
                            && topic
                                .expected_partitions
                                .as_ref()
                                .is_some_and(|partitions| partitions.contains(&record.partition))
                    })
            }
    });
    let chronology = command_sequence.is_some_and(|command| {
        public.is_some_and(|value| value.history_sequence < command)
            && independent.is_some_and(|value| {
                public.is_some_and(|public| {
                    public.history_sequence < value.history_sequence
                        && value.history_sequence < command
                })
            })
            && terminal.is_some_and(|value| value.history_sequence > command)
    });
    let exact = scenario_exact
        && chronology
        && public_id.is_some()
        && public_id == independent_id
        && matches!(observed.as_slice(), [observation] if receipt_matches(
            record,
            public_id,
            terminal,
            observation,
        ));
    if exact {
        return;
    }
    let mut evidence = commands
        .iter()
        .map(|(sequence, ..)| format!("history:{sequence}"))
        .collect::<Vec<_>>();
    evidence.extend(public.map(|value| format!("history:{}", value.history_sequence)));
    evidence
        .extend(independent.map(|value| format!("broker-state-observation:{}", value.observation)));
    evidence.extend(terminal.map(|value| format!("history:{}", value.history_sequence)));
    evidence.extend(
        observed
            .iter()
            .map(|value| format!("broker-observation:{}", value.observation)),
    );
    violations.push(violation(
        "PROD-018",
        format!(
            "operation {operation_id} expected one exact UUID-bound producer receipt after public and independent topic-ID evidence"
        ),
        Some(operation_id.clone()),
        evidence,
    ));
}

fn public_topic_id(value: &IndexedAdminTopicsDescription, record: &RecordSpec) -> Option<[u8; 16]> {
    let mut outcomes = value
        .value
        .outcomes
        .iter()
        .filter(|outcome| outcome.topic == record.topic);
    let outcome = outcomes.next()?;
    if outcomes.next().is_some() {
        return None;
    }
    let id = outcome.topic_id?;
    (outcome.topic == record.topic
        && id != [0; 16]
        && outcome.error_code.is_none()
        && outcome.description.as_ref().is_some_and(|description| {
            description.topic_id == Some(id)
                && description.partitions.iter().any(|partition| {
                    partition.partition == record.partition && partition.error_code.is_none()
                })
        }))
    .then_some(id)
}

fn independent_topic_id(
    value: &IndexedTopicIdentityObservation,
    record: &RecordSpec,
) -> Option<[u8; 16]> {
    (value.topic == record.topic
        && value.topic_id != [0; 16]
        && value.partitions.contains(&record.partition))
    .then_some(value.topic_id)
}

fn receipt_matches(
    record: &RecordSpec,
    topic_id: Option<[u8; 16]>,
    terminal: Option<&crate::index::IndexedTerminal>,
    observation: &BrokerObservation,
) -> bool {
    let Some(terminal) = terminal else {
        return false;
    };
    let Some(receipt) = terminal.receipt.as_ref() else {
        return false;
    };
    terminal.status == TerminalStatus::Acknowledged
        && terminal.code.is_none()
        && terminal.partition == Some(record.partition)
        && terminal.partition == Some(observation.record.partition)
        && terminal.offset == Some(observation.offset)
        && terminal.timestamp_millis == record.timestamp_millis
        && terminal.timestamp_millis == observation.record.timestamp_millis
        && receipt.topic == record.topic
        && receipt.topic_uuid == topic_id
        && receipt.leader_epoch.is_none_or(|value| value >= 0)
        && receipt.serialized_key_size == serialized_size(record.key.as_ref())
        && receipt.serialized_value_size == serialized_size(record.value.as_ref())
}

fn serialized_size(value: Option<&ByteString>) -> Option<usize> {
    value.map(|value| value.decode().map_or(usize::MAX, |bytes| bytes.len()))
}

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}

#[cfg(test)]
#[path = "producer_receipt_test.rs"]
mod tests;
