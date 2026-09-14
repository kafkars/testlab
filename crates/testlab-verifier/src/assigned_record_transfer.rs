//! Owned record-transfer verification joins both public owners to two broker records.

#[cfg(test)]
#[path = "assigned_record_conversion_method_test.rs"]
mod method_tests;
#[cfg(test)]
#[path = "assigned_record_transfer_test.rs"]
mod tests;

use testlab_schema::{
    AdapterCommand, AssignedRecordConversionMethod, AssignedRecordTransferAction,
    AssignedRecordTransferCommand, BrokerObservation, CommandId, Scenario, ScenarioAction,
    TerminalStatus, Violation,
};

use crate::consumer::{exact_bytes, exact_record};
use crate::index::{HistoryIndex, IndexedAssignedRecordTransfer, IndexedTerminal};
use crate::support::violation;
use crate::verify_index::observations_by_operation;

pub(crate) fn verify(
    scenario: &Scenario,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let observed = observations_by_operation(observations);
    for step in &scenario.steps {
        let ScenarioAction::TransferAssignedRecord(action) = &step.action else {
            continue;
        };
        let contract = contract(action.method);
        if !index.action_issued(&step.action) {
            continue;
        }
        let commands = commands(index, action);
        let completions = index.assigned_record_transfers.get(&action.operation_id);
        let accepted = index.accepted.get(&action.operation_id);
        let terminals = index.terminals.get(&action.operation_id);
        let source = observed.get(&action.expected_input_operation_id);
        let target = observed.get(&action.operation_id);
        let valid = matches!(commands.as_slice(), [(_, _, actual)] if actual == &command(action))
            && matches!(accepted.map(Vec::as_slice), Some([_]))
            && matches!(terminals.map(Vec::as_slice), Some([_]))
            && matches!(completions.map(Vec::as_slice), Some([_]))
            && matches!(source.map(Vec::as_slice), Some([_]))
            && matches!(target.map(Vec::as_slice), Some([_]));
        if !valid {
            violations.push(violation(
                contract,
                format!(
                    "owned transfer {} expected one exact command, admission, terminal, completion, source observation, and destination observation",
                    action.operation_id
                ),
                Some(action.operation_id.clone()),
                references(
                    commands.as_slice(),
                    accepted.map(Vec::as_slice),
                    terminals.map(Vec::as_slice),
                    completions.map(Vec::as_slice),
                    source.map(Vec::as_slice),
                    target.map(Vec::as_slice),
                ),
            ));
            continue;
        }
        let (_, command_id, actual_command) = &commands[0];
        let terminal = &terminals.map_or(&[][..], Vec::as_slice)[0];
        let completion = &completions.map_or(&[][..], Vec::as_slice)[0];
        let source = source.map_or(&[][..], Vec::as_slice)[0];
        let target = target.map_or(&[][..], Vec::as_slice)[0];
        verify_exact(
            scenario,
            action,
            actual_command,
            terminal,
            completion,
            source,
            target,
            accepted.map_or(&[][..], Vec::as_slice)[0],
            commands[0].0,
            command_id,
            index,
            violations,
        );
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "complete transfer evidence tuple"
)]
fn verify_exact(
    scenario: &Scenario,
    action: &AssignedRecordTransferAction,
    actual_command: &AssignedRecordTransferCommand,
    terminal: &IndexedTerminal,
    transfer: &IndexedAssignedRecordTransfer,
    source: &BrokerObservation,
    target: &BrokerObservation,
    accepted_sequence: u64,
    command_sequence: u64,
    command_id: &CommandId,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let Some(expected_source) =
        crate::consumer::sent_record(scenario, &action.expected_input_operation_id)
    else {
        return;
    };
    let expected_target = testlab_schema::transferred_record(
        expected_source,
        &action.operation_id,
        &action.target_topic,
        action.target_partition,
    );
    let completion = &transfer.completion;
    let ordered = command_sequence < accepted_sequence
        && accepted_sequence < terminal.history_sequence
        && terminal.history_sequence < transfer.history_sequence;
    let correlated = [
        accepted_sequence,
        terminal.history_sequence,
        transfer.history_sequence,
    ]
    .into_iter()
    .all(|sequence| event_uses_command(index, sequence, command_id));
    let source_exact = exact_record(&completion.source_record, expected_source)
        && completion.source_record.offset == source.offset
        && exact_record(&completion.source_record, &source.record)
        && completion.retained_source_record == completion.source_record;
    let target_exact = exact_record_data(&target.record, &expected_target)
        && completion.status == TerminalStatus::Acknowledged
        && completion.code.is_none()
        && completion.partition == Some(target.record.partition)
        && completion.offset == Some(target.offset)
        && completion.timestamp_millis == target.record.timestamp_millis
        && terminal_matches_completion(terminal, completion)
        && receipt_matches(completion, &expected_target);
    if actual_command == &command(action)
        && completion.operation_id == action.operation_id
        && ordered
        && correlated
        && source_exact
        && target_exact
    {
        return;
    }
    violations.push(violation(
        contract(action.method),
        format!(
            "owned transfer {} did not preserve its source lease, exact bytes, or destination terminal",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        vec![
            format!("history:{command_sequence}"),
            format!("history:{accepted_sequence}"),
            format!("history:{}", terminal.history_sequence),
            format!("history:{}", transfer.history_sequence),
            format!("broker-observation:{}", source.observation),
            format!("broker-observation:{}", target.observation),
        ],
    ));
}

fn command(action: &AssignedRecordTransferAction) -> AssignedRecordTransferCommand {
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

const fn contract(method: AssignedRecordConversionMethod) -> &'static str {
    match method {
        AssignedRecordConversionMethod::IntoOwnedBatch => "CONS-021",
        AssignedRecordConversionMethod::IntoOwnedRecords => "CONS-023",
    }
}

fn commands(
    index: &HistoryIndex,
    action: &AssignedRecordTransferAction,
) -> Vec<(u64, CommandId, AssignedRecordTransferCommand)> {
    index
        .commands
        .iter()
        .filter_map(|(sequence, command_id, command)| match command {
            AdapterCommand::TransferAssignedRecord(actual)
                if actual.operation_id == action.operation_id =>
            {
                Some((*sequence, command_id.clone(), actual.clone()))
            }
            _ => None,
        })
        .collect()
}
fn terminal_matches_completion(
    terminal: &IndexedTerminal,
    completion: &testlab_schema::AssignedRecordTransferCompletion,
) -> bool {
    terminal.status == completion.status
        && terminal.code == completion.code
        && terminal.partition == completion.partition
        && terminal.offset == completion.offset
        && terminal.timestamp_millis == completion.timestamp_millis
        && terminal.receipt == completion.receipt
}

fn receipt_matches(
    completion: &testlab_schema::AssignedRecordTransferCompletion,
    expected: &testlab_schema::RecordSpec,
) -> bool {
    completion.receipt.as_ref().is_some_and(|receipt| {
        receipt.topic == expected.topic
            && receipt.topic_uuid.is_none()
            && receipt.leader_epoch.is_none_or(|epoch| epoch >= 0)
            && receipt.serialized_key_size == decoded_len(expected.key.as_ref())
            && receipt.serialized_value_size == decoded_len(expected.value.as_ref())
    })
}

fn decoded_len(value: Option<&testlab_schema::ByteString>) -> Option<usize> {
    value.and_then(|value| value.decode().ok().map(|bytes| bytes.len()))
}

fn exact_record_data(
    actual: &testlab_schema::RecordSpec,
    expected: &testlab_schema::RecordSpec,
) -> bool {
    actual.topic == expected.topic
        && actual.partition == expected.partition
        && actual.sequence == expected.sequence
        && (expected.timestamp_millis.is_none()
            || actual.timestamp_millis == expected.timestamp_millis)
        && exact_bytes(actual.key.as_ref(), expected.key.as_ref())
        && exact_bytes(actual.value.as_ref(), expected.value.as_ref())
        && actual.headers.len() == expected.headers.len()
        && actual
            .headers
            .iter()
            .zip(&expected.headers)
            .all(|(actual, expected)| {
                actual.name == expected.name
                    && exact_bytes(actual.value.as_ref(), expected.value.as_ref())
            })
}

fn event_uses_command(index: &HistoryIndex, sequence: u64, command_id: &CommandId) -> bool {
    index.adapter_events.iter().any(|(actual_sequence, event)| {
        *actual_sequence == sequence && &event.command_id == command_id
    })
}

fn references(
    commands: &[(u64, CommandId, AssignedRecordTransferCommand)],
    accepted: Option<&[u64]>,
    terminals: Option<&[IndexedTerminal]>,
    transfers: Option<&[IndexedAssignedRecordTransfer]>,
    source: Option<&[&BrokerObservation]>,
    target: Option<&[&BrokerObservation]>,
) -> Vec<String> {
    let mut references = commands
        .iter()
        .map(|(sequence, _, _)| format!("history:{sequence}"))
        .collect::<Vec<_>>();
    references.extend(
        accepted
            .into_iter()
            .flatten()
            .map(|value| format!("history:{value}")),
    );
    references.extend(
        terminals
            .into_iter()
            .flatten()
            .map(|value| format!("history:{}", value.history_sequence)),
    );
    references.extend(
        transfers
            .into_iter()
            .flatten()
            .map(|value| format!("history:{}", value.history_sequence)),
    );
    references.extend(
        source
            .into_iter()
            .flatten()
            .chain(target.into_iter().flatten())
            .map(|value| format!("broker-observation:{}", value.observation)),
    );
    references
}
