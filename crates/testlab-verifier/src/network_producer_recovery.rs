//! Producer recovery binds exact post-control commands to acknowledged terminals.

use testlab_schema::{AdapterCommand, OperationId, Scenario, ScenarioAction, TerminalStatus};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(
    scenario: &Scenario,
    position: usize,
    control_id: &testlab_schema::EnvironmentOperationId,
    index: &HistoryIndex,
    violations: &mut Vec<testlab_schema::Violation>,
) {
    let end = scenario.steps[position + 1..]
        .iter()
        .position(|step| network_control_id(&step.action).is_some())
        .map_or(scenario.steps.len(), |relative| position + 1 + relative);
    let action = scenario.steps[position + 1..end]
        .iter()
        .find_map(|step| producer_command(&step.action).map(|_| &step.action));
    let control = control_sequence(index, control_id);
    let before = scenario
        .steps
        .get(end)
        .and_then(|step| network_control_id(&step.action))
        .and_then(|operation_id| control_sequence(index, operation_id))
        .unwrap_or(u64::MAX);
    if action.is_some_and(|action| recovered(action, index, control, before)) {
        return;
    }
    violations.push(violation(
        "NET-004",
        format!(
            "network control {control_id} expected one exact later acknowledged public single or batch producer recovery"
        ),
        action.and_then(first_operation).cloned(),
        recovery_references(action, index, control),
    ));
}

fn recovered(
    action: &ScenarioAction,
    index: &HistoryIndex,
    after: Option<u64>,
    before: u64,
) -> bool {
    let operations = producer_operations(action);
    if operations.is_empty() {
        return false;
    }
    if index.commands.is_empty() {
        return operations
            .iter()
            .all(|operation| acknowledged(index, operation, after, before, false));
    }
    let Some(expected) = producer_command(action) else {
        return false;
    };
    let commands = index
        .commands
        .iter()
        .filter_map(|(sequence, _, command)| (command == &expected).then_some(*sequence))
        .collect::<Vec<_>>();
    let Some(command) = commands.first().copied().filter(|_| commands.len() == 1) else {
        return false;
    };
    after.is_some_and(|after| after < command && command < before)
        && operations
            .iter()
            .all(|operation| acknowledged(index, operation, Some(command), before, true))
}

fn acknowledged(
    index: &HistoryIndex,
    operation: &OperationId,
    after: Option<u64>,
    before: u64,
    exact: bool,
) -> bool {
    let terminals = index
        .terminals
        .get(operation)
        .map(Vec::as_slice)
        .unwrap_or_default();
    (!exact || terminals.len() == 1)
        && terminals.iter().any(|terminal| {
            terminal.status == TerminalStatus::Acknowledged
                && after.is_some_and(|after| after < terminal.history_sequence)
                && terminal.history_sequence < before
        })
}

fn producer_command(action: &ScenarioAction) -> Option<AdapterCommand> {
    match action {
        ScenarioAction::Send {
            producer_id,
            operation_id,
            method,
            partitioning,
            topic_identity_operation_id,
            record,
        } => Some(AdapterCommand::Send {
            producer_id: producer_id.clone(),
            operation_id: operation_id.clone(),
            method: *method,
            partitioning: *partitioning,
            validate_topic_uuid: topic_identity_operation_id.is_some(),
            record: record.clone(),
        }),
        ScenarioAction::SendBatch {
            producer_id,
            operations,
        } => Some(AdapterCommand::SendBatch {
            producer_id: producer_id.clone(),
            operations: operations.clone(),
        }),
        _ => None,
    }
}

fn producer_operations(action: &ScenarioAction) -> Vec<&OperationId> {
    match action {
        ScenarioAction::Send { operation_id, .. } => vec![operation_id],
        ScenarioAction::SendBatch { operations, .. } => operations
            .iter()
            .map(|operation| &operation.operation_id)
            .collect(),
        _ => Vec::new(),
    }
}

fn first_operation(action: &ScenarioAction) -> Option<&OperationId> {
    producer_operations(action).into_iter().next()
}

fn network_control_id(action: &ScenarioAction) -> Option<&testlab_schema::EnvironmentOperationId> {
    match action {
        ScenarioAction::AlterNetworkFault(action) => Some(&action.operation_id),
        ScenarioAction::CutNetworkConnections(action) => Some(&action.operation_id),
        _ => None,
    }
}

fn recovery_references(
    action: Option<&ScenarioAction>,
    index: &HistoryIndex,
    control: Option<u64>,
) -> Vec<String> {
    let mut references = control
        .map(|sequence| vec![format!("history:{sequence}")])
        .unwrap_or_default();
    if let Some(expected) = action.and_then(producer_command) {
        references.extend(index.commands.iter().filter_map(|(sequence, _, command)| {
            (command == &expected).then(|| format!("history:{sequence}"))
        }));
    }
    if let Some(action) = action {
        for operation in producer_operations(action) {
            references.extend(
                index
                    .terminals
                    .get(operation)
                    .into_iter()
                    .flatten()
                    .map(|terminal| format!("history:{}", terminal.history_sequence)),
            );
        }
    }
    references
}

fn control_sequence(
    index: &HistoryIndex,
    operation: &testlab_schema::EnvironmentOperationId,
) -> Option<u64> {
    let mut values = index
        .network_proxy_controls
        .iter()
        .filter(|(_, control)| control.operation_id() == operation);
    let (sequence, _) = values.next()?;
    values.next().is_none().then_some(*sequence)
}
