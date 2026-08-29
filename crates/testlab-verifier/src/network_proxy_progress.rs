//! Network progress contracts bind fault windows to packaged-client outcomes.

use testlab_schema::{
    NetworkFault, NetworkFaultState, NetworkProxyControl, OperationId, Scenario, ScenarioAction,
    TerminalStatus, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for (position, step) in scenario.steps.iter().enumerate() {
        match &step.action {
            ScenarioAction::AlterNetworkFault(action)
                if action.state == NetworkFaultState::Present =>
            {
                verify_window(scenario, position, action, index, violations);
            }
            ScenarioAction::AlterNetworkFault(action)
                if action.state == NetworkFaultState::Absent =>
            {
                verify_recovery(scenario, position, &action.operation_id, index, violations);
            }
            ScenarioAction::CutNetworkConnections(action) => {
                verify_recovery(scenario, position, &action.operation_id, index, violations);
            }
            _ => {}
        }
    }
}

fn verify_window(
    scenario: &Scenario,
    position: usize,
    action: &testlab_schema::NetworkFaultAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let Some(remove_position) = scenario
        .steps
        .iter()
        .enumerate()
        .skip(position + 1)
        .find_map(|(index, step)| match &step.action {
            ScenarioAction::AlterNetworkFault(remove)
                if remove.state == NetworkFaultState::Absent
                    && remove.broker_ordinal == action.broker_ordinal
                    && remove.fault == action.fault =>
            {
                Some(index)
            }
            _ => None,
        })
    else {
        return;
    };
    let apply_sequence = control_sequence(index, &action.operation_id);
    let remove_id = match &scenario.steps[remove_position].action {
        ScenarioAction::AlterNetworkFault(remove) => &remove.operation_id,
        _ => return,
    };
    let remove_sequence = control_sequence(index, remove_id);
    let expected = match action.fault {
        NetworkFault::Blackhole => TerminalStatus::PossiblySent,
        NetworkFault::Delay { .. } => TerminalStatus::Acknowledged,
    };
    let progress = scenario.steps[position + 1..remove_position]
        .iter()
        .filter_map(|step| send_operation(&step.action))
        .find_map(|operation| terminal_in_window(index, operation, apply_sequence, remove_sequence))
        .filter(|terminal| terminal.status == expected);
    if progress.is_some() {
        return;
    }
    violations.push(violation(
        "NET-003",
        format!(
            "network fault {} expected public {expected:?} producer behavior inside its exact active window",
            action.operation_id
        ),
        None,
        window_references(index, &action.operation_id, remove_id),
    ));
}

fn verify_recovery(
    scenario: &Scenario,
    position: usize,
    control_id: &testlab_schema::EnvironmentOperationId,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let control_sequence = control_sequence(index, control_id);
    let operation = scenario.steps[position + 1..]
        .iter()
        .find_map(|step| send_operation(&step.action));
    let recovered = operation
        .and_then(|operation| index.terminals.get(operation))
        .into_iter()
        .flatten()
        .find(|terminal| {
            terminal.status == TerminalStatus::Acknowledged
                && control_sequence.is_some_and(|control| control < terminal.history_sequence)
        });
    if recovered.is_some() {
        return;
    }
    violations.push(violation(
        "NET-004",
        format!(
            "network control {control_id} expected later acknowledged public producer recovery"
        ),
        operation.cloned(),
        control_sequence
            .map(|sequence| vec![format!("history:{sequence}")])
            .unwrap_or_default(),
    ));
}

fn send_operation(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::Send { operation_id, .. } => Some(operation_id),
        _ => None,
    }
}

fn terminal_in_window<'a>(
    index: &'a HistoryIndex,
    operation: &OperationId,
    start: Option<u64>,
    end: Option<u64>,
) -> Option<&'a crate::index::IndexedTerminal> {
    let command = command_sequence(index, operation)?;
    index.terminals.get(operation)?.iter().find(|terminal| {
        start.zip(end).is_some_and(|(start, end)| {
            start < command
                && command < terminal.history_sequence
                && terminal.history_sequence < end
        })
    })
}

fn command_sequence(index: &HistoryIndex, operation: &OperationId) -> Option<u64> {
    index
        .commands
        .iter()
        .find_map(|(sequence, _, command)| match command {
            testlab_schema::AdapterCommand::Send {
                operation_id: actual,
                ..
            } if actual == operation => Some(*sequence),
            _ => None,
        })
}

fn control_sequence(
    index: &HistoryIndex,
    operation: &testlab_schema::EnvironmentOperationId,
) -> Option<u64> {
    index
        .network_proxy_controls
        .iter()
        .find_map(|(sequence, control)| (control.operation_id() == operation).then_some(*sequence))
}

fn window_references(
    index: &HistoryIndex,
    apply: &testlab_schema::EnvironmentOperationId,
    remove: &testlab_schema::EnvironmentOperationId,
) -> Vec<String> {
    index
        .network_proxy_controls
        .iter()
        .filter(|(_, control)| {
            matches!(control, NetworkProxyControl::AlterFault(action) if &action.operation_id == apply || &action.operation_id == remove)
        })
        .map(|(sequence, _)| format!("history:{sequence}"))
        .collect()
}
