//! Network consumer contracts bind recovery controls to later public records.

use testlab_schema::{AdapterCommand, OperationId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    for (position, step) in scenario.steps.iter().enumerate() {
        let Some(control_id) = recovery_control(&step.action) else {
            continue;
        };
        let end = scenario.steps[position + 1..]
            .iter()
            .position(|step| is_network_control(&step.action))
            .map_or(scenario.steps.len(), |relative| position + 1 + relative);
        for step in &scenario.steps[position + 1..end] {
            match &step.action {
                ScenarioAction::Receive { receive_id, .. } => verify_receive(
                    receive_id,
                    ReceiveKind::Assigned,
                    control_id,
                    index,
                    violations,
                ),
                ScenarioAction::GroupReceive {
                    receive_id,
                    expected_error_code: None,
                    ..
                } => verify_receive(
                    receive_id,
                    ReceiveKind::Group,
                    control_id,
                    index,
                    violations,
                ),
                ScenarioAction::ShareReceive { receive_id, .. } => verify_receive(
                    receive_id,
                    ReceiveKind::Share,
                    control_id,
                    index,
                    violations,
                ),
                _ => {}
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ReceiveKind {
    Assigned,
    Group,
    Share,
}

fn verify_receive(
    receive_id: &OperationId,
    kind: ReceiveKind,
    control_id: &testlab_schema::EnvironmentOperationId,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let control = control_sequence(index, control_id);
    let commands = index
        .commands
        .iter()
        .filter_map(|(sequence, _, command)| {
            command_matches(command, receive_id, kind).then_some(*sequence)
        })
        .collect::<Vec<_>>();
    let completions = receive_completions(index, receive_id, kind);
    let recovered = commands.first().zip(completions.first()).is_some_and(
        |(command, (completion, progressed))| {
            commands.len() == 1
                && completions.len() == 1
                && *progressed
                && control.is_some_and(|control| control < *command && *command < *completion)
        },
    );
    if recovered {
        return;
    }
    let mut references = control
        .map(|sequence| vec![format!("history:{sequence}")])
        .unwrap_or_default();
    references.extend(
        commands
            .iter()
            .map(|sequence| format!("history:{sequence}")),
    );
    references.extend(
        completions
            .iter()
            .map(|(sequence, _)| format!("history:{sequence}")),
    );
    violations.push(violation(
        "NET-005",
        format!(
            "network control {control_id} expected exact nonempty public {} recovery for receive {receive_id}",
            receive_surface(kind)
        ),
        Some(receive_id.clone()),
        references,
    ));
}

fn command_matches(command: &AdapterCommand, receive_id: &OperationId, kind: ReceiveKind) -> bool {
    match (kind, command) {
        (
            ReceiveKind::Assigned,
            AdapterCommand::Receive {
                receive_id: actual, ..
            },
        )
        | (
            ReceiveKind::Group,
            AdapterCommand::GroupReceive {
                receive_id: actual, ..
            },
        )
        | (
            ReceiveKind::Share,
            AdapterCommand::ShareReceive {
                receive_id: actual, ..
            },
        ) => actual == receive_id,
        _ => false,
    }
}

fn receive_completions(
    index: &HistoryIndex,
    receive_id: &OperationId,
    kind: ReceiveKind,
) -> Vec<(u64, bool)> {
    match kind {
        ReceiveKind::Assigned | ReceiveKind::Group => index
            .receives
            .get(receive_id)
            .into_iter()
            .flatten()
            .map(|receive| {
                let committed = match kind {
                    ReceiveKind::Assigned => receive.committed.is_none(),
                    ReceiveKind::Group => receive.committed == Some(true),
                    ReceiveKind::Share => false,
                };
                (
                    receive.history_sequence,
                    !receive.records.is_empty() && committed,
                )
            })
            .collect(),
        ReceiveKind::Share => index
            .share_receives
            .get(receive_id)
            .into_iter()
            .flatten()
            .map(|receive| (receive.history_sequence, !receive.records.is_empty()))
            .collect(),
    }
}

fn receive_surface(kind: ReceiveKind) -> &'static str {
    match kind {
        ReceiveKind::Assigned => "assigned-consumer",
        ReceiveKind::Group => "group-consumer",
        ReceiveKind::Share => "Share-consumer",
    }
}

fn recovery_control(action: &ScenarioAction) -> Option<&testlab_schema::EnvironmentOperationId> {
    match action {
        ScenarioAction::AlterNetworkFault(action)
            if action.state == testlab_schema::NetworkFaultState::Absent =>
        {
            Some(&action.operation_id)
        }
        ScenarioAction::CutNetworkConnections(action) => Some(&action.operation_id),
        _ => None,
    }
}

fn is_network_control(action: &ScenarioAction) -> bool {
    matches!(
        action,
        ScenarioAction::AlterNetworkFault(_) | ScenarioAction::CutNetworkConnections(_)
    )
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
