//! Automatic producer routing joins scenario intent, protocol input, and broker truth.

use testlab_schema::{
    AdapterCommand, BrokerObservation, OperationId, ProducerPartitioning, Scenario, ScenarioAction,
    TerminalStatus, Violation,
};

use crate::index::HistoryIndex;
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
        let ScenarioAction::Send {
            operation_id,
            partitioning: partitioning @ ProducerPartitioning::JavaKeyed { .. },
            record,
            ..
        } = &step.action
        else {
            continue;
        };
        if !index.action_issued(&step.action) {
            continue;
        }
        let expected = match partitioning.expected_partition(record) {
            Ok(partition) => partition,
            Err(error) => {
                violations.push(violation(
                    "PROD-015",
                    format!("automatic partition oracle rejected {operation_id}: {error}"),
                    Some(operation_id.clone()),
                    Vec::new(),
                ));
                continue;
            }
        };
        verify_one(
            operation_id,
            *partitioning,
            expected,
            index,
            observed.get(operation_id).map(Vec::as_slice),
            violations,
        );
    }
}

fn verify_one(
    operation_id: &OperationId,
    partitioning: ProducerPartitioning,
    expected: i32,
    index: &HistoryIndex,
    observations: Option<&[&BrokerObservation]>,
    violations: &mut Vec<Violation>,
) {
    let commands = index
        .commands
        .iter()
        .filter_map(|(sequence, _, command)| match command {
            AdapterCommand::Send {
                operation_id: actual,
                partitioning,
                ..
            } if actual == operation_id => Some((*sequence, *partitioning)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let terminal = index
        .terminals
        .get(operation_id)
        .and_then(|values| match values.as_slice() {
            [terminal] => Some(terminal),
            _ => None,
        });
    let observation = observations.and_then(|values| match values {
        [observation] => Some(*observation),
        _ => None,
    });
    let valid_command = matches!(commands.as_slice(), [(_, actual)] if *actual == partitioning);
    let valid_terminal = terminal.is_some_and(|value| {
        matches!(
            value.status,
            TerminalStatus::Acknowledged | TerminalStatus::TransactionStaged
        ) && value.partition == Some(expected)
    });
    let valid_observation = observation.is_some_and(|value| value.record.partition == expected);
    if valid_command && valid_terminal && valid_observation {
        return;
    }
    let mut evidence = commands
        .iter()
        .map(|(sequence, _)| format!("history:{sequence}"))
        .collect::<Vec<_>>();
    if let Some(terminal) = terminal {
        evidence.push(format!("history:{}", terminal.history_sequence));
    }
    if let Some(observation) = observation {
        evidence.push(format!("broker-observation:{}", observation.observation));
    }
    violations.push(violation(
        "PROD-015",
        format!(
            "Java-keyed operation {operation_id} expected partition {expected}; observed command modes {commands:?}, public terminal {:?}, and broker partition {:?}",
            terminal.and_then(|value| value.partition),
            observation.map(|value| value.record.partition)
        ),
        Some(operation_id.clone()),
        evidence,
    ));
}
