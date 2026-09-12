//! Producer send verification binds scenario selection to the exact public method command.

use testlab_schema::{AdapterCommand, ProducerSendMethod, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::Send {
            producer_id,
            operation_id,
            method: ProducerSendMethod::Send,
            partitioning,
            record,
        } = &step.action
        else {
            continue;
        };
        let commands = index
            .commands
            .iter()
            .filter_map(|(sequence, _, command)| match command {
                AdapterCommand::Send {
                    producer_id: actual_producer,
                    operation_id: actual_operation,
                    method,
                    partitioning: actual_partitioning,
                    record: actual_record,
                } if actual_operation == operation_id => Some((
                    *sequence,
                    actual_producer,
                    *method,
                    *actual_partitioning,
                    actual_record,
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        let exact = matches!(
            commands.as_slice(),
            [(_, actual_producer, ProducerSendMethod::Send, actual_partitioning, actual_record)]
                if *actual_producer == producer_id
                    && *actual_partitioning == *partitioning
                    && *actual_record == record
        );
        if exact {
            continue;
        }
        violations.push(violation(
            "PROD-016",
            format!(
                "operation {operation_id} selected Producer::send but observed {} matching command(s) without one exact waiting-send request",
                commands.len()
            ),
            Some(operation_id.clone()),
            commands
                .iter()
                .map(|(sequence, ..)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}
