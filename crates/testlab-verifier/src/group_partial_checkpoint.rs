//! Partial-checkpoint verification binds one declared prefix to one exact command.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::GroupReceive {
            consumer_id,
            method,
            receive_id,
            processing_acknowledgement_delay_ms,
            processed_record_count: Some(processed_record_count),
            timeout_ms,
            ..
        } = &step.action
        else {
            continue;
        };
        let expected = AdapterCommand::GroupReceive {
            consumer_id: consumer_id.clone(),
            method: *method,
            receive_id: receive_id.clone(),
            processing_acknowledgement_delay_ms: *processing_acknowledgement_delay_ms,
            processed_record_count: Some(*processed_record_count),
            timeout_ms: *timeout_ms,
        };
        let commands = index
            .commands
            .iter()
            .filter(|(_, _, command)| {
                matches!(
                    command,
                    AdapterCommand::GroupReceive {
                        receive_id: actual,
                        ..
                    } if actual == receive_id
                )
            })
            .collect::<Vec<_>>();
        let exact = commands.len() == 1 && matches!(&commands[0].2, actual if actual == &expected);
        if exact {
            continue;
        }
        violations.push(violation(
            "CONS-020",
            format!(
                "group receive {receive_id} selected a {processed_record_count}-record prefix but observed {} matching command(s) without that exact partial checkpoint",
                commands.len()
            ),
            Some(receive_id.clone()),
            commands
                .iter()
                .map(|(sequence, ..)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}
