//! Group acknowledgement verification binds renewal timing to one exact command.

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
            processed_record_count,
            timeout_ms,
            ..
        } = &step.action
        else {
            continue;
        };
        if *processing_acknowledgement_delay_ms == 0 {
            continue;
        }
        let expected = AdapterCommand::GroupReceive {
            consumer_id: consumer_id.clone(),
            method: *method,
            receive_id: receive_id.clone(),
            processing_acknowledgement_delay_ms: *processing_acknowledgement_delay_ms,
            processed_record_count: *processed_record_count,
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
            "CONS-019",
            format!(
                "group receive {receive_id} selected processing acknowledgement but observed {} matching command(s) without one exact renewal plan",
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
