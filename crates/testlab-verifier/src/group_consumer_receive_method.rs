//! Group-consumer receive verification binds scenario selection to one public observer.

use testlab_schema::{
    AdapterCommand, GroupConsumerReceiveMethod, Scenario, ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::GroupReceive {
            consumer_id,
            method: GroupConsumerReceiveMethod::TryTakeBatch,
            receive_id,
            processing_acknowledgement_delay_ms,
            timeout_ms,
            ..
        } = &step.action
        else {
            continue;
        };
        let commands = index
            .commands
            .iter()
            .filter_map(|(sequence, _, command)| match command {
                AdapterCommand::GroupReceive {
                    consumer_id: actual_consumer,
                    method,
                    receive_id: actual_receive,
                    processing_acknowledgement_delay_ms: actual_delay,
                    timeout_ms: actual_timeout,
                } if actual_receive == receive_id => Some((
                    *sequence,
                    actual_consumer,
                    *method,
                    *actual_delay,
                    *actual_timeout,
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        let exact = matches!(
            commands.as_slice(),
            [(_, actual_consumer, GroupConsumerReceiveMethod::TryTakeBatch, actual_delay, actual_timeout)]
                if *actual_consumer == consumer_id
                    && *actual_delay == *processing_acknowledgement_delay_ms
                    && *actual_timeout == *timeout_ms
        );
        if exact {
            continue;
        }
        violations.push(violation(
            "CONS-018",
            format!(
                "group receive {receive_id} selected try_take_batch but observed {} matching command(s) without one exact immediate-batch request",
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
