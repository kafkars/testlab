//! Expected group-receive failures retain configured policy and command correlation.

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
            expected_error_code: Some(expected),
            timeout_ms,
            ..
        } = &step.action
        else {
            continue;
        };
        let creations = scenario
            .steps
            .iter()
            .filter_map(|candidate| match &candidate.action {
                action @ ScenarioAction::CreateGroupConsumer {
                    consumer_id: actual,
                    ..
                } if actual == consumer_id => Some(action),
                _ => None,
            })
            .collect::<Vec<_>>();
        let creation_commands = index
            .commands
            .iter()
            .filter_map(|(sequence, _, command)| match command {
                AdapterCommand::CreateGroupConsumer {
                    consumer_id: actual,
                    ..
                } if actual == consumer_id => Some((*sequence, command)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let receive_commands = index
            .commands
            .iter()
            .filter_map(|(sequence, command_id, command)| match command {
                AdapterCommand::GroupReceive {
                    consumer_id: actual_consumer,
                    method: actual_method,
                    receive_id: actual_receive,
                    processing_acknowledgement_delay_ms: actual_delay,
                    processed_record_count: actual_processed_count,
                    timeout_ms: actual_timeout,
                } if actual_receive == receive_id => Some((
                    *sequence,
                    command_id.clone(),
                    actual_consumer.clone(),
                    *actual_method,
                    actual_receive.clone(),
                    *actual_delay,
                    *actual_processed_count,
                    *actual_timeout,
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        let creation = matches!(
            (creations.as_slice(), creation_commands.as_slice()),
            ([action], [(_, command)]) if creation_matches(action, command)
        );
        let receive = match receive_commands.as_slice() {
            [
                (
                    sequence,
                    command_id,
                    actual_consumer,
                    actual_method,
                    actual_receive,
                    actual_delay,
                    actual_processed_count,
                    actual_timeout,
                ),
            ] if actual_consumer == consumer_id
                && actual_method == method
                && actual_receive == receive_id
                && actual_delay == processing_acknowledgement_delay_ms
                && actual_processed_count == processed_record_count
                && actual_timeout == timeout_ms =>
            {
                Some((*sequence, command_id))
            }
            _ => None,
        };
        let failures = receive.map_or_else(Vec::new, |(_, command_id)| {
            index
                .command_failures
                .iter()
                .filter(|failure| &failure.command_id == command_id)
                .collect::<Vec<_>>()
        });
        let successful = index.receives.get(receive_id).map_or(0, Vec::len);
        let ordered_creation = match (creation_commands.as_slice(), receive) {
            ([(creation_sequence, _)], Some((receive_sequence, _))) => {
                creation && *creation_sequence < receive_sequence
            }
            _ => false,
        };
        let exact_failure = matches!(
            (receive, failures.as_slice()),
            (Some((command_sequence, _)), [failure])
                if failure.code == expected.as_str() && command_sequence < failure.history_sequence
        );
        if ordered_creation && exact_failure && successful == 0 {
            continue;
        }
        let mut evidence = creation_commands
            .iter()
            .map(|(sequence, ..)| format!("history:{sequence}"))
            .collect::<Vec<_>>();
        evidence.extend(
            receive_commands
                .iter()
                .map(|(sequence, ..)| format!("history:{sequence}")),
        );
        evidence.extend(
            failures
                .iter()
                .map(|failure| format!("history:{}", failure.history_sequence)),
        );
        evidence.extend(
            index
                .receives
                .get(receive_id)
                .into_iter()
                .flatten()
                .map(|value| format!("history:{}", value.history_sequence)),
        );
        violations.push(violation(
            "CONS-017",
            format!(
                "group receive {receive_id} expected one configured, correlated {expected} failure and no successful receive; observed {} creation command(s), {} receive command(s), {} failure event(s), and {successful} successful receive(s)",
                creation_commands.len(),
                receive_commands.len(),
                failures.len()
            ),
            Some(receive_id.clone()),
            evidence,
        ));
    }
}

fn creation_matches(action: &ScenarioAction, command: &AdapterCommand) -> bool {
    match (action, command) {
        (
            ScenarioAction::CreateGroupConsumer {
                client_id,
                consumer_id,
                group_id,
                topics,
                protocol,
                configuration,
            },
            AdapterCommand::CreateGroupConsumer {
                client_id: actual_client,
                consumer_id: actual_consumer,
                group_id: actual_group,
                topics: actual_topics,
                protocol: actual_protocol,
                configuration: actual_configuration,
            },
        ) => {
            client_id == actual_client
                && consumer_id == actual_consumer
                && group_id == actual_group
                && topics == actual_topics
                && protocol == actual_protocol
                && configuration == actual_configuration
        }
        _ => false,
    }
}
