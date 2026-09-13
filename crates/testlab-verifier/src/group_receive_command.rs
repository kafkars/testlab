//! Hosted group receives preserve every exact public batch request.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    let expected = scenario
        .steps
        .iter()
        .filter_map(|step| command(&step.action))
        .collect::<Vec<_>>();
    if expected.is_empty() {
        return;
    }
    let actual = index
        .commands
        .iter()
        .filter(|(_, _, command)| matches!(command, AdapterCommand::GroupReceive { .. }))
        .collect::<Vec<_>>();
    let exact = actual.len() == expected.len()
        && actual
            .iter()
            .zip(&expected)
            .all(|((_, _, actual), expected)| *actual == *expected);
    if exact {
        return;
    }
    violations.push(violation(
        "CONS-032",
        format!(
            "group receive expected {} exact ordered consumer, observer, checkpoint, processing, identity, and timeout command(s)",
            expected.len()
        ),
        None,
        actual
            .into_iter()
            .map(|(sequence, _, _)| format!("history:{sequence}"))
            .collect(),
    ));
}

fn command(action: &ScenarioAction) -> Option<AdapterCommand> {
    let ScenarioAction::GroupReceive {
        consumer_id,
        method,
        checkpoint_method,
        receive_id,
        processing_acknowledgement_delay_ms,
        processed_record_count,
        timeout_ms,
        ..
    } = action
    else {
        return None;
    };
    Some(AdapterCommand::GroupReceive {
        consumer_id: consumer_id.clone(),
        method: *method,
        checkpoint_method: *checkpoint_method,
        receive_id: receive_id.clone(),
        processing_acknowledgement_delay_ms: *processing_acknowledgement_delay_ms,
        processed_record_count: *processed_record_count,
        timeout_ms: *timeout_ms,
    })
}

#[cfg(test)]
mod tests {
    use testlab_schema::{
        AdapterCommand, ConsumerId, GroupCheckpointMethod, GroupConsumerReceiveMethod,
        HistoryEntry, HistoryPayload, OperationId, Scenario,
    };

    use super::{command, verify};
    use crate::index::HistoryIndex;
    use crate::verify_fixture::command as history_command;

    #[test]
    fn classic_receive_preserves_every_exact_request_field_once() {
        let scenario = parse("classic-group-round-trip.toml");
        let exact = receive_history(&scenario);
        assert_eq!(exact.len(), 1);
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_identity = exact.clone();
        let AdapterCommand::GroupReceive {
            consumer_id,
            receive_id,
            ..
        } = command_mut(&mut wrong_identity[0])
        else {
            panic!("group receive command");
        };
        *consumer_id = ConsumerId::new("substituted-consumer").expect("consumer ID");
        *receive_id = OperationId::new("substituted-receive").expect("receive ID");
        assert_contract(&violations(&scenario, &wrong_identity));

        let mut wrong_methods = exact.clone();
        let AdapterCommand::GroupReceive {
            method,
            checkpoint_method,
            ..
        } = command_mut(&mut wrong_methods[0])
        else {
            panic!("group receive command");
        };
        *method = GroupConsumerReceiveMethod::TryTakeBatch;
        *checkpoint_method = GroupCheckpointMethod::Checkpoint;
        assert_contract(&violations(&scenario, &wrong_methods));

        let mut wrong_processing = exact.clone();
        let AdapterCommand::GroupReceive {
            processing_acknowledgement_delay_ms,
            processed_record_count,
            timeout_ms,
            ..
        } = command_mut(&mut wrong_processing[0])
        else {
            panic!("group receive command");
        };
        *processing_acknowledgement_delay_ms += 1;
        *processed_record_count = Some(1);
        *timeout_ms += 1;
        assert_contract(&violations(&scenario, &wrong_processing));

        let duplicate_command = exact[0].clone();
        let mut duplicate = exact;
        duplicate.push(duplicate_command);
        assert_contract(&violations(&scenario, &duplicate));
    }

    #[test]
    fn consumer_protocol_receive_also_has_one_exact_command() {
        let scenario = parse("consumer-protocol-group-round-trip.toml");
        let history = receive_history(&scenario);
        assert_eq!(history.len(), 1);
        assert!(violations(&scenario, &history).is_empty());
    }

    fn receive_history(scenario: &Scenario) -> Vec<HistoryEntry> {
        scenario
            .steps
            .iter()
            .filter_map(|step| command(&step.action))
            .enumerate()
            .map(|(index, command)| history_command(index as u64, command))
            .collect()
    }

    fn parse(name: &str) -> Scenario {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scenarios/kafka")
            .join(name);
        toml::from_str(
            &std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
        )
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
    }

    fn command_mut(entry: &mut HistoryEntry) -> &mut AdapterCommand {
        let HistoryPayload::HarnessCommand { command: envelope } = &mut entry.payload else {
            panic!("command history entry");
        };
        &mut envelope.command
    }

    fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
        let mut violations = Vec::new();
        verify(scenario, &HistoryIndex::build(history), &mut violations);
        violations
    }

    fn assert_contract(violations: &[testlab_schema::Violation]) {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contract_id.as_str() == "CONS-032"),
            "{violations:?}"
        );
    }
}
