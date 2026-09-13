//! Ordinary producer verification preserves every exact caller-selected request.

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
        .filter(|(_, _, command)| is_producer_operation(command))
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
        "PROD-021",
        format!(
            "ordinary production expected {} exact ordered single or batch command(s)",
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

fn is_producer_operation(command: &AdapterCommand) -> bool {
    matches!(
        command,
        AdapterCommand::Send { .. } | AdapterCommand::SendBatch { .. }
    )
}

#[cfg(test)]
mod tests {
    use testlab_schema::{
        AdapterCommand, HistoryEntry, HistoryPayload, OperationId, ProducerId,
        ProducerPartitioning, ProducerSendMethod, Scenario,
    };

    use super::{command, verify};
    use crate::index::HistoryIndex;
    use crate::verify_fixture::command as history_command;

    #[test]
    fn single_send_preserves_every_exact_request_field() {
        let scenario = parse("producer-round-trip.toml");
        let exact = operation_history(&scenario);
        assert_eq!(exact.len(), 1);
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_identity = exact.clone();
        let AdapterCommand::Send {
            producer_id,
            operation_id,
            ..
        } = command_mut(&mut wrong_identity[0])
        else {
            panic!("single send command");
        };
        *producer_id = ProducerId::new("substituted-producer")
            .unwrap_or_else(|error| panic!("producer ID: {error}"));
        *operation_id = OperationId::new("substituted-operation")
            .unwrap_or_else(|error| panic!("operation ID: {error}"));
        assert_contract(&violations(&scenario, &wrong_identity));

        let mut wrong_method = exact.clone();
        let AdapterCommand::Send {
            method,
            partitioning,
            validate_topic_uuid,
            ..
        } = command_mut(&mut wrong_method[0])
        else {
            panic!("single send command");
        };
        *method = ProducerSendMethod::Send;
        *partitioning = ProducerPartitioning::JavaKeyed { partition_count: 3 };
        *validate_topic_uuid = true;
        assert_contract(&violations(&scenario, &wrong_method));

        let mut wrong_record = exact.clone();
        let AdapterCommand::Send { record, .. } = command_mut(&mut wrong_record[0]) else {
            panic!("single send command");
        };
        record.topic.push_str("-substituted");
        assert_contract(&violations(&scenario, &wrong_record));

        let duplicate_command = exact[0].clone();
        let mut duplicate = exact;
        duplicate.push(duplicate_command);
        assert_contract(&violations(&scenario, &duplicate));
    }

    #[test]
    fn batch_send_preserves_kind_producer_and_record_order() {
        let scenario = parse("producer-batch.toml");
        let exact = operation_history(&scenario);
        assert_eq!(exact.len(), 1);
        assert!(violations(&scenario, &exact).is_empty());

        let mut reordered = exact.clone();
        let AdapterCommand::SendBatch {
            producer_id,
            operations,
        } = command_mut(&mut reordered[0])
        else {
            panic!("batch send command");
        };
        *producer_id = ProducerId::new("substituted-producer")
            .unwrap_or_else(|error| panic!("producer ID: {error}"));
        operations.reverse();
        assert_contract(&violations(&scenario, &reordered));

        let mut substituted = exact;
        let AdapterCommand::SendBatch {
            producer_id,
            operations,
        } = command_mut(&mut substituted[0]).clone()
        else {
            panic!("batch send command");
        };
        let first = operations
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("batch record"));
        *command_mut(&mut substituted[0]) = AdapterCommand::Send {
            producer_id,
            operation_id: first.operation_id,
            method: ProducerSendMethod::TrySend,
            partitioning: ProducerPartitioning::Explicit,
            validate_topic_uuid: false,
            record: first.record,
        };
        assert_contract(&violations(&scenario, &substituted));
    }

    fn operation_history(scenario: &Scenario) -> Vec<HistoryEntry> {
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
                .any(|violation| violation.contract_id.as_str() == "PROD-021"),
            "{violations:?}"
        );
    }
}
