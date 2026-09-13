//! Transaction command regressions cover ordinary, transform, and fence requests.

use testlab_schema::{
    AdapterCommand, ConsumerId, HistoryEntry, HistoryPayload, OperationId, ProducerId, Scenario,
    TransactionDisposition, TransactionFenceMethod, TransactionSendMethod,
};

use super::{command, verify};
use crate::index::HistoryIndex;
use crate::verify_fixture::command as history_command;

#[test]
fn successive_transactions_preserve_exact_order_and_fields() {
    let scenario = parse("transaction-successive-boundaries.toml");
    let exact = transaction_history(&scenario);
    assert_eq!(exact.len(), 3);
    assert!(violations(&scenario, &exact).is_empty());

    let mut wrong_identity = exact.clone();
    let AdapterCommand::ExecuteTransaction {
        producer_id,
        transaction_id,
        ..
    } = command_mut(&mut wrong_identity[0])
    else {
        panic!("execute transaction command");
    };
    *producer_id = ProducerId::new("substituted-producer").expect("producer ID");
    *transaction_id = OperationId::new("substituted-transaction").expect("transaction ID");
    assert_contract(&violations(&scenario, &wrong_identity));

    let mut wrong_request = exact.clone();
    let AdapterCommand::ExecuteTransaction {
        operations,
        method,
        disposition,
        validate_topic_uuids,
        timeout_ms,
        ..
    } = command_mut(&mut wrong_request[1])
    else {
        panic!("execute transaction command");
    };
    operations.clear();
    *method = TransactionSendMethod::SendBatch;
    *disposition = TransactionDisposition::Commit;
    *validate_topic_uuids = true;
    *timeout_ms += 1;
    assert_contract(&violations(&scenario, &wrong_request));

    let mut reordered = exact.clone();
    reordered.swap(0, 1);
    assert_contract(&violations(&scenario, &reordered));

    let duplicate_command = exact[0].clone();
    let mut duplicate = exact;
    duplicate.push(duplicate_command);
    assert_contract(&violations(&scenario, &duplicate));
}

#[test]
fn transactional_transforms_preserve_exact_public_input() {
    let scenario = parse("transactional-offset-classic.toml");
    let exact = transaction_history(&scenario);
    assert_eq!(exact.len(), 2);
    assert!(violations(&scenario, &exact).is_empty());

    let mut wrong = exact;
    let AdapterCommand::ExecuteTransactionalTransform(command) = command_mut(&mut wrong[0]) else {
        panic!("transactional transform command");
    };
    command.producer_id = ProducerId::new("substituted-producer").expect("producer ID");
    command.consumer_id = ConsumerId::new("substituted-consumer").expect("consumer ID");
    command.transaction_id = OperationId::new("substituted-transaction").expect("transaction ID");
    command.operations.clear();
    command.disposition = TransactionDisposition::Abort;
    command.timeout_ms += 1;
    assert_contract(&violations(&scenario, &wrong));
}

#[test]
fn fence_requests_preserve_exact_method_and_replacement_policy() {
    let scenario = parse("transaction-fencing.toml");
    let exact = transaction_history(&scenario);
    assert_eq!(exact.len(), 4);
    assert!(violations(&scenario, &exact).is_empty());

    let mut wrong = exact;
    let AdapterCommand::FenceTransaction {
        fence_method,
        replacement_producer_id,
        transaction_timeout_ms,
        initialization_timeout_ms,
        timeout_ms,
        ..
    } = command_mut(&mut wrong[0])
    else {
        panic!("fence transaction command");
    };
    *fence_method = TransactionFenceMethod::AdminForceTermination;
    *replacement_producer_id = ProducerId::new("substituted-replacement").expect("producer ID");
    *transaction_timeout_ms += 1;
    *initialization_timeout_ms += 1;
    *timeout_ms += 1;
    assert_contract(&violations(&scenario, &wrong));
}

fn transaction_history(scenario: &Scenario) -> Vec<HistoryEntry> {
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
    let HistoryPayload::Command(envelope) = &mut entry.payload else {
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
            .any(|violation| violation.contract_id.as_str() == "TXN-012"),
        "{violations:?}"
    );
}
