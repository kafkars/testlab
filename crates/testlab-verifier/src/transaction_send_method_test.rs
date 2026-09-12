//! Transactional batch tests reject public method substitution.

use testlab_schema::{
    AdapterCommand, HistoryEntry, Scenario, ScenarioAction, TransactionSendMethod,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::command;

#[test]
fn exact_transactional_batch_command_passes() {
    let (scenario, history) = fixture(TransactionSendMethod::SendBatch);
    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn individual_or_duplicate_transaction_command_fails() {
    let (scenario, history) = fixture(TransactionSendMethod::Send);
    assert!(has_contract(&violations(&scenario, &history), "TXN-009"));

    let (scenario, mut history) = fixture(TransactionSendMethod::SendBatch);
    history.push(history[0].clone());
    assert!(has_contract(&violations(&scenario, &history), "TXN-009"));
}

fn fixture(method: TransactionSendMethod) -> (Scenario, Vec<HistoryEntry>) {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/transaction-batch-commit.toml"
    ))
    .unwrap_or_else(|error| panic!("transactional batch scenario: {error}"));
    let command_value = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::ExecuteTransaction {
                producer_id,
                transaction_id,
                operations,
                method: TransactionSendMethod::SendBatch,
                disposition,
                timeout_ms,
            } => Some(AdapterCommand::ExecuteTransaction {
                producer_id: producer_id.clone(),
                transaction_id: transaction_id.clone(),
                operations: operations.clone(),
                method,
                disposition: *disposition,
                timeout_ms: *timeout_ms,
            }),
            _ => None,
        })
        .unwrap_or_else(|| panic!("transactional batch action missing"));
    (scenario, vec![command(0, command_value)])
}

fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::transaction_send_method::verify(scenario, &index, &mut violations);
    violations
}

fn has_contract(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
