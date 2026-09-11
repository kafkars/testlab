//! Transaction Admin validation rejects ambiguous or unbounded expectations.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ClientId, DescribeTransactionsAction, ListTransactionsAction, OperationId, ScenarioAction,
    TransactionDescriptionExpectation, TransactionListingExpectation, TransactionTopicSnapshot,
};

#[test]
fn canonical_list_and_caller_ordered_descriptions_validate() {
    assert!(problems(ScenarioAction::ListTransactions(list_action())).is_empty());
    assert!(problems(ScenarioAction::DescribeTransactions(describe_action())).is_empty());
}

#[test]
fn duplicate_ids_whitespace_and_noncanonical_topics_fail() {
    let mut list = list_action();
    list.expected_transactions.reverse();
    assert!(!problems(ScenarioAction::ListTransactions(list)).is_empty());

    let mut describe = describe_action();
    describe.transactions[1].transactional_id = "transaction z".to_owned();
    describe.transactions[1].expected_topics = vec![
        TransactionTopicSnapshot {
            topic: "zulu".to_owned(),
            partitions: vec![1, 0],
        },
        TransactionTopicSnapshot {
            topic: "alpha".to_owned(),
            partitions: vec![0],
        },
    ];
    assert!(!problems(ScenarioAction::DescribeTransactions(describe)).is_empty());
}

fn list_action() -> ListTransactionsAction {
    ListTransactionsAction {
        client_id: client(),
        operation_id: operation("list-transactions"),
        expected_transactions: vec![listing("alpha"), listing("zulu")],
        timeout_ms: 1_000,
    }
}

fn describe_action() -> DescribeTransactionsAction {
    DescribeTransactionsAction {
        client_id: client(),
        operation_id: operation("describe-transactions"),
        transactions: vec![description("zulu"), description("alpha")],
        timeout_ms: 1_000,
    }
}

fn listing(id: &str) -> TransactionListingExpectation {
    TransactionListingExpectation {
        transactional_id: id.to_owned(),
        expected_state: "Empty".to_owned(),
    }
}

fn description(id: &str) -> TransactionDescriptionExpectation {
    TransactionDescriptionExpectation {
        transactional_id: id.to_owned(),
        expected_state: "Empty".to_owned(),
        expected_transaction_timeout_ms: 30_000,
        expected_start_time_present: false,
        expected_topics: Vec::new(),
    }
}

fn problems(action: ScenarioAction) -> Vec<String> {
    let mut problems = Vec::new();
    super::validate(
        &action,
        &BTreeMap::from([(client(), false)]),
        &mut BTreeSet::new(),
        &mut problems,
    );
    problems
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
