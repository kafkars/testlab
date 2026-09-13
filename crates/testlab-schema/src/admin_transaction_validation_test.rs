//! Transaction Admin validation rejects ambiguous or unbounded expectations.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ClientId, DescribeTransactionsAction, FenceProducersAction, ListTransactionsAction,
    OperationId, ProducerFenceExpectation, ScenarioAction, TransactionDescriptionExpectation,
    TransactionListingExpectation, TransactionTopicSnapshot,
};

#[test]
fn canonical_list_and_caller_ordered_descriptions_validate() {
    assert!(problems(ScenarioAction::ListTransactions(list_action())).is_empty());
    assert!(problems(ScenarioAction::DescribeTransactions(describe_action())).is_empty());
    assert!(
        problems(ScenarioAction::FenceProducers(fence_action())).is_empty(),
        "canonical producer fencing must validate"
    );
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

    let mut fence = fence_action();
    fence.producers[1].transactional_id = fence.producers[0].transactional_id.clone();
    assert!(!problems(ScenarioAction::FenceProducers(fence)).is_empty());
}

#[test]
fn listing_filters_require_bounded_unique_intent_and_a_baseline() {
    let mut action = list_action();
    action.state_filters = vec!["Ongoing".to_owned(), "Ongoing".to_owned()];
    action.producer_id_filters = vec![7, 7];
    action.duration_filter_ms = Some(i64::MAX as u64 + 1);
    action.transactional_id_pattern = Some(String::new());
    let invalid = problems(ScenarioAction::ListTransactions(action.clone()));
    for expected in [
        "state_filters must contain unique",
        "producer_id_filters must contain unique",
        "duration_filter_ms must fit",
        "transactional_id_pattern must contain",
        "filtered listing requires baseline_operation_id",
    ] {
        assert!(
            invalid.iter().any(|problem| problem.contains(expected)),
            "missing {expected:?} in {invalid:?}"
        );
    }

    action.state_filters = vec!["Ongoing".to_owned()];
    action.producer_id_filters.clear();
    action.duration_filter_ms = Some(i64::MAX as u64);
    action.transactional_id_pattern = Some("^alpha$".to_owned());
    action.baseline_operation_id = Some(operation("baseline-list-transactions"));
    action.expected_transactions.clear();
    assert!(
        problems(ScenarioAction::ListTransactions(action)).is_empty(),
        "canonical filtered listing must validate"
    );
}

fn list_action() -> ListTransactionsAction {
    ListTransactionsAction {
        client_id: client(),
        operation_id: operation("list-transactions"),
        state_filters: Vec::new(),
        producer_id_filters: Vec::new(),
        duration_filter_ms: None,
        transactional_id_pattern: None,
        baseline_operation_id: None,
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

fn fence(id: &str) -> ProducerFenceExpectation {
    ProducerFenceExpectation {
        transactional_id: id.to_owned(),
        expected_state: "Empty".to_owned(),
        expected_start_time_present: false,
        expected_topics: Vec::new(),
    }
}

fn fence_action() -> FenceProducersAction {
    FenceProducersAction {
        client_id: client(),
        operation_id: operation("fence-producers"),
        producers: vec![fence("zulu"), fence("alpha")],
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

#[allow(
    clippy::needless_pass_by_value,
    reason = "the test helper owns each constructed action fixture"
)]
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
