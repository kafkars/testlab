//! Filtered transaction listings must retain intent and prove real narrowing.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTransactionsListing, BrokerStateObservation,
    BrokerTransactionListing, BrokerTransactionsState, HistoryEntry, HistoryPayload,
    ListTransactionsAction, ListTransactionsCommand, OperationId, ScenarioAction, TerminalStatus,
    TransactionListingExpectation, TransactionListingSnapshot, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn each_transaction_listing_filter_passes_with_stable_narrowing() {
    for action in filter_actions() {
        assert!(
            violations(&action, history(&action)).is_empty(),
            "{action:?}"
        );
    }
}

#[test]
fn substituted_filter_and_ignored_filter_fail() {
    let action = filter_actions()[0].clone();
    let mut substituted = history(&action);
    filtered_command(&mut substituted).state_filters.clear();
    assert_contract(&violations(&action, substituted));

    let mut ignored = history(&action);
    filtered_listing(&mut ignored).transactions = full_listing();
    assert_contract(&violations(&action, ignored));
}

#[test]
fn substituted_result_missing_baseline_and_delayed_snapshot_fail() {
    let action = filter_actions()[3].clone();
    let mut substituted = history(&action);
    filtered_listing(&mut substituted).transactions = vec![snapshot(ZULU, 9)];
    assert_contract(&violations(&action, substituted));

    let mut missing = action.clone();
    missing.baseline_operation_id = Some(operation("missing-baseline"));
    assert_contract(&violations(&missing, history(&action)));

    let mut delayed = history(&action);
    delayed.insert(5, command(5, AdapterCommand::Finish));
    delayed[6].sequence = 6;
    assert_contract(&violations(&action, delayed));
}

fn filter_actions() -> [ListTransactionsAction; 4] {
    let mut state = filtered_action();
    state.state_filters = vec!["Ongoing".to_owned()];
    let mut producer = filtered_action();
    producer.producer_id_filters = vec![i64::MAX];
    let mut duration = filtered_action();
    duration.duration_filter_ms = Some(i64::MAX as u64);
    let mut pattern = filtered_action();
    pattern.transactional_id_pattern = Some("^alpha$".to_owned());
    pattern.expected_transactions = vec![expectation(ALPHA)];
    [state, producer, duration, pattern]
}

fn filtered_action() -> ListTransactionsAction {
    ListTransactionsAction {
        client_id: client(),
        operation_id: operation("filtered-listing"),
        state_filters: Vec::new(),
        producer_id_filters: Vec::new(),
        duration_filter_ms: None,
        transactional_id_pattern: None,
        baseline_operation_id: Some(operation("baseline-listing")),
        expected_transactions: Vec::new(),
        timeout_ms: 1_000,
    }
}

fn baseline_action() -> ListTransactionsAction {
    ListTransactionsAction {
        client_id: client(),
        operation_id: operation("baseline-listing"),
        state_filters: Vec::new(),
        producer_id_filters: Vec::new(),
        duration_filter_ms: None,
        transactional_id_pattern: None,
        baseline_operation_id: None,
        expected_transactions: vec![expectation(ALPHA), expectation(ZULU)],
        timeout_ms: 1_000,
    }
}

fn history(action: &ListTransactionsAction) -> Vec<HistoryEntry> {
    let baseline = baseline_action();
    let full = full_listing();
    let selected = action
        .expected_transactions
        .iter()
        .filter_map(|expected| {
            full.iter()
                .find(|row| row.transactional_id == expected.transactional_id)
                .cloned()
        })
        .collect();
    vec![
        command(0, AdapterCommand::ListTransactions(list_command(&baseline))),
        listed(1, &baseline.operation_id, full.clone()),
        observed(2, 7, &baseline.operation_id, full.clone()),
        command(3, AdapterCommand::ListTransactions(list_command(action))),
        listed(4, &action.operation_id, selected),
        observed(5, 8, &action.operation_id, full),
    ]
}

fn list_command(action: &ListTransactionsAction) -> ListTransactionsCommand {
    ListTransactionsCommand {
        client_id: action.client_id.clone(),
        operation_id: action.operation_id.clone(),
        state_filters: action.state_filters.clone(),
        producer_id_filters: action.producer_id_filters.clone(),
        duration_filter_ms: action.duration_filter_ms,
        transactional_id_pattern: action.transactional_id_pattern.clone(),
        timeout_ms: action.timeout_ms,
    }
}

fn listed(
    sequence: u64,
    operation: &OperationId,
    transactions: Vec<TransactionListingSnapshot>,
) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::TransactionsListed(AdminTransactionsListing {
            operation_id: operation.clone(),
            transactions,
        }),
    )
}

fn observed(
    sequence: u64,
    observation: u64,
    operation: &OperationId,
    transactions: Vec<TransactionListingSnapshot>,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::Transactions(BrokerTransactionsState {
                observation,
                operation_id: operation.clone(),
                transactions: transactions
                    .into_iter()
                    .map(|transaction| BrokerTransactionListing {
                        coordinator_id: 1,
                        transaction,
                    })
                    .collect(),
            }),
        },
    }
}

fn violations(
    action: &ListTransactionsAction,
    history: Vec<HistoryEntry>,
) -> Vec<testlab_schema::Violation> {
    let mut fixture = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    fixture.steps.insert(
        2,
        step(
            "baseline-transaction-listing",
            ScenarioAction::ListTransactions(baseline_action()),
        ),
    );
    fixture.steps.insert(
        3,
        step(
            "filtered-transaction-listing",
            ScenarioAction::ListTransactions(action.clone()),
        ),
    );
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&fixture, &index, &[], &mut violations);
    violations
}

fn filtered_command(history: &mut [HistoryEntry]) -> &mut ListTransactionsCommand {
    let HistoryPayload::HarnessCommand { command } = &mut history[3].payload else {
        panic!("filtered command")
    };
    let AdapterCommand::ListTransactions(command) = &mut command.command else {
        panic!("filtered listing command")
    };
    command
}

fn filtered_listing(history: &mut [HistoryEntry]) -> &mut AdminTransactionsListing {
    let HistoryPayload::AdapterEvent { event } = &mut history[4].payload else {
        panic!("filtered event")
    };
    let AdapterEvent::TransactionsListed(listing) = &mut event.event else {
        panic!("filtered listing event")
    };
    listing
}

fn full_listing() -> Vec<TransactionListingSnapshot> {
    vec![snapshot(ALPHA, 4), snapshot(ZULU, 9)]
}

fn snapshot(id: &str, producer_id: i64) -> TransactionListingSnapshot {
    TransactionListingSnapshot {
        transactional_id: id.to_owned(),
        producer_id,
        transaction_state: "Empty".to_owned(),
    }
}

fn expectation(id: &str) -> TransactionListingExpectation {
    TransactionListingExpectation {
        transactional_id: id.to_owned(),
        expected_state: "Empty".to_owned(),
    }
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-083"),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}

const ALPHA: &str = "alpha";
const ZULU: &str = "zulu";
