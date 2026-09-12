//! Transaction-discovery verdicts pin exact fields, order, and independent timing.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTransactionsDescription, AdminTransactionsListing,
    BrokerStateObservation, BrokerTransactionListing, BrokerTransactionState,
    BrokerTransactionsState, DescribeTransactionsAction, DescribeTransactionsCommand, HistoryEntry,
    HistoryPayload, ListTransactionsAction, ListTransactionsCommand, OperationId, ScenarioAction,
    TerminalStatus, TransactionDescriptionExpectation, TransactionDescriptionSnapshot,
    TransactionListingExpectation, TransactionListingSnapshot, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

const ALPHA_ID: &str = "testlab-admin-transaction-alpha";
const ZULU_ID: &str = "testlab-admin-transaction-zulu";

#[test]
fn exact_listing_and_cli_snapshot_pass() {
    assert!(listing_violations(listing_history()).is_empty());
}

#[test]
fn listing_order_field_and_timing_mismatches_fail() {
    let mut reordered = listing_history();
    listing(&mut reordered).transactions.reverse();
    assert_contract(&listing_violations(reordered), "ADMIN-052");

    let mut wrong_coordinator = listing_history();
    listed_state(&mut wrong_coordinator).transactions[0].coordinator_id = -1;
    assert_contract(&listing_violations(wrong_coordinator), "ADMIN-052");

    let mut delayed = listing_history();
    delayed.insert(2, command(2, AdapterCommand::Finish));
    delayed[3].sequence = 3;
    assert_contract(&listing_violations(delayed), "ADMIN-052");
}

#[test]
fn exact_caller_ordered_descriptions_and_cli_snapshots_pass() {
    assert!(description_violations(description_history()).is_empty());
}

#[test]
fn description_order_field_and_sequence_mismatches_fail() {
    let mut reordered = description_history();
    descriptions(&mut reordered).transactions.reverse();
    assert_contract(&description_violations(reordered), "ADMIN-053");

    let mut wrong_timeout = description_history();
    descriptions(&mut wrong_timeout).transactions[0].transaction_timeout_ms = 29_999;
    assert_contract(&description_violations(wrong_timeout), "ADMIN-053");

    let mut noncontiguous = description_history();
    described_state(&mut noncontiguous, 1).observation = 9;
    assert_contract(&description_violations(noncontiguous), "ADMIN-053");
}

fn listing_history() -> Vec<HistoryEntry> {
    let transactions = listing_snapshots();
    vec![
        command(
            0,
            AdapterCommand::ListTransactions(ListTransactionsCommand {
                client_id: client(),
                operation_id: listing_operation(),
                state_filters: Vec::new(),
                producer_id_filters: Vec::new(),
                duration_filter_ms: None,
                transactional_id_pattern: None,
                timeout_ms: 20_000,
            }),
        ),
        event(
            1,
            AdapterEvent::TransactionsListed(AdminTransactionsListing {
                operation_id: listing_operation(),
                transactions: transactions.clone(),
            }),
        ),
        state(
            2,
            BrokerStateObservation::Transactions(BrokerTransactionsState {
                observation: 5,
                operation_id: listing_operation(),
                transactions: transactions
                    .into_iter()
                    .map(|transaction| BrokerTransactionListing {
                        coordinator_id: 1,
                        transaction,
                    })
                    .collect(),
            }),
        ),
    ]
}

fn description_history() -> Vec<HistoryEntry> {
    let descriptions = description_snapshots();
    vec![
        command(
            0,
            AdapterCommand::DescribeTransactions(DescribeTransactionsCommand {
                client_id: client(),
                operation_id: description_operation(),
                transactional_ids: vec![ZULU_ID.to_owned(), ALPHA_ID.to_owned()],
                timeout_ms: 20_000,
            }),
        ),
        event(
            1,
            AdapterEvent::TransactionsDescribed(AdminTransactionsDescription {
                operation_id: description_operation(),
                transactions: descriptions.clone(),
            }),
        ),
        state(2, broker(7, descriptions[0].clone())),
        state(3, broker(8, descriptions[1].clone())),
    ]
}

fn listing_violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let action = ScenarioAction::ListTransactions(ListTransactionsAction {
        client_id: client(),
        operation_id: listing_operation(),
        state_filters: Vec::new(),
        producer_id_filters: Vec::new(),
        duration_filter_ms: None,
        transactional_id_pattern: None,
        baseline_operation_id: None,
        expected_transactions: [ALPHA_ID, ZULU_ID].map(listing_expectation).into(),
        timeout_ms: 20_000,
    });
    violations(history, action)
}

fn description_violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let action = ScenarioAction::DescribeTransactions(DescribeTransactionsAction {
        client_id: client(),
        operation_id: description_operation(),
        transactions: [ZULU_ID, ALPHA_ID].map(description_expectation).into(),
        timeout_ms: 20_000,
    });
    violations(history, action)
}

fn violations(
    history: Vec<HistoryEntry>,
    action: ScenarioAction,
) -> Vec<testlab_schema::Violation> {
    let mut fixture = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    fixture
        .steps
        .insert(2, step("transaction-discovery", action));
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&fixture, &index, &[], &mut violations);
    violations
}

fn listing_snapshots() -> Vec<TransactionListingSnapshot> {
    vec![listing_snapshot(ALPHA_ID, 4), listing_snapshot(ZULU_ID, 9)]
}

fn listing_snapshot(transactional_id: &str, producer_id: i64) -> TransactionListingSnapshot {
    TransactionListingSnapshot {
        transactional_id: transactional_id.to_owned(),
        producer_id,
        transaction_state: "Empty".to_owned(),
    }
}

fn description_snapshots() -> Vec<TransactionDescriptionSnapshot> {
    vec![
        description_snapshot(ZULU_ID, 9),
        description_snapshot(ALPHA_ID, 4),
    ]
}

fn description_snapshot(
    transactional_id: &str,
    producer_id: i64,
) -> TransactionDescriptionSnapshot {
    TransactionDescriptionSnapshot {
        transactional_id: transactional_id.to_owned(),
        transaction_state: "Empty".to_owned(),
        transaction_timeout_ms: 30_000,
        transaction_start_time_ms: None,
        producer_id,
        producer_epoch: 0,
        topics: Vec::new(),
    }
}

fn broker(observation: u64, transaction: TransactionDescriptionSnapshot) -> BrokerStateObservation {
    BrokerStateObservation::Transaction(BrokerTransactionState {
        observation,
        operation_id: description_operation(),
        coordinator_id: 1,
        transaction,
    })
}

fn listing_expectation(transactional_id: &str) -> TransactionListingExpectation {
    TransactionListingExpectation {
        transactional_id: transactional_id.to_owned(),
        expected_state: "Empty".to_owned(),
    }
}

fn description_expectation(transactional_id: &str) -> TransactionDescriptionExpectation {
    TransactionDescriptionExpectation {
        transactional_id: transactional_id.to_owned(),
        expected_state: "Empty".to_owned(),
        expected_transaction_timeout_ms: 30_000,
        expected_start_time_present: false,
        expected_topics: Vec::new(),
    }
}

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn listing(entries: &mut [HistoryEntry]) -> &mut AdminTransactionsListing {
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("transaction listing event");
    };
    let AdapterEvent::TransactionsListed(value) = &mut event.event else {
        panic!("transaction listing payload");
    };
    value
}

fn listed_state(entries: &mut [HistoryEntry]) -> &mut BrokerTransactionsState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2].payload else {
        panic!("transaction listing observation");
    };
    let BrokerStateObservation::Transactions(value) = observation else {
        panic!("transaction listing state");
    };
    value
}

fn descriptions(entries: &mut [HistoryEntry]) -> &mut AdminTransactionsDescription {
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("transaction description event");
    };
    let AdapterEvent::TransactionsDescribed(value) = &mut event.event else {
        panic!("transaction description payload");
    };
    value
}

fn described_state(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerTransactionState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2 + index].payload
    else {
        panic!("transaction description observation");
    };
    let BrokerStateObservation::Transaction(value) = observation else {
        panic!("transaction description state");
    };
    value
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn listing_operation() -> OperationId {
    OperationId::new("admin-list-transactions").unwrap_or_else(|error| panic!("operation: {error}"))
}

fn description_operation() -> OperationId {
    OperationId::new("admin-describe-transactions")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}
