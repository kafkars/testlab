//! Transaction Admin protocol tests pin expectation-free exact identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminProducersFenced, AdminTransactionsDescription,
    AdminTransactionsListing, ClientId, DescribeTransactionsAction, FenceProducersAction,
    FencedProducerSnapshot, ListTransactionsAction, OperationId, ProducerFenceExpectation,
    ScenarioAction, TransactionDescriptionExpectation, TransactionDescriptionSnapshot,
    TransactionListingExpectation,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_omits_listing_and_description_expectations() {
    let Some((AdapterCommand::ListTransactions(list), expected)) =
        crate::session_command_admin_transactions::translate(&ScenarioAction::ListTransactions(
            list_action(),
        ))
    else {
        panic!("transaction-list translation");
    };
    assert_eq!(list.client_id, client());
    assert_eq!(list.operation_id, operation("list-transactions"));
    assert_eq!(list.timeout_ms, 1_000);
    assert!(matches!(expected, ExpectedEvent::TransactionsListed(_)));

    let Some((AdapterCommand::DescribeTransactions(describe), expected)) =
        crate::session_command_admin_transactions::translate(
            &ScenarioAction::DescribeTransactions(describe_action()),
        )
    else {
        panic!("transaction-description translation");
    };
    assert_eq!(
        describe.transactional_ids,
        vec!["zulu".to_owned(), "alpha".to_owned()]
    );
    let encoded = serde_json::to_string(&describe)
        .unwrap_or_else(|error| panic!("encode description command: {error}"));
    assert!(!encoded.contains("expected_state"));
    assert!(matches!(expected, ExpectedEvent::TransactionsDescribed(..)));

    let Some((AdapterCommand::FenceProducers(fence), expected)) =
        crate::session_command_admin_transactions::translate(&ScenarioAction::FenceProducers(
            fence_action(),
        ))
    else {
        panic!("producer-fencing translation");
    };
    assert_eq!(
        fence.transactional_ids,
        vec!["zulu".to_owned(), "alpha".to_owned()]
    );
    let encoded = serde_json::to_string(&fence)
        .unwrap_or_else(|error| panic!("encode producer-fencing command: {error}"));
    assert!(!encoded.contains("expected_state"));
    assert!(matches!(expected, ExpectedEvent::ProducersFenced(..)));
}

#[test]
fn completions_require_exact_operation_and_caller_order() {
    let listed = ExpectedEvent::TransactionsListed(operation("list-transactions"));
    assert_eq!(
        listed
            .classify(&AdapterEvent::TransactionsListed(
                AdminTransactionsListing {
                    operation_id: operation("list-transactions"),
                    transactions: Vec::new(),
                }
            ))
            .unwrap_or_else(|error| panic!("classify transaction list: {error}")),
        EventDisposition::Complete
    );

    let described = ExpectedEvent::TransactionsDescribed(
        operation("describe-transactions"),
        vec!["zulu".to_owned(), "alpha".to_owned()],
    );
    let mut event = AdapterEvent::TransactionsDescribed(AdminTransactionsDescription {
        operation_id: operation("describe-transactions"),
        transactions: vec![description("zulu"), description("alpha")],
    });
    assert_eq!(
        described
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify transaction descriptions: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::TransactionsDescribed(actual) = &mut event else {
        panic!("transaction description event");
    };
    actual.transactions.swap(0, 1);
    let error = described
        .classify(&event)
        .expect_err("reordered descriptions must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");

    let fenced = ExpectedEvent::ProducersFenced(
        operation("fence-producers"),
        vec!["zulu".to_owned(), "alpha".to_owned()],
    );
    let mut event = AdapterEvent::ProducersFenced(AdminProducersFenced {
        operation_id: operation("fence-producers"),
        throttle_time_ms: 3,
        producers: vec![fenced_producer("zulu"), fenced_producer("alpha")],
    });
    assert_eq!(
        fenced
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify producer fencing: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ProducersFenced(actual) = &mut event else {
        panic!("producer fencing event");
    };
    actual.producers.swap(0, 1);
    assert!(fenced.classify(&event).is_err());
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
        transactions: vec![expectation("zulu"), expectation("alpha")],
        timeout_ms: 1_000,
    }
}

fn fence_expectation(id: &str) -> ProducerFenceExpectation {
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
        producers: vec![fence_expectation("zulu"), fence_expectation("alpha")],
        timeout_ms: 1_000,
    }
}

fn listing(id: &str) -> TransactionListingExpectation {
    TransactionListingExpectation {
        transactional_id: id.to_owned(),
        expected_state: "Empty".to_owned(),
    }
}

fn expectation(id: &str) -> TransactionDescriptionExpectation {
    TransactionDescriptionExpectation {
        transactional_id: id.to_owned(),
        expected_state: "Empty".to_owned(),
        expected_transaction_timeout_ms: 30_000,
        expected_start_time_present: false,
        expected_topics: Vec::new(),
    }
}

fn description(id: &str) -> TransactionDescriptionSnapshot {
    TransactionDescriptionSnapshot {
        transactional_id: id.to_owned(),
        transaction_state: "Empty".to_owned(),
        transaction_timeout_ms: 30_000,
        transaction_start_time_ms: None,
        producer_id: 71,
        producer_epoch: 0,
        topics: Vec::new(),
    }
}

fn fenced_producer(id: &str) -> FencedProducerSnapshot {
    FencedProducerSnapshot {
        transactional_id: id.to_owned(),
        producer_id: 71,
        producer_epoch: 2,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
