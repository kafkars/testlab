//! Transaction discovery payload tests pin trust-boundary ownership.

use super::*;
use crate::{AdapterCommand, AdapterEvent, BrokerStateObservation, ScenarioAction};

#[test]
fn transaction_discovery_versions_are_explicit() {
    assert_eq!(crate::PROTOCOL_VERSION, 115);
    assert_eq!(crate::SCENARIO_SCHEMA_VERSION, 118);
    assert_eq!(crate::EVIDENCE_SCHEMA_VERSION, 104);
}

#[test]
fn transaction_fence_methods_are_explicit_and_admin_is_declared() {
    let replacement: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/transaction-fencing.toml"
    ))
    .unwrap_or_else(|error| panic!("parse replacement fencing: {error}"));
    let mut admin: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/transaction-admin-force-termination.toml"
    ))
    .unwrap_or_else(|error| panic!("parse Admin termination: {error}"));

    assert!(matches!(
        &replacement.steps[3].action,
        ScenarioAction::FenceTransaction {
            fence_method: TransactionFenceMethod::ReplacementInitialization,
            ..
        }
    ));
    assert!(matches!(
        &admin.steps[3].action,
        ScenarioAction::FenceTransaction {
            fence_method: TransactionFenceMethod::AdminForceTermination,
            ..
        }
    ));
    round_trip(&replacement.steps[3].action);
    round_trip(&admin.steps[3].action);
    admin
        .validate()
        .unwrap_or_else(|error| panic!("validate Admin termination: {error}"));
    admin.requires.remove(&Capability::Admin);
    let error = admin
        .validate()
        .expect_err("Admin force termination must require Admin capability");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem == "admin steps require the admin capability")
    );
}

#[test]
fn expectations_stay_off_commands_and_all_results_round_trip() {
    let list_action = ScenarioAction::ListTransactions(ListTransactionsAction {
        client_id: client(),
        operation_id: operation("list-transactions"),
        state_filters: vec!["Ongoing".to_owned()],
        producer_id_filters: vec![71],
        duration_filter_ms: Some(2_000),
        transactional_id_pattern: Some("^alpha$".to_owned()),
        baseline_operation_id: Some(operation("list-transactions-baseline")),
        expected_transactions: vec![listing_expectation("alpha")],
        timeout_ms: 1_000,
    });
    let list_command = AdapterCommand::ListTransactions(ListTransactionsCommand {
        client_id: client(),
        operation_id: operation("list-transactions"),
        state_filters: vec!["Ongoing".to_owned()],
        producer_id_filters: vec![71],
        duration_filter_ms: Some(2_000),
        transactional_id_pattern: Some("^alpha$".to_owned()),
        timeout_ms: 1_000,
    });
    round_trip(&list_action);
    round_trip(&list_command);
    assert!(!encoded(&list_command).contains("expected_transactions"));
    assert!(!encoded(&list_command).contains("baseline_operation_id"));

    let listing = listing("alpha", 71);
    round_trip(&AdapterEvent::TransactionsListed(
        AdminTransactionsListing {
            operation_id: operation("list-transactions"),
            transactions: vec![listing.clone()],
        },
    ));
    round_trip(&BrokerStateObservation::Transactions(
        BrokerTransactionsState {
            observation: 3,
            operation_id: operation("list-transactions"),
            transactions: vec![BrokerTransactionListing {
                coordinator_id: 1,
                transaction: listing,
            }],
        },
    ));

    let describe_action = ScenarioAction::DescribeTransactions(DescribeTransactionsAction {
        client_id: client(),
        operation_id: operation("describe-transactions"),
        transactions: vec![description_expectation("alpha")],
        timeout_ms: 1_000,
    });
    let describe_command = AdapterCommand::DescribeTransactions(DescribeTransactionsCommand {
        client_id: client(),
        operation_id: operation("describe-transactions"),
        transactional_ids: vec!["alpha".to_owned()],
        timeout_ms: 1_000,
    });
    round_trip(&describe_action);
    round_trip(&describe_command);
    let encoded = encoded(&describe_command);
    assert!(!encoded.contains("expected_state"));
    assert!(!encoded.contains("expected_transaction_timeout_ms"));

    let description = description("alpha", 71);
    round_trip(&AdapterEvent::TransactionsDescribed(
        AdminTransactionsDescription {
            operation_id: operation("describe-transactions"),
            transactions: vec![description.clone()],
        },
    ));
    round_trip(&BrokerStateObservation::Transaction(
        BrokerTransactionState {
            observation: 4,
            operation_id: operation("describe-transactions"),
            coordinator_id: 1,
            transaction: description,
        },
    ));

    let fence_action = ScenarioAction::FenceProducers(FenceProducersAction {
        client_id: client(),
        operation_id: operation("fence-producers"),
        producers: vec![fence_expectation("alpha")],
        timeout_ms: 1_000,
    });
    let fence_command = AdapterCommand::FenceProducers(FenceProducersCommand {
        client_id: client(),
        operation_id: operation("fence-producers"),
        transactional_ids: vec!["alpha".to_owned()],
        timeout_ms: 1_000,
    });
    round_trip(&fence_action);
    round_trip(&fence_command);
    assert!(!encoded(&fence_command).contains("expected_state"));
    round_trip(&AdapterEvent::ProducersFenced(AdminProducersFenced {
        operation_id: operation("fence-producers"),
        throttle_time_ms: 7,
        producers: vec![FencedProducerSnapshot {
            transactional_id: "alpha".to_owned(),
            producer_id: 71,
            producer_epoch: 2,
        }],
    }));
}

fn fence_expectation(transactional_id: &str) -> ProducerFenceExpectation {
    ProducerFenceExpectation {
        transactional_id: transactional_id.to_owned(),
        expected_state: "Empty".to_owned(),
        expected_start_time_present: false,
        expected_topics: Vec::new(),
    }
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

fn listing(transactional_id: &str, producer_id: i64) -> TransactionListingSnapshot {
    TransactionListingSnapshot {
        transactional_id: transactional_id.to_owned(),
        producer_id,
        transaction_state: "Empty".to_owned(),
    }
}

fn description(transactional_id: &str, producer_id: i64) -> TransactionDescriptionSnapshot {
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

fn encoded<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|error| panic!("encode transaction value: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let decoded = serde_json::from_str::<T>(&encoded(value))
        .unwrap_or_else(|error| panic!("decode transaction value: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
