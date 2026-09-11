//! Kafka transaction CLI parsing tests pin columns, ordering, and empty state.

use testlab_schema::BrokerStateObservation;

use super::*;

#[test]
fn listings_are_exact_and_canonicalized() {
    let output = "TransactionalId Coordinator ProducerId TransactionState\nzulu 1 72 Empty\nalpha 0 71 Empty\n";
    let observed = normalize_list(4, &operation("list"), output.as_bytes())
        .unwrap_or_else(|error| panic!("normalize listing: {error}"));
    let BrokerStateObservation::Transactions(observed) = observed else {
        panic!("transaction listing kind");
    };
    assert_eq!(
        observed.transactions[0].transaction.transactional_id,
        "alpha"
    );
    assert_eq!(observed.transactions[1].transaction.producer_id, 72);
}

#[test]
fn empty_and_populated_descriptions_preserve_exact_fields() {
    let empty = "CoordinatorId TransactionalId ProducerId ProducerEpoch TransactionState TransactionTimeoutMs CurrentTransactionStartTimeMs TransactionDurationMs TopicPartitions\n0 alpha 71 2 Empty 30000 None None\n";
    let observed = normalize_description(5, &operation("describe"), "alpha", empty.as_bytes())
        .unwrap_or_else(|error| panic!("normalize empty description: {error}"));
    let BrokerStateObservation::Transaction(observed) = observed else {
        panic!("transaction description kind");
    };
    assert_eq!(observed.transaction.transaction_timeout_ms, 30_000);
    assert_eq!(observed.transaction.transaction_start_time_ms, None);
    assert!(observed.transaction.topics.is_empty());

    let active = "CoordinatorId TransactionalId ProducerId ProducerEpoch TransactionState TransactionTimeoutMs CurrentTransactionStartTimeMs TransactionDurationMs TopicPartitions\n1 alpha 71 2 Ongoing 30000 1700000000000 25 zulu-1,alpha-log-2,alpha-log-0\n";
    let observed = normalize_description(6, &operation("describe"), "alpha", active.as_bytes())
        .unwrap_or_else(|error| panic!("normalize active description: {error}"));
    let BrokerStateObservation::Transaction(observed) = observed else {
        panic!("transaction description kind");
    };
    assert_eq!(observed.transaction.topics[0].topic, "alpha-log");
    assert_eq!(observed.transaction.topics[0].partitions, [0, 2]);
    assert_eq!(observed.transaction.topics[1].topic, "zulu");
}

#[test]
fn malformed_headers_rows_identity_duration_and_duplicates_fail_closed() {
    let header = DESCRIPTION_HEADERS.join(" ");
    assert!(normalize_description(1, &operation("x"), "alpha", b"Bad Header\n").is_err());
    assert!(
        normalize_description(
            1,
            &operation("x"),
            "alpha",
            format!("{header}\n0 other 1 0 Empty 30000 None None\n").as_bytes(),
        )
        .is_err()
    );
    assert!(
        normalize_description(
            1,
            &operation("x"),
            "alpha",
            format!("{header}\n0 alpha 1 0 Ongoing 30000 17 None orders-0\n").as_bytes(),
        )
        .is_err()
    );
    assert!(
        normalize_description(
            1,
            &operation("x"),
            "alpha",
            format!("{header}\n0 alpha 1 0 Ongoing 30000 17 2 orders-0,orders-0\n").as_bytes(),
        )
        .is_err()
    );
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
