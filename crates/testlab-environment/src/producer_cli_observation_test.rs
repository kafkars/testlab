//! Tests for the producer cli observation contract.

use testlab_schema::OperationId;

use super::*;

const OUTPUT: &str = "ProducerId ProducerEpoch LatestCoordinatorEpoch LastSequence LastTimestamp CurrentTransactionStartOffset\n71 2 4 0 1700000000000 None\n70 1 3 9 1699999999000 17\n";

#[test]
fn rows_are_parsed_exactly_and_canonicalized_by_producer_id() {
    let observed = normalize(7, &target(), OUTPUT.as_bytes())
        .unwrap_or_else(|error| panic!("normalize producer state: {error}"));
    let BrokerStateObservation::Producers(observed) = observed else {
        panic!("producer observation kind");
    };
    assert_eq!(observed.observation, 7);
    assert_eq!(observed.producers.len(), 2);
    assert_eq!(observed.producers[0].producer_id, 70);
    assert_eq!(
        observed.producers[0].current_transaction_start_offset,
        Some(17)
    );
    assert_eq!(observed.producers[1].last_timestamp, 1_700_000_000_000);
    assert_eq!(observed.producers[1].current_transaction_start_offset, None);
}

#[test]
fn empty_tables_are_valid_but_bad_headers_rows_and_duplicates_fail_closed() {
    let header = format!("{}\n", HEADERS.join(" "));
    assert!(normalize(1, &target(), header.as_bytes()).is_ok());
    assert!(normalize(1, &target(), b"ProducerId Bad\n").is_err());
    assert!(normalize(1, &target(), b"ProducerId ProducerEpoch LatestCoordinatorEpoch LastSequence LastTimestamp CurrentTransactionStartOffset\n1 2 3\n").is_err());
    let duplicate = format!("{header}1 0 0 -1 -1 None\n1 1 0 0 4 None\n");
    assert!(normalize(1, &target(), duplicate.as_bytes()).is_err());
}

#[test]
fn legacy_unknown_coordinator_epoch_retains_kafkas_sentinel() {
    let output = format!("{}\n1 0 -1 0 1700000000000 None\n", HEADERS.join(" "));
    let observed = normalize(1, &target(), output.as_bytes())
        .unwrap_or_else(|error| panic!("normalize legacy producer state: {error}"));
    let BrokerStateObservation::Producers(observed) = observed else {
        panic!("producer observation kind");
    };
    assert_eq!(observed.producers[0].coordinator_epoch, -1);

    let below_sentinel = output.replace(" -1 ", " -2 ");
    assert!(normalize(1, &target(), below_sentinel.as_bytes()).is_err());
}

fn target() -> ProducerTarget {
    ProducerTarget {
        operation_id: OperationId::new("admin-producers")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        topic: "orders".to_owned(),
        partition: 0,
    }
}
