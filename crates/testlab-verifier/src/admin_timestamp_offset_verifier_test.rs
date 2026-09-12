//! Timestamp offset evidence rejects selector and record substitutions.

use testlab_schema::{
    AdapterEvent, AdminOffsetListing, AdminOffsetSelector, BrokerObservation, ByteString,
    ListOffsetsAction, RecordSpec, ScenarioAction,
};

const TIMESTAMP: i64 = 1_700_000_000_123;

#[test]
fn timestamp_offset_matches_exact_public_and_independent_record_truth() {
    let scenario = timestamp_scenario();
    let history = history(Some(TIMESTAMP));
    let observations = [observed(0, TIMESTAMP - 1), observed(1, TIMESTAMP)];
    assert!(super::admin_violations(&scenario, &history, &observations).is_empty());
}

#[test]
fn timestamp_offset_rejects_wrong_public_timestamp_or_missing_prior_record() {
    let scenario = timestamp_scenario();
    let observations = [observed(0, TIMESTAMP - 1), observed(1, TIMESTAMP)];
    super::assert_contract(
        &super::admin_violations(&scenario, &history(Some(TIMESTAMP + 1)), &observations),
        "ADMIN-077",
    );
    super::assert_contract(
        &super::admin_violations(&scenario, &history(Some(TIMESTAMP)), &observations[1..]),
        "ADMIN-077",
    );
}

fn timestamp_scenario() -> testlab_schema::Scenario {
    super::admin_scenario(ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: super::client(),
        operation_id: super::operation("timestamp-offset"),
        topic: "offsets".to_owned(),
        partition: 0,
        position: AdminOffsetSelector::Timestamp,
        timestamp_millis: Some(TIMESTAMP),
        expected_offset: Some(1),
        expected_error_code: None,
        timeout_ms: 1_000,
    }))
}

fn history(timestamp_millis: Option<i64>) -> [testlab_schema::HistoryEntry; 2] {
    [
        super::event(
            1,
            AdapterEvent::OffsetListed(AdminOffsetListing {
                operation_id: super::operation("timestamp-offset"),
                topic: "offsets".to_owned(),
                partition: 0,
                offset: Some(1),
                timestamp_millis,
            }),
        ),
        super::partition_offsets(2, super::operation("timestamp-offset"), "offsets", 0, 0, 2),
    ]
}

fn observed(offset: i64, timestamp_millis: i64) -> BrokerObservation {
    let record = RecordSpec {
        topic: "offsets".to_owned(),
        partition: 0,
        sequence: u64::try_from(offset + 1).unwrap_or_else(|error| panic!("sequence: {error}")),
        timestamp_millis: Some(timestamp_millis),
        key: None,
        value: Some(ByteString::utf8(format!("offset-{offset}"))),
        headers: Vec::new(),
    };
    let digest = record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));
    BrokerObservation {
        observation: u64::try_from(offset + 1)
            .unwrap_or_else(|error| panic!("observation: {error}")),
        offset,
        operation_id: super::operation(&format!("send-{offset}")),
        record,
        digest,
    }
}
