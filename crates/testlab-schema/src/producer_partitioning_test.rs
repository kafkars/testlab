//! Java-keyed producer partitioning tests pin external compatibility vectors.

use crate::{ByteString, ProducerPartitioning, RecordSpec};

#[test]
fn kafka_key_selects_partition_two_of_three() {
    let record = record(2, Some(ByteString::utf8("kafkars")));

    let partition = ProducerPartitioning::JavaKeyed { partition_count: 3 }
        .expected_partition(&record)
        .unwrap_or_else(|error| panic!("resolve keyed partition: {error}"));

    assert_eq!(partition, 2);
}

#[test]
fn apache_java_vectors_remain_compatible() {
    let vectors = [
        (ByteString::utf8(""), 9),
        (ByteString::utf8("kafka"), 4),
        (ByteString::utf8("café"), 6),
        (ByteString::utf8("😀"), 9),
        (ByteString::hex([0x00, 0xff, 0x80, 0x7f]), 3),
    ];
    for (key, expected) in vectors {
        let partition = ProducerPartitioning::JavaKeyed {
            partition_count: 12,
        }
        .expected_partition(&record(expected, Some(key)))
        .unwrap_or_else(|error| panic!("resolve Apache Java vector: {error}"));
        assert_eq!(partition, expected);
    }
}

#[test]
fn absent_key_and_incorrect_declared_partition_are_rejected() {
    let partitioning = ProducerPartitioning::JavaKeyed { partition_count: 3 };

    assert!(partitioning.expected_partition(&record(0, None)).is_err());
    assert!(
        partitioning
            .expected_partition(&record(1, Some(ByteString::utf8("kafkars"))))
            .is_err()
    );
}

#[test]
fn scenario_validation_rejects_a_wrong_automatic_observation_target() {
    let mut scenario: crate::Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-automatic-keyed-partition.toml"
    ))
    .unwrap_or_else(|error| panic!("parse automatic partition scenario: {error}"));
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate automatic partition scenario: {error}"));
    let Some(crate::ScenarioAction::Send { record, .. }) = scenario
        .steps
        .iter_mut()
        .find(|step| step.id.as_str() == "send-automatically-partitioned-record")
        .map(|step| &mut step.action)
    else {
        panic!("automatic send action missing");
    };
    record.partition = 1;

    let error = scenario
        .validate()
        .expect_err("wrong automatic partition target must fail");

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("calculated Java-keyed partition 2"))
    );
}

fn record(partition: i32, key: Option<ByteString>) -> RecordSpec {
    RecordSpec {
        topic: "records".to_owned(),
        partition,
        sequence: 1,
        timestamp_millis: None,
        key,
        value: None,
        headers: Vec::new(),
    }
}
