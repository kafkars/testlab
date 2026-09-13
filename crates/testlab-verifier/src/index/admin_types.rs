//! Indexed admin values retain public history positions and independent observation ordinals.

use std::collections::BTreeMap;

pub(crate) fn push<K: Ord>(map: &mut BTreeMap<K, Vec<u64>>, key: K, sequence: u64) {
    map.entry(key).or_default().push(sequence);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedAdminTopicCompletion {
    pub(crate) history_sequence: u64,
    pub(crate) topic: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedAdminTopicsCreationBatch {
    pub(crate) history_sequence: u64,
    pub(crate) outcomes: Vec<testlab_schema::AdminTopicCreationOutcome>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedAdminTopicsDescription {
    pub(crate) history_sequence: u64,
    pub(crate) value: testlab_schema::AdminTopicsDescription,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedAdminTopicsDeletion {
    pub(crate) history_sequence: u64,
    pub(crate) value: testlab_schema::AdminTopicsDeletion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedAdminGroupCompletion {
    pub(crate) history_sequence: u64,
    pub(crate) group_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedAdminGroupOffsetCompletion {
    pub(crate) history_sequence: u64,
    pub(crate) group_id: String,
    pub(crate) topic: String,
    pub(crate) partition: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTopicDescription {
    pub(crate) history_sequence: u64,
    pub(crate) topic: String,
    pub(crate) partitions: Vec<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTopicsList {
    pub(crate) history_sequence: u64,
    pub(crate) outcomes: Vec<testlab_schema::AdminListedTopicOutcome>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedOffsetList {
    pub(crate) history_sequence: u64,
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) offset: Option<i64>,
    pub(crate) timestamp_millis: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedRecordsDeleted {
    pub(crate) history_sequence: u64,
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) low_watermark: Option<i64>,
    pub(crate) error_code: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTopicConfigDescription {
    pub(crate) history_sequence: u64,
    pub(crate) topic: String,
    pub(crate) config_name: String,
    pub(crate) value: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedAdminTopicConfigsDescription {
    pub(crate) history_sequence: u64,
    pub(crate) value: testlab_schema::AdminTopicConfigsDescription,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedConfigBatch {
    pub(crate) history_sequence: u64,
    pub(crate) value: testlab_schema::AdminTopicConfigsAlteration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedAdminTopicConfigCompletion {
    pub(crate) history_sequence: u64,
    pub(crate) topic: String,
    pub(crate) config_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedClusterDescription {
    pub(crate) history_sequence: u64,
    pub(crate) cluster_id: Option<String>,
    pub(crate) broker_ids: Vec<i32>,
    pub(crate) fenced_broker_ids: Vec<i32>,
    pub(crate) authorized_operations: Option<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedConsumerGroupsList {
    pub(crate) history_sequence: u64,
    pub(crate) group_ids: Vec<String>,
    pub(crate) broker_errors: Vec<testlab_schema::AdminBrokerError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedConsumerGroupDescription {
    pub(crate) history_sequence: u64,
    pub(crate) group_id: String,
    pub(crate) member_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedConsumerGroupOffset {
    pub(crate) history_sequence: u64,
    pub(crate) group_id: String,
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) offset: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTopicObservation {
    pub(crate) history_sequence: u64,
    pub(crate) observation: u64,
    pub(crate) topic: String,
    pub(crate) exists: bool,
    pub(crate) partitions: Vec<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTopicIdentityObservation {
    pub(crate) history_sequence: u64,
    pub(crate) observation: u64,
    pub(crate) topic: String,
    pub(crate) topic_id: [u8; 16],
    pub(crate) partitions: Vec<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedClusterObservation {
    pub(crate) history_sequence: u64,
    pub(crate) observation: u64,
    pub(crate) cluster_id: Option<String>,
    pub(crate) broker_ids: Vec<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedConsumerGroupObservation {
    pub(crate) history_sequence: u64,
    pub(crate) observation: u64,
    pub(crate) group_id: String,
    pub(crate) exists: bool,
    pub(crate) member_count: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedConsumerGroupOffsetObservation {
    pub(crate) history_sequence: u64,
    pub(crate) observation: u64,
    pub(crate) group_id: String,
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) offset: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTopicConfigObservation {
    pub(crate) history_sequence: u64,
    pub(crate) observation: u64,
    pub(crate) topic: String,
    pub(crate) config_name: String,
    pub(crate) value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedPartitionOffsetsObservation {
    pub(crate) history_sequence: u64,
    pub(crate) observation: u64,
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) low_watermark: i64,
    pub(crate) high_watermark: i64,
}
