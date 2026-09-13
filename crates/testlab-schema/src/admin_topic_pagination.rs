//! Explicit topic-partition pagination intent and observed page facts.

use serde::{Deserialize, Serialize};

use crate::AdminTopicPartitionDescriptionOutcome;

/// Caller-selected controls for explicit `DescribeTopicPartitions` pages.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopicDescriptionPagination {
    /// Positive maximum partition count returned by one page.
    pub response_partition_limit: u32,
    /// Whether each returned cursor starts one separately submitted page.
    pub follow_cursors: bool,
}

/// Public continuation cursor returned by one topic-partition page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicPageCursor {
    /// Exact topic name returned by the public cursor.
    pub topic_name: String,
    /// First partition eligible for the next explicit page.
    pub partition_index: i32,
}

/// Public facts retained from one separately submitted topic-partition page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicDescriptionPage {
    /// Sorted partition identifiers carried by this page.
    pub partitions: Vec<i32>,
    /// Complete public partition facts carried by this page.
    pub partition_details: Vec<AdminTopicPartitionDescriptionOutcome>,
    /// Public continuation cursor, when Kafka reports another page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<AdminTopicPageCursor>,
}
