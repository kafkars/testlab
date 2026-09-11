//! Mixed-protocol consumer-group descriptions preserve caller order and public facts.

#[path = "admin_consumer_group_description_transition_validation.rs"]
pub(crate) mod transition_validation;

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{ClientId, GroupProtocol, OperationId};

const MAX_GROUPS: usize = 32;

/// One scenario-side expectation for an active classic or KIP-848 group.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerGroupDescriptionExpectation {
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Membership protocol the public description must identify.
    pub protocol: GroupProtocol,
    /// Exact public group state.
    pub expected_state: String,
    /// Exact public and independently observed member count.
    pub expected_member_count: u32,
    /// Exact broker-selected assignor name.
    pub expected_assignor_name: String,
    /// Topic required in the modeled member and typed KIP-848 description.
    pub expected_topic: String,
    /// Partition required in the modeled member and typed KIP-848 assignment.
    pub expected_partition: i32,
}

/// Scenario intent for one caller-ordered mixed consumer-group description batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeConsumerGroupsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered active group expectations.
    pub groups: Vec<ConsumerGroupDescriptionExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one caller-ordered mixed consumer-group description batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeConsumerGroupsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered exact Kafka consumer-group identities.
    pub group_ids: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Selected public facts for one successful classic or KIP-848 description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupDescriptionValue {
    /// Kafka's exact group state string.
    pub state: String,
    /// Protocol-specific public description variant.
    pub protocol: GroupProtocol,
    /// Public member count.
    pub member_count: u32,
    /// Classic protocol type, absent for KIP-848 descriptions.
    pub protocol_type: Option<String>,
    /// KIP-848 group epoch, absent for classic descriptions.
    pub group_epoch: Option<i32>,
    /// KIP-848 target-assignment epoch, absent for classic descriptions.
    pub assignment_epoch: Option<i32>,
    /// Classic selected protocol data or KIP-848 server assignor name.
    pub assignor_name: String,
    /// Public members ordered by member ID bytes.
    pub members: Vec<AdminConsumerGroupMemberDescription>,
}

/// One typed KIP-848 topic assignment retained from the public description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupTopicAssignment {
    /// Kafka topic UUID bytes.
    pub topic_id: [u8; 16],
    /// Exact Kafka topic name.
    pub topic_name: String,
    /// Canonical nonnegative partition indexes.
    pub partitions: Vec<i32>,
}

/// One public classic or KIP-848 group member description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupMemberDescription {
    /// Stable broker-issued member ID.
    pub member_id: String,
    /// Optional static member identity.
    pub group_instance_id: Option<String>,
    /// Member client ID.
    pub client_id: String,
    /// Member client host.
    pub client_host: String,
    /// Optional KIP-848 rack identity.
    pub rack_id: Option<String>,
    /// KIP-848 member epoch, absent for classic members.
    pub member_epoch: Option<i32>,
    /// Canonical KIP-848 explicit subscriptions; empty for classic members.
    pub subscribed_topic_names: Vec<String>,
    /// Optional KIP-848 subscription regular expression.
    pub subscribed_topic_regex: Option<String>,
    /// Typed KIP-848 current assignment; empty for classic members.
    pub assignment: Vec<AdminConsumerGroupTopicAssignment>,
    /// Typed KIP-848 target assignment; empty for classic members.
    pub target_assignment: Vec<AdminConsumerGroupTopicAssignment>,
    /// Exact KIP-848 v1 member type, when represented.
    pub member_type: Option<i8>,
    /// Exact classic protocol metadata; empty for KIP-848 members.
    pub classic_metadata: Vec<u8>,
    /// Exact classic protocol assignment; empty for KIP-848 members.
    pub classic_assignment: Vec<u8>,
}

/// One public consumer-group description outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupDescriptionOutcome {
    /// Exact caller-selected consumer-group identity.
    pub group_id: String,
    /// Selected public facts, absent when this group failed.
    pub description: Option<AdminConsumerGroupDescriptionValue>,
    /// Stable normalized per-group error code.
    pub error_code: Option<String>,
}

/// Public completion for one caller-ordered mixed consumer-group description batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupsDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// One exact public outcome per requested group in caller order.
    pub outcomes: Vec<AdminConsumerGroupDescriptionOutcome>,
}

pub(crate) fn validate_action(action: &DescribeConsumerGroupsAction, problems: &mut Vec<String>) {
    if !(2..=MAX_GROUPS).contains(&action.groups.len()) {
        problems.push(format!(
            "admin operation {} groups must contain 2 to {MAX_GROUPS} entries",
            action.operation_id
        ));
    }
    let mut group_ids = BTreeSet::new();
    for group in &action.groups {
        if group.group_id.is_empty()
            || group.group_id.len() > 255
            || !group_ids.insert(group.group_id.clone())
        {
            problems.push(format!(
                "admin operation {} groups must contain unique valid group ids",
                action.operation_id
            ));
        }
        validate_expectation(&action.operation_id, group, problems);
    }
}

fn validate_expectation(
    operation_id: &OperationId,
    group: &ConsumerGroupDescriptionExpectation,
    problems: &mut Vec<String>,
) {
    if group.expected_state.is_empty() || group.expected_state.len() > 64 {
        problems.push(format!(
            "admin operation {operation_id} group {} has invalid expected_state",
            group.group_id
        ));
    }
    if group.expected_member_count == 0 || group.expected_member_count > 32_768 {
        problems.push(format!(
            "admin operation {operation_id} group {} expected_member_count must be between 1 and 32768",
            group.group_id
        ));
    }
    if group.expected_assignor_name.is_empty() || group.expected_assignor_name.len() > 255 {
        problems.push(format!(
            "admin operation {operation_id} group {} has invalid expected_assignor_name",
            group.group_id
        ));
    }
    if group.expected_topic.is_empty() || group.expected_topic.len() > 249 {
        problems.push(format!(
            "admin operation {operation_id} group {} has invalid expected_topic",
            group.group_id
        ));
    }
    if group.expected_partition < 0 {
        problems.push(format!(
            "admin operation {operation_id} group {} expected_partition must be nonnegative",
            group.group_id
        ));
    }
}
