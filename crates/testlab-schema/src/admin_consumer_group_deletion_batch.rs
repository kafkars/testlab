//! Consumer-group batch deletion keeps caller order and public outcomes explicit.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId, Scenario, ScenarioAction};

/// Scenario intent for deleting multiple empty consumer groups through one public call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteConsumerGroupsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered distinct consumer-group identities.
    pub group_ids: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for deleting multiple consumer groups through one public call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteConsumerGroupsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered distinct consumer-group identities.
    pub group_ids: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One caller-positioned public consumer-group deletion outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupDeletionOutcome {
    /// Exact consumer-group identity returned for this request position.
    pub group_id: String,
    /// Stable normalized public error, or none on success.
    pub error_code: Option<String>,
}

/// Public result for one caller-ordered consumer-group batch deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupsDeletion {
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered public outcomes.
    pub outcomes: Vec<AdminConsumerGroupDeletionOutcome>,
}

pub(crate) fn validate_transition(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut empty_descriptions = BTreeSet::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DescribeClassicGroups(action)
                if action
                    .groups
                    .iter()
                    .all(|group| group.expected_member_count == 0) =>
            {
                empty_descriptions.insert(
                    action
                        .groups
                        .iter()
                        .map(|group| group.group_id.clone())
                        .collect::<Vec<_>>(),
                );
            }
            ScenarioAction::DeleteConsumerGroups(action) => {
                if !empty_descriptions.remove(&action.group_ids) {
                    problems.push(format!(
                        "admin operation {} requires a prior caller-ordered zero-member classic-group description",
                        action.operation_id
                    ));
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
#[path = "admin_consumer_group_deletion_batch_test.rs"]
mod tests;
