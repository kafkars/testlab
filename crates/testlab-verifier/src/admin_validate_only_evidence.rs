//! Validate-only violations retain public, mutation, and independent evidence separately.

use testlab_schema::{AlterTopicConfigAction, OperationId, Violation};

use crate::index::{
    IndexedAdminTopicCompletion, IndexedAdminTopicConfigCompletion, IndexedTopicConfigObservation,
    IndexedTopicObservation,
};
use crate::support::violation;

pub(crate) fn topic_violation(
    contract: &str,
    operation_id: &OperationId,
    operation: &str,
    public: Option<&Vec<IndexedAdminTopicCompletion>>,
    mutation: Option<&Vec<IndexedAdminTopicCompletion>>,
    independent: Option<&Vec<IndexedTopicObservation>>,
    baseline_observation: Option<u64>,
) -> Violation {
    violation(
        contract,
        format!(
            "admin operation {operation_id} expected one exact {operation}, no mutation completion, and immediate independent unchanged state"
        ),
        Some(operation_id.clone()),
        public
            .into_iter()
            .flatten()
            .chain(mutation.into_iter().flatten())
            .map(|value| format!("history:{}", value.history_sequence))
            .chain(
                independent
                    .into_iter()
                    .flatten()
                    .map(|value| format!("broker-state-observation:{}", value.observation)),
            )
            .chain(
                baseline_observation
                    .into_iter()
                    .map(|value| format!("broker-state-observation:{value}")),
            )
            .collect(),
    )
}

pub(crate) fn config_violation(
    action: &AlterTopicConfigAction,
    public: Option<&Vec<IndexedAdminTopicConfigCompletion>>,
    mutation: Option<&Vec<IndexedAdminTopicConfigCompletion>>,
    independent: Option<&Vec<IndexedTopicConfigObservation>>,
    baseline: Option<&IndexedTopicConfigObservation>,
) -> Violation {
    violation(
        "ADMIN-022",
        format!(
            "admin operation {} expected one exact configuration validation, no mutation completion, and immediate independent unchanged state",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        public
            .into_iter()
            .flatten()
            .chain(mutation.into_iter().flatten())
            .map(|value| format!("history:{}", value.history_sequence))
            .chain(
                independent
                    .into_iter()
                    .flatten()
                    .map(|value| format!("broker-state-observation:{}", value.observation)),
            )
            .chain(
                baseline
                    .into_iter()
                    .map(|value| format!("broker-state-observation:{}", value.observation)),
            )
            .collect(),
    )
}
