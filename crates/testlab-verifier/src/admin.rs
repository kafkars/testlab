//! Admin verification requires one exact public completion per declared operation.

use testlab_schema::{BrokerObservation, Scenario, ScenarioAction, Violation};

use crate::admin_discovery::verify_discovery_action;
use crate::index::{HistoryIndex, IndexedAdminTopicCompletion};
use crate::support::{references, violation};

pub(crate) fn verify_admin(
    scenario: &Scenario,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        if !index.action_issued(&step.action) {
            continue;
        }
        if verify_discovery_action(&step.action, index, observations, violations) {
            continue;
        }
        match &step.action {
            ScenarioAction::CreateTopic {
                operation_id,
                topic,
                ..
            } => verify_completion(
                "ADMIN-001",
                "topic creation",
                operation_id,
                topic,
                index.topics_created.get(operation_id).map(Vec::as_slice),
                violations,
            ),
            ScenarioAction::CreatePartitions {
                operation_id,
                topic,
                ..
            } => verify_completion(
                "ADMIN-002",
                "partition creation",
                operation_id,
                topic,
                index
                    .topic_partitions_created
                    .get(operation_id)
                    .map(Vec::as_slice),
                violations,
            ),
            _ => {}
        }
    }
}

fn verify_completion(
    contract: &str,
    operation: &str,
    operation_id: &testlab_schema::OperationId,
    topic: &str,
    completions: Option<&[IndexedAdminTopicCompletion]>,
    violations: &mut Vec<Violation>,
) {
    let exact = completions.is_some_and(|values| {
        values.len() == 1 && values.first().is_some_and(|value| value.topic == topic)
    });
    if exact {
        return;
    }
    violations.push(violation(
        contract,
        format!(
            "admin operation {operation_id} expected one {operation} for topic {topic}, observed {} completion(s)",
            completions.map_or(0, <[IndexedAdminTopicCompletion]>::len)
        ),
        Some(operation_id.clone()),
        references(completions.map(|values| {
            values
                .iter()
                .map(|value| value.history_sequence)
                .collect::<Vec<_>>()
        }).as_deref()),
    ));
}
