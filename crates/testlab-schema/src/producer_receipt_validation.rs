//! Receipt validation binds each selected ordinary send to one prior exact topic-ID proof.

use std::collections::BTreeMap;

use crate::{
    ClientId, DescribeTopicsAction, OperationId, ProducerId, Scenario, ScenarioAction,
    TopicSelection,
};

pub(super) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut descriptions = BTreeMap::new();
    let mut producer_clients = BTreeMap::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateProducer {
                client_id,
                producer_id,
                ..
            } => {
                producer_clients.insert(producer_id.clone(), client_id.clone());
            }
            ScenarioAction::DescribeTopics(action) => {
                descriptions.insert(action.operation_id.clone(), action);
            }
            ScenarioAction::Send {
                producer_id,
                operation_id,
                topic_identity_operation_id: Some(identity_operation_id),
                record,
                ..
            } => validate_send(
                producer_id,
                operation_id,
                identity_operation_id,
                &record.topic,
                record.partition,
                producer_clients.get(producer_id),
                &descriptions,
                problems,
            ),
            _ => {}
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one UUID-bound producer receipt contract"
)]
fn validate_send(
    producer_id: &ProducerId,
    operation_id: &OperationId,
    identity_operation_id: &OperationId,
    topic: &str,
    partition: i32,
    producer_client: Option<&ClientId>,
    descriptions: &BTreeMap<OperationId, &DescribeTopicsAction>,
    problems: &mut Vec<String>,
) {
    let Some(description) = descriptions.get(identity_operation_id).copied() else {
        problems.push(format!(
            "UUID-bound send {operation_id} references missing prior topic-ID operation {identity_operation_id}"
        ));
        return;
    };
    if description.selection != TopicSelection::TopicId {
        problems.push(format!(
            "UUID-bound send {operation_id} requires a topic_id description"
        ));
    }
    if producer_client != Some(&description.client_id) {
        problems.push(format!(
            "UUID-bound send {operation_id} topic-ID operation must use producer {producer_id}'s client"
        ));
    }
    let exact = description.topics.as_slice();
    if !matches!(exact, [expected] if expected.topic == topic
        && expected.expected_error_code.is_none()
        && expected.expected_partitions.as_ref().is_some_and(|values| values.contains(&partition)))
    {
        problems.push(format!(
            "UUID-bound send {operation_id} requires successful exact topic-ID topology for {topic} partition {partition}"
        ));
    }
}

#[cfg(test)]
mod tests {
    use crate::{Capability, Scenario, ScenarioAction};

    fn scenario() -> Scenario {
        toml::from_str(include_str!(
            "../../../scenarios/kafka/producer-receipt-metadata.toml"
        ))
        .unwrap_or_else(|error| panic!("parse producer receipt scenario: {error}"))
    }

    #[test]
    fn checked_in_receipt_scenario_is_valid_and_capability_gated() {
        let mut scenario = scenario();
        scenario
            .validate()
            .unwrap_or_else(|error| panic!("validate producer receipt scenario: {error}"));
        assert!(
            scenario
                .requires
                .remove(&Capability::ProducerReceiptMetadata)
        );
        assert!(
            scenario
                .validation_error("receipt capability must be explicit")
                .to_string()
                .contains("producer_receipt_metadata")
        );
    }

    #[test]
    fn receipt_send_requires_prior_exact_successful_topology() {
        let mut scenario = scenario();
        let description = scenario
            .steps
            .iter_mut()
            .find_map(|step| match &mut step.action {
                ScenarioAction::DescribeTopics(action) => Some(action),
                _ => None,
            })
            .unwrap_or_else(|| panic!("topic-ID description missing"));
        description.topics[0].expected_partitions = Some(vec![1]);
        let error = scenario
            .validation_error("mismatched topology must fail")
            .to_string();
        assert!(error.contains("requires successful exact topic-ID topology"));
    }
}
