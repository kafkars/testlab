//! UUID-bound transaction validation joins one prior topic-ID proof to exact record topics.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BatchRecord, ClientId, DescribeTopicsAction, OperationId, ProducerId, Scenario, ScenarioAction,
    TopicSelection, TransactionDisposition,
};

pub(super) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut descriptions = BTreeMap::new();
    let mut producer_clients = BTreeMap::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateTransactionalProducer {
                client_id,
                producer_id,
                expected_error_code: None,
                ..
            } => {
                producer_clients.insert(producer_id.clone(), client_id.clone());
            }
            ScenarioAction::DescribeTopics(action) => {
                descriptions.insert(action.operation_id.clone(), action);
            }
            ScenarioAction::ExecuteTransaction {
                producer_id,
                transaction_id,
                operations,
                disposition,
                topic_identity_operation_id: Some(identity_operation_id),
                ..
            } => validate_transaction(
                producer_id,
                transaction_id,
                operations,
                *disposition,
                identity_operation_id,
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
    reason = "one UUID-bound transaction contract"
)]
fn validate_transaction(
    producer_id: &ProducerId,
    transaction_id: &OperationId,
    operations: &[BatchRecord],
    disposition: TransactionDisposition,
    identity_operation_id: &OperationId,
    producer_client: Option<&ClientId>,
    descriptions: &BTreeMap<OperationId, &DescribeTopicsAction>,
    problems: &mut Vec<String>,
) {
    if disposition != TransactionDisposition::Commit {
        problems.push(format!(
            "UUID-bound transaction {transaction_id} must commit after validation"
        ));
    }
    let Some(description) = descriptions.get(identity_operation_id) else {
        problems.push(format!(
            "UUID-bound transaction {transaction_id} references missing prior topic-ID operation {identity_operation_id}"
        ));
        return;
    };
    let description = *description;
    if description.selection != TopicSelection::TopicId {
        problems.push(format!(
            "UUID-bound transaction {transaction_id} requires a topic_id description"
        ));
    }
    if producer_client != Some(&description.client_id) {
        problems.push(format!(
            "UUID-bound transaction {transaction_id} topic-ID operation must use producer {producer_id}'s client"
        ));
    }
    let expected = ordered_topics(operations);
    let described = description
        .topics
        .iter()
        .map(|topic| topic.topic.as_str())
        .collect::<Vec<_>>();
    if described != expected {
        problems.push(format!(
            "UUID-bound transaction {transaction_id} topic-ID operation must exactly order its distinct record topics"
        ));
    }
    for topic in &description.topics {
        let partitions = operations
            .iter()
            .filter(|operation| operation.record.topic == topic.topic)
            .map(|operation| operation.record.partition)
            .collect::<BTreeSet<_>>();
        if topic.expected_error_code.is_some()
            || topic.expected_partitions.as_ref().is_none_or(|expected| {
                partitions
                    .iter()
                    .any(|partition| !expected.contains(partition))
            })
        {
            problems.push(format!(
                "UUID-bound transaction {transaction_id} requires successful topic-ID topology for {}",
                topic.topic
            ));
        }
    }
}

fn ordered_topics(operations: &[BatchRecord]) -> Vec<&str> {
    let mut seen = BTreeSet::new();
    operations
        .iter()
        .filter_map(|operation| {
            let topic = operation.record.topic.as_str();
            seen.insert(topic).then_some(topic)
        })
        .collect()
}

#[cfg(test)]
#[path = "transaction_topic_uuid_validation_test.rs"]
mod tests;
