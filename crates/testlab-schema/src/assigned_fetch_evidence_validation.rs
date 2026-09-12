//! Fetch-evidence scenarios require unique independent truth before observation.

use crate::{
    AdminOffsetPosition, OperationId, RecordSpec, Scenario, ScenarioAction, TopicSelection,
};

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let selected = scenario
        .steps
        .iter()
        .enumerate()
        .filter_map(|(index, step)| match &step.action {
            ScenarioAction::Receive {
                consumer_id,
                expected_operation_id,
                observe_fetch_evidence: true,
                ..
            } => Some((index, consumer_id, expected_operation_id)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [(receive_index, consumer_id, operation_id)] = selected.as_slice() else {
        if !selected.is_empty() {
            problems.push("Fetch evidence requires exactly one selected receive".to_owned());
        }
        return;
    };
    let Some(record) = sent_record(scenario, operation_id) else {
        problems.push(format!(
            "Fetch evidence receive references unknown operation {operation_id}"
        ));
        return;
    };
    let prior = &scenario.steps[..*receive_index];
    let assignments = prior
        .iter()
        .filter(|step| {
            matches!(&step.action,
                ScenarioAction::AssignBeginning {
                    consumer_id: assigned,
                    topic,
                    partition,
                } if assigned == *consumer_id
                    && topic == &record.topic
                    && *partition == record.partition)
        })
        .count();
    if assignments != 1 {
        problems.push(format!(
            "Fetch evidence receive requires one prior beginning assignment for {}[{}]",
            record.topic, record.partition
        ));
    }
    let identities = prior
        .iter()
        .filter(|step| {
            matches!(&step.action, ScenarioAction::DescribeTopics(action)
            if action.selection == TopicSelection::TopicId
                && action.topics.iter().any(|topic| {
                    topic.topic == record.topic
                        && topic.expected_partitions.as_ref().is_some_and(|partitions| {
                            partitions.contains(&record.partition)
                        })
                }))
        })
        .count();
    if identities != 1 {
        problems.push(format!(
            "Fetch evidence receive requires one prior topic-ID description for {}[{}]",
            record.topic, record.partition
        ));
    }
    let watermarks = prior
        .iter()
        .filter(|step| {
            matches!(&step.action, ScenarioAction::ListOffsetsBatch(action)
            if action.queries.iter().any(|query| {
                query.topic == record.topic
                    && query.partition == record.partition
                    && query.position == AdminOffsetPosition::Latest
            }))
        })
        .count();
    if watermarks != 1 {
        problems.push(format!(
            "Fetch evidence receive requires one prior latest-offset batch for {}[{}]",
            record.topic, record.partition
        ));
    }
}

fn sent_record<'a>(scenario: &'a Scenario, operation_id: &OperationId) -> Option<&'a RecordSpec> {
    scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::Send {
            operation_id: actual,
            record,
            ..
        } if actual == operation_id => Some(record),
        ScenarioAction::SendBatch { operations, .. } => operations
            .iter()
            .find(|operation| &operation.operation_id == operation_id)
            .map(|operation| &operation.record),
        _ => None,
    })
}
