//! Destructive admin scenarios require independently observable precondition actions.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Scenario, ScenarioAction};

type TopicDefinition = (i32, i16);

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    crate::admin_config_transition_validation::validate(scenario, problems);
    crate::admin_delete_records_transition_validation::validate(scenario, problems);
    crate::admin_group_offset_transition_validation::validate(scenario, problems);
    crate::admin_classic_group_transition_validation::validate(scenario, problems);
    validate_describe_errors(scenario, problems);
    let mut state = TransitionState::default();
    for step in &scenario.steps {
        state.validate_step(&step.action, problems);
    }
}

fn validate_describe_errors(scenario: &Scenario, problems: &mut Vec<String>) {
    for (index, step) in scenario.steps.iter().enumerate() {
        let ScenarioAction::DescribeTopic(action) = &step.action else {
            continue;
        };
        let Some(code) = action.expected_error_code.as_deref() else {
            continue;
        };
        if code == crate::UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE {
            continue;
        }
        let preceded_by_metadata_fault = index.checked_sub(1).is_some_and(|prior| {
            matches!(
                &scenario.steps[prior].action,
                ScenarioAction::ArmProtocolFault(control) if control.api == crate::KafkaApi::Metadata
            )
        });
        if !preceded_by_metadata_fault {
            problems.push(format!(
                "admin operation {} non-broker error {code} requires an immediately preceding metadata protocol fault",
                action.operation_id
            ));
        }
    }
}

#[derive(Default)]
struct TransitionState {
    topics_described: BTreeSet<String>,
    created_topics: BTreeMap<String, TopicDefinition>,
    singleton_topics: BTreeSet<String>,
    group_member_counts: BTreeMap<String, u32>,
    terminal_failure: Option<crate::OperationId>,
}

impl TransitionState {
    fn validate_step(&mut self, action: &ScenarioAction, problems: &mut Vec<String>) {
        validate_terminal_placement(self.terminal_failure.as_ref(), action, problems);
        let expected_failure = crate::expected_admin_error(action);
        self.validate_action(action, problems);
        if let Some((operation_id, code)) = expected_failure
            && code != crate::ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE
        {
            self.terminal_failure = Some(operation_id.clone());
        }
    }

    fn validate_action(&mut self, action: &ScenarioAction, problems: &mut Vec<String>) {
        match action {
            ScenarioAction::CreateTopic(action) => self.validate_create_topic(action, problems),
            ScenarioAction::CreateTopicsBatch(action) => validate_create_topics_batch(
                action,
                &mut self.created_topics,
                &mut self.singleton_topics,
                problems,
            ),
            ScenarioAction::CreatePartitions(action) => {
                self.validate_create_partitions(action, problems);
            }
            ScenarioAction::DescribeTopic(action) => self.validate_describe_topic(action, problems),
            ScenarioAction::DeleteTopic(action) => self.validate_delete_topic(action, problems),
            ScenarioAction::ListOffsets(action) if action.expected_error_code.is_some() => {
                self.validate_missing_partition(action, problems);
            }
            ScenarioAction::DescribeConsumerGroup(action) => self.record_group_description(action),
            ScenarioAction::DeleteConsumerGroup(action) => {
                self.validate_delete_group(action, problems);
            }
            _ => {}
        }
    }

    fn validate_create_topic(
        &mut self,
        action: &crate::CreateTopicAction,
        problems: &mut Vec<String>,
    ) {
        let definition = (action.partitions, action.replication_factor);
        match action.expected_error_code.as_deref() {
            Some(crate::TOPIC_ALREADY_EXISTS_ERROR_CODE) => {
                if self.created_topics.get(&action.topic) != Some(&definition) {
                    problems.push(format!(
                        "admin operation {} requires a prior identical successful topic creation",
                        action.operation_id
                    ));
                }
            }
            Some(crate::ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE) => {
                crate::admin_expected_error::require_untracked_topic(
                    &action.operation_id,
                    &action.topic,
                    &self.created_topics,
                    problems,
                );
            }
            Some(_) => {
                problems.push(format!(
                    "admin operation {} has an unsupported create-topic failure",
                    action.operation_id
                ));
            }
            None if !action.validate_only => {
                self.created_topics.insert(action.topic.clone(), definition);
                self.singleton_topics.insert(action.topic.clone());
            }
            None => {}
        }
    }

    fn validate_create_partitions(
        &mut self,
        action: &crate::CreatePartitionsAction,
        problems: &mut Vec<String>,
    ) {
        if action.expected_error_code.is_some() {
            crate::admin_expected_error::require_untracked_topic(
                &action.operation_id,
                &action.topic,
                &self.created_topics,
                problems,
            );
        } else if action.validate_only {
            let actual = self
                .created_topics
                .get(&action.topic)
                .map(|(partitions, _)| *partitions);
            crate::admin_validate_only_validation::validate_partition_transition(
                action, actual, problems,
            );
        } else if let Some((partitions, _)) = self.created_topics.get_mut(&action.topic) {
            *partitions = action.total_count;
        }
    }

    fn validate_describe_topic(
        &mut self,
        action: &crate::DescribeTopicAction,
        problems: &mut Vec<String>,
    ) {
        if action.expected_error_code.is_some() {
            crate::admin_expected_error::require_untracked_topic(
                &action.operation_id,
                &action.topic,
                &self.created_topics,
                problems,
            );
        } else {
            self.topics_described.insert(action.topic.clone());
        }
    }

    fn validate_delete_topic(
        &mut self,
        action: &crate::DeleteTopicAction,
        problems: &mut Vec<String>,
    ) {
        if action.expected_error_code.is_some() {
            crate::admin_expected_error::require_untracked_topic(
                &action.operation_id,
                &action.topic,
                &self.created_topics,
                problems,
            );
            return;
        }
        if !self.topics_described.contains(&action.topic) {
            problems.push(format!(
                "admin operation {} requires a prior topic description for {}",
                action.operation_id, action.topic
            ));
        }
        self.created_topics.remove(&action.topic);
        self.singleton_topics.remove(&action.topic);
    }

    fn validate_missing_partition(
        &self,
        action: &crate::ListOffsetsAction,
        problems: &mut Vec<String>,
    ) {
        if self
            .created_topics
            .get(&action.topic)
            .is_some_and(|(count, _)| *count > action.partition)
        {
            problems.push(format!(
                "admin operation {} expects missing partition {} but prior topic {} contains it",
                action.operation_id, action.partition, action.topic
            ));
        }
    }

    fn record_group_description(&mut self, action: &crate::DescribeConsumerGroupAction) {
        self.group_member_counts
            .insert(action.group_id.clone(), action.expected_member_count);
    }

    fn validate_delete_group(
        &self,
        action: &crate::DeleteConsumerGroupAction,
        problems: &mut Vec<String>,
    ) {
        if self.group_member_counts.get(&action.group_id) != Some(&0) {
            problems.push(format!(
                "admin operation {} requires a prior zero-member group description",
                action.operation_id
            ));
        }
    }
}

fn validate_create_topics_batch(
    action: &crate::CreateTopicsBatchAction,
    created_topics: &mut BTreeMap<String, TopicDefinition>,
    singleton_topics: &mut BTreeSet<String>,
    problems: &mut Vec<String>,
) {
    for item in &action.topics {
        let definition = (item.partitions, item.replication_factor);
        if item.expected_error_code.is_some() {
            let has_singleton = singleton_topics.contains(&item.topic)
                && created_topics.get(&item.topic) == Some(&definition);
            if !has_singleton {
                problems.push(format!(
                    "admin operation {} batch duplicate {} requires a prior identical successful singleton topic creation",
                    action.operation_id, item.topic
                ));
            }
        } else if created_topics.contains_key(&item.topic) {
            problems.push(format!(
                "admin operation {} expects successful creation of existing topic {}",
                action.operation_id, item.topic
            ));
        } else {
            created_topics.insert(item.topic.clone(), definition);
            singleton_topics.remove(&item.topic);
        }
    }
}

fn validate_terminal_placement(
    terminal_failure: Option<&crate::OperationId>,
    action: &ScenarioAction,
    problems: &mut Vec<String>,
) {
    if let Some(operation_id) = terminal_failure
        && !matches!(action, ScenarioAction::ShutdownClient { .. })
    {
        problems.push(format!(
            "admin operation {operation_id} expects a terminal command failure and must be followed only by client shutdown"
        ));
    }
}
