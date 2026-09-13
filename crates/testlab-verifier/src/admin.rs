use crate::admin_acl::verify_acl_action;
use crate::admin_batch::verify_batch_action;
use crate::admin_broker_unregistration::verify as verify_broker_unregistration;
use crate::admin_client_quota::verify_client_quota_action;
use crate::admin_cluster::verify_cluster_action;
use crate::admin_config::verify_config_action;
use crate::admin_config_batch::verify_config_batch_action;
use crate::admin_config_resources::verify_config_resources_action;
use crate::admin_delegation_token::verify as verify_delegation_token;
use crate::admin_discovery::verify_discovery_action;
use crate::admin_failure::verify_expected_failure;
use crate::admin_features::verify_features_action;
use crate::admin_group::verify_group_action;
use crate::admin_group_batch::verify_group_batch_action;
use crate::admin_leader_election::verify_leader_election_action;
use crate::admin_log_dirs::verify_log_dirs_action;
use crate::admin_metadata_quorum::verify_metadata_quorum_action;
use crate::admin_offset_batch::verify_offset_batch_action;
use crate::admin_operation::operation_id;
use crate::admin_partition_reassignments::verify_partition_reassignments_action;
use crate::admin_producers::verify_producers_action;
use crate::admin_records::verify_records_action;
use crate::admin_records_batch::verify_records_batch_action;
use crate::admin_replica_log_dirs::verify_replica_log_dirs_action;
use crate::admin_share_group::verify_share_group_action;
use crate::admin_streams_group::verify as verify_streams_group;
use crate::admin_topic::verify_topic_action;
use crate::admin_topics_deletion::verify_topics_deletion_action;
use crate::admin_topics_description::verify_topics_description_action;
use crate::admin_transactions::verify_transactions_action;
use crate::admin_user_scram::verify_user_scram_action;
use crate::admin_validate_only::verify_validate_only_action;
use crate::index::HistoryIndex;
use crate::support::violation;
use testlab_schema::{BrokerObservation, Scenario, ScenarioAction, Violation};
pub(crate) fn verify_admin(
    scenario: &Scenario,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let mut prior_admin_command = None;
    let expected_failure_step = scenario.steps.iter().position(|step| {
        testlab_schema::expected_admin_error(&step.action).is_some()
            && !index.admin_command_failures(&step.action).is_empty()
    });
    for (step_index, step) in scenario.steps.iter().enumerate() {
        let Some(contract) = contract(&step.action) else {
            continue;
        };
        let operation_id = operation_id(&step.action).cloned();
        let (exact, count) = index.admin_command_state(&step.action);
        if count == 0 {
            if index.command_failures.is_empty()
                || expected_failure_step.is_some_and(|failure| step_index < failure)
            {
                violations.push(violation(
                    contract,
                    "admin action expected one exact wire command, observed none".to_owned(),
                    operation_id.clone(),
                    scenario_evidence(operation_id.as_ref()),
                ));
            }
            continue;
        }
        if !exact {
            violations.push(violation(
                contract,
                format!("admin action expected one exact wire command, observed {count} same-operation command(s)"),
                operation_id.clone(),
                scenario_evidence(operation_id.as_ref()),
            ));
            continue;
        }
        let Some(command_sequence) = index.admin_command_sequence(&step.action) else {
            continue;
        };
        if prior_admin_command.is_some_and(|prior| command_sequence <= prior) {
            violations.push(violation(
                contract,
                format!(
                    "admin command at history sequence {command_sequence} did not follow the prior scenario admin command"
                ),
                operation_id,
                vec![format!("history:{command_sequence}")],
            ));
            continue;
        }
        prior_admin_command = Some(command_sequence);
        if crate::adversary::verify_admin_failure(scenario, &step.action, index, violations)
            || verify_expected_failure(&step.action, index, violations)
            || verify_acl_action(&step.action, index, violations)
            || verify_client_quota_action(&step.action, index, violations)
            || verify_user_scram_action(&step.action, index, violations)
            || verify_delegation_token(&step.action, index, violations)
            || verify_streams_group(&step.action, index, violations)
            || verify_share_group_action(scenario, &step.action, index, violations)
            || verify_batch_action(&step.action, index, violations)
            || verify_offset_batch_action(&step.action, index, violations)
            || verify_leader_election_action(&step.action, index, violations)
            || verify_partition_reassignments_action(&step.action, index, violations)
            || verify_validate_only_action(&step.action, index, violations)
            || verify_group_batch_action(scenario, &step.action, index, violations)
            || verify_config_resources_action(&step.action, index, violations)
            || verify_config_batch_action(scenario, &step.action, index, violations)
            || verify_config_action(&step.action, index, violations)
            || verify_topic_action(&step.action, index, violations)
            || verify_topics_deletion_action(scenario, &step.action, index, violations)
            || verify_topics_description_action(&step.action, index, violations)
            || verify_broker_unregistration(&step.action, index, violations)
            || verify_cluster_action(&step.action, index, violations)
            || verify_features_action(&step.action, index, violations)
            || verify_producers_action(&step.action, index, violations)
            || verify_log_dirs_action(&step.action, index, violations)
            || verify_replica_log_dirs_action(&step.action, index, violations)
            || verify_metadata_quorum_action(&step.action, index, violations)
            || verify_transactions_action(&step.action, index, violations)
            || verify_group_action(&step.action, index, violations)
            || verify_records_action(&step.action, index, violations)
            || verify_records_batch_action(&step.action, index, violations)
        {
            continue;
        }
        let _ = verify_discovery_action(&step.action, index, observations, violations);
    }
}
pub(crate) type AdminCommandWindow = (u64, Option<u64>);
pub(crate) fn public_after_command(window: Option<AdminCommandWindow>, public: u64) -> bool {
    window.is_some_and(|(command, _)| command < public)
}
pub(crate) fn immediate_after_public(
    window: Option<AdminCommandWindow>,
    public: u64,
    observation: u64,
) -> bool {
    window
        .is_some_and(|(_, next)| public < observation && next.is_none_or(|next| observation < next))
}
fn scenario_evidence(operation_id: Option<&testlab_schema::OperationId>) -> Vec<String> {
    operation_id.map_or_else(Vec::new, |value| {
        vec![format!("scenario:operation:{value}")]
    })
}
fn contract(action: &ScenarioAction) -> Option<&'static str> {
    Some(match action {
        ScenarioAction::CreateTopic(value) if value.expected_error_code.is_some() => "ADMIN-014",
        ScenarioAction::CreatePartitions(value) if value.expected_error_code.is_some() => {
            "ADMIN-019"
        }
        ScenarioAction::DeleteTopic(value) if value.expected_error_code.is_some() => "ADMIN-019",
        ScenarioAction::DescribeTopic(value) if value.expected_error_code.is_some() => "ADMIN-019",
        ScenarioAction::ListOffsets(value) if value.expected_error_code.is_some() => "ADMIN-019",
        ScenarioAction::CreateTopic(value) if value.validate_only => "ADMIN-020",
        ScenarioAction::CreateTopic(value) if value.replica_assignments.is_some() => "ADMIN-084",
        ScenarioAction::CreatePartitions(value) if value.validate_only => "ADMIN-021",
        ScenarioAction::CreatePartitions(value) if value.replica_assignments.is_some() => {
            "ADMIN-085"
        }
        ScenarioAction::AlterTopicConfig(value) if value.validate_only => "ADMIN-022",
        ScenarioAction::ListConsumerGroupOffsetsBatch(_) => "ADMIN-023",
        ScenarioAction::ListConsumerGroupsOffsets(_) => "ADMIN-024",
        ScenarioAction::AlterConsumerGroupOffsets(_) => "ADMIN-025",
        ScenarioAction::DeleteConsumerGroupOffsets(_) => "ADMIN-026",
        ScenarioAction::DescribeClassicGroups(_) => "ADMIN-027",
        ScenarioAction::DescribeConsumerGroups(_) => "ADMIN-069",
        ScenarioAction::ListOffsetsBatch(_) => "ADMIN-028",
        ScenarioAction::ListConsumerGroups(value)
            if crate::admin_group::filters::selected(value) =>
        {
            "ADMIN-082"
        }
        ScenarioAction::ListConsumerGroups(value)
            if value.api == testlab_schema::GroupListingApi::AllGroups =>
        {
            "ADMIN-029"
        }
        ScenarioAction::CreateAcls(_) => "ADMIN-030",
        ScenarioAction::DescribeAcls(_) => "ADMIN-031",
        ScenarioAction::DeleteAcls(_) => "ADMIN-032",
        ScenarioAction::DescribeClientQuota(_) => "ADMIN-033",
        ScenarioAction::AlterClientQuota(value) if value.validate_only => "ADMIN-086",
        ScenarioAction::AlterClientQuota(_) => "ADMIN-034",
        ScenarioAction::DescribeUserScramCredential(_) => "ADMIN-035",
        ScenarioAction::AlterUserScramCredential(_) => "ADMIN-036",
        ScenarioAction::DescribeShareGroup(_) => "ADMIN-037",
        ScenarioAction::DescribeShareGroups(_) => "ADMIN-042",
        ScenarioAction::ListShareGroupsOffsets(_) => "ADMIN-043",
        ScenarioAction::ListShareGroupOffsets(_) => "ADMIN-038",
        ScenarioAction::AlterShareGroupOffsets(_) => "ADMIN-039",
        ScenarioAction::DeleteShareGroupOffsets(_) => "ADMIN-040",
        ScenarioAction::DeleteShareGroups(_) => "ADMIN-041",
        ScenarioAction::CreateTopic(_) => "ADMIN-001",
        ScenarioAction::CreateTopicsBatch(_) => "ADMIN-018",
        ScenarioAction::CreatePartitions(_) => "ADMIN-002",
        ScenarioAction::DescribeTopic(_) => "ADMIN-003",
        ScenarioAction::DescribeTopics(value)
            if value.selection == testlab_schema::TopicSelection::TopicId =>
        {
            "ADMIN-061"
        }
        ScenarioAction::DeleteTopics(value)
            if value.selection == testlab_schema::TopicSelection::TopicId =>
        {
            "ADMIN-062"
        }
        ScenarioAction::DescribeTopics(_) => "ADMIN-044",
        ScenarioAction::DeleteTopics(_) => "ADMIN-045",
        ScenarioAction::DeleteConsumerGroups(_) => "ADMIN-046",
        ScenarioAction::RemoveConsumerGroupMembers(_) => "ADMIN-068",
        ScenarioAction::ListTopics(_) => "ADMIN-004",
        ScenarioAction::ListConfigResources(value) => match value.api {
            testlab_schema::ConfigResourceListingApi::Resource => "ADMIN-063",
            testlab_schema::ConfigResourceListingApi::ClientMetrics => "ADMIN-071",
        },
        ScenarioAction::ListOffsets(value)
            if value.read_isolation == testlab_schema::AdminReadIsolation::ReadUncommitted =>
        {
            "ADMIN-088"
        }
        ScenarioAction::ListOffsets(value) => match value.position {
            testlab_schema::AdminOffsetSelector::Timestamp => "ADMIN-077",
            testlab_schema::AdminOffsetSelector::MaxTimestamp => "ADMIN-078",
            _ => "ADMIN-005",
        },
        ScenarioAction::DeleteRecords(_) => "ADMIN-017",
        ScenarioAction::DeleteRecordsBatch(_) => "ADMIN-047",
        ScenarioAction::DescribeTopicConfig(_) => "ADMIN-015",
        ScenarioAction::DescribeTopicConfigs(value)
            if value.include_synonyms || value.include_documentation =>
        {
            "ADMIN-081"
        }
        ScenarioAction::DescribeTopicConfigs(value) => match value.api {
            testlab_schema::TopicConfigApi::Topic => "ADMIN-048",
            testlab_schema::TopicConfigApi::Resource => "ADMIN-064",
        },
        ScenarioAction::AlterTopicConfigs(value)
            if value.topics.iter().any(|topic| {
                topic.method == testlab_schema::TopicConfigMutationMethod::RestoreDefault
            }) =>
        {
            "ADMIN-079"
        }
        ScenarioAction::AlterTopicConfigs(value)
            if value
                .topics
                .iter()
                .any(|topic| topic.method != testlab_schema::TopicConfigMutationMethod::Set) =>
        {
            "ADMIN-080"
        }
        ScenarioAction::AlterTopicConfigs(value) => match value.api {
            testlab_schema::TopicConfigMutationApi::Topic => "ADMIN-049",
            testlab_schema::TopicConfigMutationApi::Resource => "ADMIN-065",
            testlab_schema::TopicConfigMutationApi::LegacyTopic => "ADMIN-066",
            testlab_schema::TopicConfigMutationApi::LegacyResource => "ADMIN-067",
        },
        ScenarioAction::DescribeFeatures(_) => "ADMIN-050",
        ScenarioAction::ValidateFeatureUpdates(_) => "ADMIN-072",
        ScenarioAction::ExerciseDelegationTokenLifecycle(value)
            if value.expire_after_ms.is_some() =>
        {
            "ADMIN-087"
        }
        ScenarioAction::ExerciseDelegationTokenLifecycle(_) => "ADMIN-073",
        ScenarioAction::ExerciseStreamsGroupAdminLifecycle(_) => "ADMIN-074",
        ScenarioAction::DescribeProducers(_) => "ADMIN-051",
        ScenarioAction::ListTransactions(value)
            if crate::admin_transactions::filters::selected(value) =>
        {
            "ADMIN-083"
        }
        ScenarioAction::ListTransactions(_) => "ADMIN-052",
        ScenarioAction::DescribeTransactions(_) => "ADMIN-053",
        ScenarioAction::FenceProducers(_) => "ADMIN-057",
        ScenarioAction::AlterPartitionReassignments(_) => "ADMIN-058",
        ScenarioAction::ListPartitionReassignments(_) => "ADMIN-059",
        ScenarioAction::ElectLeaders(_) => "ADMIN-060",
        ScenarioAction::DescribeLogDirs(_) => "ADMIN-054",
        ScenarioAction::DescribeReplicaLogDirs(_) => "ADMIN-055",
        ScenarioAction::AlterReplicaLogDirs(_) => "ADMIN-070",
        ScenarioAction::DescribeMetadataQuorum(_) => "ADMIN-056",
        ScenarioAction::AlterTopicConfig(_) => "ADMIN-016",
        ScenarioAction::ListConsumerGroupOffsets(_) => "ADMIN-006",
        ScenarioAction::DeleteTopic(_) => "ADMIN-007",
        ScenarioAction::DescribeCluster(_) => "ADMIN-008",
        ScenarioAction::UnregisterBroker(_) => "ADMIN-076",
        ScenarioAction::ListConsumerGroups(_) => "ADMIN-009",
        ScenarioAction::DescribeConsumerGroup(_) => "ADMIN-010",
        ScenarioAction::AlterConsumerGroupOffset(_) => "ADMIN-011",
        ScenarioAction::DeleteConsumerGroupOffset(_) => "ADMIN-012",
        ScenarioAction::DeleteConsumerGroup(_) => "ADMIN-013",
        _ => return None,
    })
}
