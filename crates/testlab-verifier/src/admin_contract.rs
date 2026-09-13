//! Administrative contract routing maps every action to its stable requirement.

use testlab_schema::ScenarioAction;

use crate::admin_client_quota::contract as quota_contract;
use crate::admin_group::describe_group_contract as group_contract;
use crate::admin_group_batch_mutation::alter_contract;
use crate::admin_producers::contract as producer_contract;
use crate::admin_streams_group::contract as streams_contract;
use crate::admin_topic::contract as topic_contract;

#[allow(
    clippy::too_many_lines,
    reason = "exhaustive admin contract routing keeps every public action explicit"
)]
pub(super) fn contract(action: &ScenarioAction) -> Option<&'static str> {
    Some(match action {
        ScenarioAction::CreateTopic(value) => topic_contract(value),
        ScenarioAction::CreatePartitions(value) if value.expected_error_code.is_some() => {
            "ADMIN-019"
        }
        ScenarioAction::DeleteTopic(value) if value.expected_error_code.is_some() => "ADMIN-019",
        ScenarioAction::DescribeTopic(value) if value.expected_error_code.is_some() => "ADMIN-019",
        ScenarioAction::ListOffsets(value) if value.expected_error_code.is_some() => "ADMIN-019",
        ScenarioAction::CreatePartitions(value) if value.validate_only => "ADMIN-021",
        ScenarioAction::CreatePartitions(value) if value.replica_assignments.is_some() => {
            "ADMIN-085"
        }
        ScenarioAction::AlterTopicConfig(value) if value.validate_only => "ADMIN-022",
        ScenarioAction::ListConsumerGroupOffsetsBatch(_) => "ADMIN-023",
        ScenarioAction::ListConsumerGroupsOffsets(_) => "ADMIN-024",
        ScenarioAction::AlterConsumerGroupOffsets(value) => alter_contract(value),
        ScenarioAction::DeleteConsumerGroupOffsets(_) => "ADMIN-026",
        ScenarioAction::DescribeClassicGroups(_) => "ADMIN-027",
        ScenarioAction::DescribeConsumerGroups(_) => "ADMIN-069",
        ScenarioAction::ListOffsetsBatch(value)
            if value.read_isolation == testlab_schema::AdminReadIsolation::ReadUncommitted =>
        {
            "ADMIN-089"
        }
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
        ScenarioAction::DescribeClientQuota(value) => quota_contract(value.strict),
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
        ScenarioAction::CreateTopicsBatch(value) => crate::admin_batch::contract(value),
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
        ScenarioAction::ExerciseStreamsGroupAdminLifecycle(value) => streams_contract(value),
        ScenarioAction::DescribeProducers(value) => producer_contract(value),
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
        ScenarioAction::DescribeConsumerGroup(value) => group_contract(value),
        ScenarioAction::AlterConsumerGroupOffset(_) => "ADMIN-011",
        ScenarioAction::DeleteConsumerGroupOffset(_) => "ADMIN-012",
        ScenarioAction::DeleteConsumerGroup(_) => "ADMIN-013",
        _ => return None,
    })
}
