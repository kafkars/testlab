//! Admin command routing keeps public read and write operations separate.

use std::io::Write;

use testlab_schema::{AdapterCommand, CommandId};

use crate::AdapterError;
use crate::protocol_admin_classic_group;
use crate::protocol_admin_cluster;
use crate::protocol_admin_config;
use crate::protocol_admin_group;
use crate::protocol_admin_group_offset_batch;
use crate::protocol_admin_group_offset_batch_mutation;
use crate::protocol_admin_list_offsets_batch;
use crate::protocol_admin_read;
use crate::protocol_admin_write;
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        command
        @ (AdapterCommand::AlterClientQuota(_) | AdapterCommand::DescribeClientQuota(_)) => {
            crate::protocol_admin_client_quota::dispatch(state, writer, command_id, command)
        }
        command @ (AdapterCommand::AlterUserScramCredential(_)
        | AdapterCommand::DescribeUserScramCredential(_)) => {
            crate::protocol_admin_user_scram::dispatch(state, writer, command_id, command)
        }
        command @ (AdapterCommand::CreateAcls(_)
        | AdapterCommand::DescribeAcls(_)
        | AdapterCommand::DeleteAcls(_)) => {
            crate::protocol_admin_acl::dispatch(state, writer, command_id, command)
        }
        command @ (AdapterCommand::CreateTopic(_)
        | AdapterCommand::CreateTopicsBatch(_)
        | AdapterCommand::CreatePartitions(_)
        | AdapterCommand::DeleteTopic(_)
        | AdapterCommand::DeleteTopics(_)
        | AdapterCommand::DeleteRecords(_)) => {
            protocol_admin_write::dispatch(state, writer, command_id, command)
        }
        command @ (AdapterCommand::DescribeTopic(_)
        | AdapterCommand::DescribeTopics(_)
        | AdapterCommand::ListTopics(_)
        | AdapterCommand::ListOffsets(_)
        | AdapterCommand::ListConsumerGroupOffsets(_)) => {
            protocol_admin_read::dispatch(state, writer, command_id, command)
        }
        AdapterCommand::ListOffsetsBatch(command) => {
            protocol_admin_list_offsets_batch::list(state, writer, command_id, command)
        }
        AdapterCommand::DeleteRecordsBatch(command) => {
            crate::protocol_admin_delete_records_batch::delete(state, writer, command_id, command)
        }
        command @ (AdapterCommand::ListConsumerGroupOffsetsBatch(_)
        | AdapterCommand::ListConsumerGroupsOffsets(_)) => {
            protocol_admin_group_offset_batch::dispatch(state, writer, command_id, command)
        }
        command @ (AdapterCommand::AlterConsumerGroupOffsets(_)
        | AdapterCommand::DeleteConsumerGroupOffsets(_)) => {
            protocol_admin_group_offset_batch_mutation::dispatch(state, writer, command_id, command)
        }
        AdapterCommand::DescribeClassicGroups(command) => {
            protocol_admin_classic_group::describe(state, writer, command_id, command)
        }
        AdapterCommand::DeleteConsumerGroups(command) => {
            crate::protocol_admin_consumer_group_deletion_batch::delete(
                state, writer, command_id, command,
            )
        }
        AdapterCommand::DescribeCluster(command) => {
            protocol_admin_cluster::describe(state, writer, command_id, command)
        }
        AdapterCommand::DescribeFeatures(command) => {
            crate::protocol_admin_features::describe(state, writer, command_id, command)
        }
        AdapterCommand::DescribeProducers(command) => {
            crate::protocol_admin_producers::describe(state, writer, command_id, command)
        }
        command @ (AdapterCommand::ListTransactions(_)
        | AdapterCommand::DescribeTransactions(_)) => {
            crate::protocol_admin_transactions::dispatch(state, writer, command_id, command)
        }
        command @ (AdapterCommand::DescribeTopicConfig(_)
        | AdapterCommand::DescribeTopicConfigs(_)
        | AdapterCommand::AlterTopicConfigs(_)
        | AdapterCommand::AlterTopicConfig(_)) => {
            protocol_admin_config::dispatch(state, writer, command_id, command)
        }
        command @ (AdapterCommand::ListConsumerGroups(_)
        | AdapterCommand::DescribeConsumerGroup(_)
        | AdapterCommand::AlterConsumerGroupOffset(_)
        | AdapterCommand::DeleteConsumerGroupOffset(_)
        | AdapterCommand::DeleteConsumerGroup(_)) => {
            protocol_admin_group::dispatch(state, writer, command_id, command)
        }
        AdapterCommand::DescribeShareGroup(command) => {
            crate::protocol_admin_share_group::describe(state, writer, command_id, command)
        }
        AdapterCommand::DescribeShareGroups(command) => {
            crate::protocol_admin_share_group_description_batch::describe(
                state, writer, command_id, command,
            )
        }
        AdapterCommand::ListShareGroupOffsets(command) => {
            crate::protocol_admin_share_group::list_offsets(state, writer, command_id, command)
        }
        AdapterCommand::ListShareGroupsOffsets(command) => {
            crate::protocol_admin_share_group_offset_batch::list(state, writer, command_id, command)
        }
        AdapterCommand::AlterShareGroupOffsets(command) => {
            crate::protocol_admin_share_group::alter_offsets(state, writer, command_id, command)
        }
        AdapterCommand::DeleteShareGroupOffsets(command) => {
            crate::protocol_admin_share_group_offset_deletion::delete(
                state, writer, command_id, command,
            )
        }
        AdapterCommand::DeleteShareGroups(command) => {
            crate::protocol_admin_share_group_deletion::delete(state, writer, command_id, command)
        }
        _ => Err(AdapterError::AdminResult(
            "non-admin command reached admin dispatcher".to_owned(),
        )),
    }
}
