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
        command @ (AdapterCommand::CreateTopic(_)
        | AdapterCommand::CreateTopicsBatch(_)
        | AdapterCommand::CreatePartitions(_)
        | AdapterCommand::DeleteTopic(_)
        | AdapterCommand::DeleteRecords(_)) => {
            protocol_admin_write::dispatch(state, writer, command_id, command)
        }
        command @ (AdapterCommand::DescribeTopic(_)
        | AdapterCommand::ListTopics(_)
        | AdapterCommand::ListOffsets(_)
        | AdapterCommand::ListConsumerGroupOffsets(_)) => {
            protocol_admin_read::dispatch(state, writer, command_id, command)
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
        AdapterCommand::DescribeCluster(command) => {
            protocol_admin_cluster::describe(state, writer, command_id, command)
        }
        command
        @ (AdapterCommand::DescribeTopicConfig(_) | AdapterCommand::AlterTopicConfig(_)) => {
            protocol_admin_config::dispatch(state, writer, command_id, command)
        }
        command @ (AdapterCommand::ListConsumerGroups(_)
        | AdapterCommand::DescribeConsumerGroup(_)
        | AdapterCommand::AlterConsumerGroupOffset(_)
        | AdapterCommand::DeleteConsumerGroupOffset(_)
        | AdapterCommand::DeleteConsumerGroup(_)) => {
            protocol_admin_group::dispatch(state, writer, command_id, command)
        }
        _ => Err(AdapterError::AdminResult(
            "non-admin command reached admin dispatcher".to_owned(),
        )),
    }
}
