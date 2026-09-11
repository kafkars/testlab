//! Mixed consumer-group descriptions retain exact public protocol and member facts.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminConsumerGroupDescriptionOutcome,
    AdminConsumerGroupDescriptionValue, AdminConsumerGroupMemberDescription,
    AdminConsumerGroupTopicAssignment, AdminConsumerGroupsDescription, CommandId,
    DescribeConsumerGroupsCommand, GroupProtocol,
};

use crate::AdapterError;
use crate::kafkars_api::{
    ConsumerGroupDescription, ConsumerGroupDescriptionDetails, ConsumerGroupMember,
    ConsumerGroupMemberDetails,
};
use crate::protocol::emit;
use crate::protocol_admin_plural_result::{ResourceResult, ordered_group_results};
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeConsumerGroupsCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .describe_consumer_groups(command.group_ids.clone())
        .include_authorized_operations(false)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let groups = ordered_group_results(
        result.into_groups().into_entries(),
        &command.group_ids,
        &command.operation_id,
        "consumer-group description",
    )?;
    let outcomes = groups
        .into_iter()
        .map(|group| match group.result {
            ResourceResult::Success(description) => Ok(AdminConsumerGroupDescriptionOutcome {
                group_id: group.group_id,
                description: Some(public_description(&command.operation_id, &description)?),
                error_code: None,
            }),
            ResourceResult::Failure(error_code) => Ok(AdminConsumerGroupDescriptionOutcome {
                group_id: group.group_id,
                description: None,
                error_code: Some(error_code),
            }),
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ConsumerGroupsDescribed(AdminConsumerGroupsDescription {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

fn public_description(
    operation_id: &testlab_schema::OperationId,
    description: &ConsumerGroupDescription,
) -> Result<AdminConsumerGroupDescriptionValue, AdapterError> {
    let member_count = u32::try_from(description.members().len())
        .map_err(|_| invalid(operation_id, "returned too many consumer-group members"))?;
    let (protocol, protocol_type, group_epoch, assignment_epoch, assignor_name) =
        match description.details() {
            ConsumerGroupDescriptionDetails::Classic(details) => (
                GroupProtocol::Classic,
                Some(details.protocol_type().to_owned()),
                None,
                None,
                details.protocol_data().to_owned(),
            ),
            ConsumerGroupDescriptionDetails::Consumer(details) => (
                GroupProtocol::Consumer,
                None,
                Some(details.group_epoch()),
                Some(details.assignment_epoch()),
                details.assignor_name().to_owned(),
            ),
        };
    let members = description
        .members()
        .iter()
        .map(|member| public_member(operation_id, protocol, member))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AdminConsumerGroupDescriptionValue {
        state: description.state().to_owned(),
        protocol,
        member_count,
        protocol_type,
        group_epoch,
        assignment_epoch,
        assignor_name,
        members,
    })
}

fn public_member(
    operation_id: &testlab_schema::OperationId,
    protocol: GroupProtocol,
    member: &ConsumerGroupMember,
) -> Result<AdminConsumerGroupMemberDescription, AdapterError> {
    let common = (
        member.member_id().to_owned(),
        member.group_instance_id().map(str::to_owned),
        member.client_id().to_owned(),
        member.client_host().to_owned(),
    );
    match (protocol, member.details()) {
        (GroupProtocol::Classic, ConsumerGroupMemberDetails::Classic(details)) => {
            Ok(AdminConsumerGroupMemberDescription {
                member_id: common.0,
                group_instance_id: common.1,
                client_id: common.2,
                client_host: common.3,
                rack_id: None,
                member_epoch: None,
                subscribed_topic_names: Vec::new(),
                subscribed_topic_regex: None,
                assignment: Vec::new(),
                target_assignment: Vec::new(),
                member_type: None,
                classic_metadata: details.metadata().to_vec(),
                classic_assignment: details.assignment().to_vec(),
            })
        }
        (GroupProtocol::Consumer, ConsumerGroupMemberDetails::Consumer(details)) => {
            Ok(AdminConsumerGroupMemberDescription {
                member_id: common.0,
                group_instance_id: common.1,
                client_id: common.2,
                client_host: common.3,
                rack_id: details.rack_id().map(str::to_owned),
                member_epoch: Some(details.member_epoch()),
                subscribed_topic_names: details.subscribed_topic_names().to_vec(),
                subscribed_topic_regex: details.subscribed_topic_regex().map(str::to_owned),
                assignment: public_assignment(details.assignment()),
                target_assignment: public_assignment(details.target_assignment()),
                member_type: details.member_type(),
                classic_metadata: Vec::new(),
                classic_assignment: Vec::new(),
            })
        }
        _ => Err(invalid(
            operation_id,
            "returned mismatched group and member protocol variants",
        )),
    }
}

fn public_assignment(
    assignment: &crate::kafkars_api::ConsumerGroupAssignment,
) -> Vec<AdminConsumerGroupTopicAssignment> {
    assignment
        .topics()
        .iter()
        .map(|topic| AdminConsumerGroupTopicAssignment {
            topic_id: topic.topic_id(),
            topic_name: topic.topic_name().to_owned(),
            partitions: topic.partitions().to_vec(),
        })
        .collect()
}

fn invalid(operation_id: &testlab_schema::OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
