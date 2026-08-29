//! Exact admin-command matching prevents scenario expectations from leaking onto the wire.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    Some(match action {
        ScenarioAction::CreateTopic(value) => &value.operation_id,
        ScenarioAction::CreatePartitions(value) => &value.operation_id,
        ScenarioAction::DeleteTopic(value) => &value.operation_id,
        ScenarioAction::DescribeTopic(value) => &value.operation_id,
        ScenarioAction::ListTopics(value) => &value.operation_id,
        ScenarioAction::ListOffsets(value) => &value.operation_id,
        ScenarioAction::DescribeCluster(value) => &value.operation_id,
        ScenarioAction::ListConsumerGroups(value) => &value.operation_id,
        ScenarioAction::DescribeConsumerGroup(value) => &value.operation_id,
        ScenarioAction::ListConsumerGroupOffsets(value) => &value.operation_id,
        ScenarioAction::AlterConsumerGroupOffset(value) => &value.operation_id,
        ScenarioAction::DeleteConsumerGroupOffset(value) => &value.operation_id,
        ScenarioAction::DeleteConsumerGroup(value) => &value.operation_id,
        _ => return None,
    })
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    Some(match command {
        AdapterCommand::CreateTopic(value) => &value.operation_id,
        AdapterCommand::CreatePartitions(value) => &value.operation_id,
        AdapterCommand::DeleteTopic(value) => &value.operation_id,
        AdapterCommand::DescribeTopic(value) => &value.operation_id,
        AdapterCommand::ListTopics(value) => &value.operation_id,
        AdapterCommand::ListOffsets(value) => &value.operation_id,
        AdapterCommand::DescribeCluster(value) => &value.operation_id,
        AdapterCommand::ListConsumerGroups(value) => &value.operation_id,
        AdapterCommand::DescribeConsumerGroup(value) => &value.operation_id,
        AdapterCommand::ListConsumerGroupOffsets(value) => &value.operation_id,
        AdapterCommand::AlterConsumerGroupOffset(value) => &value.operation_id,
        AdapterCommand::DeleteConsumerGroupOffset(value) => &value.operation_id,
        AdapterCommand::DeleteConsumerGroup(value) => &value.operation_id,
        _ => return None,
    })
}

#[allow(clippy::too_many_lines, reason = "exact exhaustive command matching")]
pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> bool {
    match (action, command) {
        (ScenarioAction::CreateTopic(a), AdapterCommand::CreateTopic(c)) => {
            a.client_id == c.client_id
                && a.operation_id == c.operation_id
                && a.topic == c.topic
                && a.partitions == c.partitions
                && a.replication_factor == c.replication_factor
                && a.validate_only == c.validate_only
                && a.timeout_ms == c.timeout_ms
        }
        (ScenarioAction::CreatePartitions(a), AdapterCommand::CreatePartitions(c)) => {
            same_topic(
                &a.client_id,
                &a.operation_id,
                &a.topic,
                a.timeout_ms,
                &c.client_id,
                &c.operation_id,
                &c.topic,
                c.timeout_ms,
            ) && a.total_count == c.total_count
                && a.validate_only == c.validate_only
        }
        (ScenarioAction::DeleteTopic(a), AdapterCommand::DeleteTopic(c)) => same_topic(
            &a.client_id,
            &a.operation_id,
            &a.topic,
            a.timeout_ms,
            &c.client_id,
            &c.operation_id,
            &c.topic,
            c.timeout_ms,
        ),
        (ScenarioAction::DescribeTopic(a), AdapterCommand::DescribeTopic(c)) => same_topic(
            &a.client_id,
            &a.operation_id,
            &a.topic,
            a.timeout_ms,
            &c.client_id,
            &c.operation_id,
            &c.topic,
            c.timeout_ms,
        ),
        (ScenarioAction::ListTopics(a), AdapterCommand::ListTopics(c)) => {
            same_base(
                &a.client_id,
                &a.operation_id,
                a.timeout_ms,
                &c.client_id,
                &c.operation_id,
                c.timeout_ms,
            ) && a.include_internal == c.include_internal
        }
        (ScenarioAction::ListOffsets(a), AdapterCommand::ListOffsets(c)) => {
            same_topic(
                &a.client_id,
                &a.operation_id,
                &a.topic,
                a.timeout_ms,
                &c.client_id,
                &c.operation_id,
                &c.topic,
                c.timeout_ms,
            ) && a.partition == c.partition
                && a.position == c.position
        }
        (ScenarioAction::DescribeCluster(a), AdapterCommand::DescribeCluster(c)) => same_base(
            &a.client_id,
            &a.operation_id,
            a.timeout_ms,
            &c.client_id,
            &c.operation_id,
            c.timeout_ms,
        ),
        (ScenarioAction::ListConsumerGroups(a), AdapterCommand::ListConsumerGroups(c)) => {
            same_base(
                &a.client_id,
                &a.operation_id,
                a.timeout_ms,
                &c.client_id,
                &c.operation_id,
                c.timeout_ms,
            )
        }
        (ScenarioAction::DescribeConsumerGroup(a), AdapterCommand::DescribeConsumerGroup(c)) => {
            same_group(
                &a.client_id,
                &a.operation_id,
                &a.group_id,
                a.timeout_ms,
                &c.client_id,
                &c.operation_id,
                &c.group_id,
                c.timeout_ms,
            )
        }
        (
            ScenarioAction::ListConsumerGroupOffsets(a),
            AdapterCommand::ListConsumerGroupOffsets(c),
        ) => {
            same_group_offset(
                &a.client_id,
                &a.operation_id,
                &a.group_id,
                &a.topic,
                a.partition,
                a.timeout_ms,
                &c.client_id,
                &c.operation_id,
                &c.group_id,
                &c.topic,
                c.partition,
                c.timeout_ms,
            ) && a.require_stable == c.require_stable
        }
        (
            ScenarioAction::AlterConsumerGroupOffset(a),
            AdapterCommand::AlterConsumerGroupOffset(c),
        ) => {
            same_group_offset(
                &a.client_id,
                &a.operation_id,
                &a.group_id,
                &a.topic,
                a.partition,
                a.timeout_ms,
                &c.client_id,
                &c.operation_id,
                &c.group_id,
                &c.topic,
                c.partition,
                c.timeout_ms,
            ) && a.offset == c.offset
        }
        (
            ScenarioAction::DeleteConsumerGroupOffset(a),
            AdapterCommand::DeleteConsumerGroupOffset(c),
        ) => same_group_offset(
            &a.client_id,
            &a.operation_id,
            &a.group_id,
            &a.topic,
            a.partition,
            a.timeout_ms,
            &c.client_id,
            &c.operation_id,
            &c.group_id,
            &c.topic,
            c.partition,
            c.timeout_ms,
        ),
        (ScenarioAction::DeleteConsumerGroup(a), AdapterCommand::DeleteConsumerGroup(c)) => {
            same_group(
                &a.client_id,
                &a.operation_id,
                &a.group_id,
                a.timeout_ms,
                &c.client_id,
                &c.operation_id,
                &c.group_id,
                c.timeout_ms,
            )
        }
        _ => false,
    }
}

fn same_base(
    a_client: &testlab_schema::ClientId,
    a_operation: &OperationId,
    a_timeout: u64,
    c_client: &testlab_schema::ClientId,
    c_operation: &OperationId,
    c_timeout: u64,
) -> bool {
    a_client == c_client && a_operation == c_operation && a_timeout == c_timeout
}

#[allow(
    clippy::too_many_arguments,
    reason = "exact matching keeps both scenario and wire topic identities explicit"
)]
fn same_topic(
    a_client: &testlab_schema::ClientId,
    a_operation: &OperationId,
    a_topic: &str,
    a_timeout: u64,
    c_client: &testlab_schema::ClientId,
    c_operation: &OperationId,
    c_topic: &str,
    c_timeout: u64,
) -> bool {
    same_base(
        a_client,
        a_operation,
        a_timeout,
        c_client,
        c_operation,
        c_timeout,
    ) && a_topic == c_topic
}

#[allow(
    clippy::too_many_arguments,
    reason = "exact matching keeps both scenario and wire group identities explicit"
)]
fn same_group(
    a_client: &testlab_schema::ClientId,
    a_operation: &OperationId,
    a_group: &str,
    a_timeout: u64,
    c_client: &testlab_schema::ClientId,
    c_operation: &OperationId,
    c_group: &str,
    c_timeout: u64,
) -> bool {
    same_base(
        a_client,
        a_operation,
        a_timeout,
        c_client,
        c_operation,
        c_timeout,
    ) && a_group == c_group
}

#[allow(
    clippy::too_many_arguments,
    reason = "exact matching keeps both scenario and wire group-offset identities explicit"
)]
fn same_group_offset(
    a_client: &testlab_schema::ClientId,
    a_operation: &OperationId,
    a_group: &str,
    a_topic: &str,
    a_partition: i32,
    a_timeout: u64,
    c_client: &testlab_schema::ClientId,
    c_operation: &OperationId,
    c_group: &str,
    c_topic: &str,
    c_partition: i32,
    c_timeout: u64,
) -> bool {
    same_group(
        a_client,
        a_operation,
        a_group,
        a_timeout,
        c_client,
        c_operation,
        c_group,
        c_timeout,
    ) && a_topic == c_topic
        && a_partition == c_partition
}
