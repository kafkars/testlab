//! Detailed public values for mixed group-description verifier tests.

use testlab_schema::{
    AdminConsumerGroupDescriptionOutcome, AdminConsumerGroupDescriptionValue,
    AdminConsumerGroupMemberDescription, AdminConsumerGroupTopicAssignment,
    ConsumerGroupDescriptionExpectation, GroupProtocol,
};

pub(super) fn consumer_outcome() -> AdminConsumerGroupDescriptionOutcome {
    outcome(
        "consumer-group",
        GroupProtocol::Consumer,
        None,
        Some(5),
        Some(5),
        "uniform",
        consumer_member(),
    )
}

pub(super) fn classic_outcome() -> AdminConsumerGroupDescriptionOutcome {
    outcome(
        "classic-group",
        GroupProtocol::Classic,
        Some("consumer"),
        None,
        None,
        "range",
        classic_member(),
    )
}

fn outcome(
    group_id: &str,
    protocol: GroupProtocol,
    protocol_type: Option<&str>,
    group_epoch: Option<i32>,
    assignment_epoch: Option<i32>,
    assignor_name: &str,
    member: AdminConsumerGroupMemberDescription,
) -> AdminConsumerGroupDescriptionOutcome {
    AdminConsumerGroupDescriptionOutcome {
        group_id: group_id.to_owned(),
        description: Some(AdminConsumerGroupDescriptionValue {
            state: "Stable".to_owned(),
            protocol,
            member_count: 1,
            authorized_operations: Some(1),
            protocol_type: protocol_type.map(str::to_owned),
            group_epoch,
            assignment_epoch,
            assignor_name: assignor_name.to_owned(),
            members: vec![member],
        }),
        error_code: None,
    }
}

fn consumer_member() -> AdminConsumerGroupMemberDescription {
    let assignment = AdminConsumerGroupTopicAssignment {
        topic_id: [1; 16],
        topic_name: "consumer-topic".to_owned(),
        partitions: vec![0],
    };
    member(
        Some(4),
        vec!["consumer-topic".to_owned()],
        vec![assignment],
        Vec::new(),
        Vec::new(),
    )
}

fn classic_member() -> AdminConsumerGroupMemberDescription {
    member(None, Vec::new(), Vec::new(), vec![1], vec![2])
}

fn member(
    member_epoch: Option<i32>,
    subscribed_topic_names: Vec<String>,
    assignment: Vec<AdminConsumerGroupTopicAssignment>,
    classic_metadata: Vec<u8>,
    classic_assignment: Vec<u8>,
) -> AdminConsumerGroupMemberDescription {
    AdminConsumerGroupMemberDescription {
        member_id: "member-1".to_owned(),
        group_instance_id: None,
        client_id: "client-1".to_owned(),
        client_host: "/127.0.0.1".to_owned(),
        rack_id: None,
        member_epoch,
        subscribed_topic_names,
        subscribed_topic_regex: None,
        assignment,
        target_assignment: Vec::new(),
        member_type: None,
        classic_metadata,
        classic_assignment,
    }
}

pub(super) fn expectation(
    group_id: &str,
    topic: &str,
    protocol: GroupProtocol,
    assignor: &str,
) -> ConsumerGroupDescriptionExpectation {
    ConsumerGroupDescriptionExpectation {
        group_id: group_id.to_owned(),
        protocol,
        expected_state: "Stable".to_owned(),
        expected_member_count: 1,
        expected_assignor_name: assignor.to_owned(),
        expected_topic: topic.to_owned(),
        expected_partition: 0,
    }
}
