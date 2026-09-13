//! Mixed group-description targets retain exact wire and independent query order.

use testlab_schema::{
    AdapterCommand, ClientId, ConsumerGroupDescriptionExpectation, DescribeConsumerGroupsAction,
    DescribeConsumerGroupsCommand, GroupProtocol, OperationId, ScenarioAction,
};

use crate::group_cli_observation;
use crate::observer_admin_target::AdminTarget;

#[test]
fn mixed_descriptions_preserve_order_size_and_exact_wire_identity() {
    let action = action();
    let wire_command = command(vec!["consumer-group", "classic-group"]);
    let target = AdminTarget::from_exact(&action, &wire_command)
        .unwrap_or_else(|error| panic!("target: {error}"))
        .unwrap_or_else(|| panic!("mixed description target"));
    let AdminTarget::ConsumerGroupDescriptions(target_value) = &target else {
        panic!("mixed group target kind");
    };
    assert_eq!(target_value.group_ids, ["consumer-group", "classic-group"]);
    assert_eq!(target.observation_count(), 2);
    assert!(group_cli_observation::supports(&target));
    assert_eq!(
        group_cli_observation::selection(&target),
        ["--group", "consumer-group", "--group", "classic-group"]
    );

    let mismatched = command(vec!["classic-group", "consumer-group"]);
    assert!(AdminTarget::from_exact(&action, &mismatched).is_err());
}

#[test]
fn cli_observation_returns_requested_groups_in_caller_order() {
    let target =
        AdminTarget::from_exact(&action(), &command(vec!["consumer-group", "classic-group"]))
            .unwrap_or_else(|error| panic!("target: {error}"))
            .unwrap_or_else(|| panic!("mixed description target"));
    let stdout = b"GROUP COORDINATOR (ID) ASSIGNMENT-STRATEGY STATE #MEMBERS\nclassic-group broker:9092 (1) range Stable 1\nGROUP COORDINATOR (ID) ASSIGNMENT-STRATEGY STATE #MEMBERS\nconsumer-group broker:9092 (1) uniform Stable 1\n";

    let observed = group_cli_observation::normalize(7, &target, stdout)
        .unwrap_or_else(|error| panic!("normalize: {error}"));
    let testlab_schema::BrokerStateObservation::ConsumerGroup(first) = &observed[0] else {
        panic!("consumer group observation");
    };
    let testlab_schema::BrokerStateObservation::ConsumerGroup(second) = &observed[1] else {
        panic!("consumer group observation");
    };
    assert_eq!(
        (first.observation, first.group_id.as_str()),
        (7, "consumer-group")
    );
    assert_eq!(
        (second.observation, second.group_id.as_str()),
        (8, "classic-group")
    );
}

fn action() -> ScenarioAction {
    ScenarioAction::DescribeConsumerGroups(DescribeConsumerGroupsAction {
        client_id: client(),
        operation_id: operation(),
        groups: vec![
            expectation("consumer-group", GroupProtocol::Consumer, "uniform"),
            expectation("classic-group", GroupProtocol::Classic, "range"),
        ],
        include_authorized_operations: true,
        timeout_ms: 1_000,
    })
}

fn command(groups: Vec<&str>) -> AdapterCommand {
    AdapterCommand::DescribeConsumerGroups(DescribeConsumerGroupsCommand {
        client_id: client(),
        operation_id: operation(),
        group_ids: groups.into_iter().map(str::to_owned).collect(),
        include_authorized_operations: true,
        timeout_ms: 1_000,
    })
}

fn expectation(
    group_id: &str,
    protocol: GroupProtocol,
    assignor: &str,
) -> ConsumerGroupDescriptionExpectation {
    ConsumerGroupDescriptionExpectation {
        group_id: group_id.to_owned(),
        protocol,
        expected_state: "Stable".to_owned(),
        expected_member_count: 1,
        expected_assignor_name: assignor.to_owned(),
        expected_topic: "records".to_owned(),
        expected_partition: 0,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-groups").unwrap_or_else(|error| panic!("operation id: {error}"))
}
