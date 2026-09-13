//! Group registration policy tests distinguish builder readback from returned-handle identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, ConsumerFetchConfiguration, ConsumerLimitsConfiguration,
    GroupClassicAssignor, GroupConsumerConfiguration, GroupConsumerConfigurationSelection,
    GroupOffsetReset, GroupOperationConfigMethod, GroupProtocol, GroupReadIsolation, Scenario,
    ScenarioAction,
};

#[test]
fn configured_policy_is_exact_correlated_unique_and_later() {
    let scenario = configured_scenario();
    let exact = history(&scenario, observation(Some(selection())), 2);
    assert!(super::violations(&scenario, exact.clone()).is_empty());

    assert_policy(&super::violations(
        &scenario,
        history(&scenario, observation(None), 2),
    ));

    let mut changed = selection();
    changed.processing_timeout_ms = Some(61_001);
    assert_policy(&super::violations(
        &scenario,
        history(&scenario, observation(Some(changed)), 2),
    ));

    let mut wrong_protocol = observation(Some(selection()));
    wrong_protocol.selected_protocol = GroupProtocol::Consumer;
    assert_policy(&super::violations(
        &scenario,
        history(&scenario, wrong_protocol, 2),
    ));

    let mut duplicate = exact;
    duplicate.push(super::event(
        3,
        "group",
        AdapterEvent::GroupConsumerCreated(observation(Some(selection()))),
    ));
    assert_policy(&super::violations(&scenario, duplicate));

    assert_policy(&super::violations(
        &scenario,
        history(&scenario, observation(Some(selection())), 0),
    ));
}

#[test]
fn unconfigured_policy_requires_exact_omission() {
    let mut scenario = super::registration_scenario();
    scenario.steps.truncate(1);
    let exact = history(&scenario, super::group_observation(), 2);
    assert!(super::violations(&scenario, exact).is_empty());

    let mut unexpected = super::group_observation();
    unexpected.selected_configuration = Some(selection());
    assert_policy(&super::violations(
        &scenario,
        history(&scenario, unexpected, 2),
    ));
}

fn configured_scenario() -> Scenario {
    let mut scenario = super::registration_scenario();
    scenario.steps.truncate(1);
    let ScenarioAction::CreateGroupConsumer {
        protocol,
        configuration: selected,
        ..
    } = &mut scenario.steps[0].action
    else {
        unreachable!("group registration fixture");
    };
    *protocol = GroupProtocol::Classic;
    *selected = Some(configuration());
    scenario
}

fn history(
    scenario: &Scenario,
    observation: testlab_schema::GroupConsumerRegistrationObservation,
    event_sequence: u64,
) -> Vec<testlab_schema::HistoryEntry> {
    vec![
        super::command(1, "group", command(scenario)),
        super::event(
            event_sequence,
            "group",
            AdapterEvent::GroupConsumerCreated(observation),
        ),
    ]
}

fn command(scenario: &Scenario) -> AdapterCommand {
    let ScenarioAction::CreateGroupConsumer {
        client_id,
        consumer_id,
        group_id,
        topics,
        protocol,
        configuration,
    } = &scenario.steps[0].action
    else {
        unreachable!("group registration fixture");
    };
    AdapterCommand::CreateGroupConsumer {
        client_id: client_id.clone(),
        consumer_id: consumer_id.clone(),
        group_id: group_id.clone(),
        topics: topics.clone(),
        protocol: *protocol,
        configuration: configuration.clone(),
    }
}

fn observation(
    selected_configuration: Option<GroupConsumerConfigurationSelection>,
) -> testlab_schema::GroupConsumerRegistrationObservation {
    let mut observation = super::group_observation();
    observation.selected_protocol = GroupProtocol::Classic;
    observation.selected_configuration = selected_configuration;
    observation
}

fn selection() -> GroupConsumerConfigurationSelection {
    GroupConsumerConfigurationSelection::from(&configuration())
}

fn configuration() -> GroupConsumerConfiguration {
    GroupConsumerConfiguration {
        offset_reset: GroupOffsetReset::Latest,
        read_isolation: GroupReadIsolation::ReadCommitted,
        fetch: Some(ConsumerFetchConfiguration {
            max_wait_ms: 250,
            min_bytes: 2,
            max_bytes: 524_288,
            partition_max_bytes: 262_144,
            attempt_timeout_ms: 17_000,
        }),
        limits: Some(ConsumerLimitsConfiguration {
            in_flight_fetches: 3,
            buffered_batches: 4,
            buffered_bytes: 2_097_152,
            max_batch_bytes: 524_288,
        }),
        processing_timeout_ms: Some(61_000),
        membership_start_timeout_ms: Some(23_000),
        seek_timeout_ms: Some(17_000),
        close_timeout_ms: Some(19_000),
        operation_config_method: GroupOperationConfigMethod::OperationConfig,
        group_instance_id: Some("worker-1".to_owned()),
        classic_assignor: Some(GroupClassicAssignor::CooperativeSticky),
        classic_session_timeout_ms: Some(11_000),
        classic_rebalance_timeout_ms: Some(31_000),
        classic_heartbeat_interval_ms: Some(4_000),
        classic_heartbeat_attempt_timeout_ms: Some(12_000),
        classic_rejoin_backoff_ms: Some(2_000),
        classic_rejoin_attempt_timeout_ms: Some(32_000),
    }
}

fn assert_policy(violations: &[testlab_schema::Violation]) {
    super::assert_contract(violations, "CONS-035");
}
