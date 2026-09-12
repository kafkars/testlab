//! Group protocol tests retain configured policy and its capability boundary.

use crate::{
    Capability, GroupClassicAssignor, GroupConsumerConfiguration, GroupOffsetReset,
    GroupReadIsolation, Scenario, ScenarioAction,
};

#[test]
fn configured_group_policy_round_trips() {
    let action = ScenarioAction::CreateGroupConsumer {
        client_id: id(crate::ClientId::new("client-1")),
        consumer_id: id(crate::ConsumerId::new("consumer-1")),
        group_id: "workers".to_owned(),
        topics: vec!["orders".to_owned(), "returns".to_owned()],
        protocol: crate::GroupProtocol::Classic,
        configuration: Some(GroupConsumerConfiguration {
            offset_reset: GroupOffsetReset::Latest,
            read_isolation: GroupReadIsolation::ReadCommitted,
            fetch: Some(crate::ConsumerFetchConfiguration {
                max_wait_ms: 250,
                min_bytes: 2,
                max_bytes: 524_288,
                partition_max_bytes: 262_144,
                attempt_timeout_ms: 17_000,
            }),
            limits: Some(crate::ConsumerLimitsConfiguration {
                in_flight_fetches: 3,
                buffered_batches: 4,
                buffered_bytes: 2_097_152,
                max_batch_bytes: 524_288,
            }),
            processing_timeout_ms: Some(60_000),
            membership_start_timeout_ms: Some(25_000),
            seek_timeout_ms: Some(15_000),
            close_timeout_ms: Some(20_000),
            group_instance_id: Some("worker-static-1".to_owned()),
            classic_assignor: Some(GroupClassicAssignor::CooperativeSticky),
            classic_session_timeout_ms: Some(120_000),
            classic_rebalance_timeout_ms: Some(150_000),
            classic_heartbeat_interval_ms: Some(2_000),
            classic_heartbeat_attempt_timeout_ms: Some(9_000),
            classic_rejoin_backoff_ms: Some(250),
            classic_rejoin_attempt_timeout_ms: Some(45_000),
        }),
    };
    let encoded = serde_json::to_string(&action)
        .unwrap_or_else(|error| panic!("encode configured group action: {error}"));
    let decoded: ScenarioAction = serde_json::from_str(&encoded)
        .unwrap_or_else(|error| panic!("decode configured group action: {error}"));
    assert_eq!(decoded, action);
}

#[test]
fn configured_group_requires_its_capability() {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-round-trip.toml"
    ))
    .unwrap_or_else(|error| panic!("parse classic group scenario: {error}"));
    let Some(ScenarioAction::CreateGroupConsumer { configuration, .. }) = scenario
        .steps
        .iter_mut()
        .map(|step| &mut step.action)
        .find(|action| matches!(action, ScenarioAction::CreateGroupConsumer { .. }))
    else {
        panic!("group creation missing");
    };
    *configuration = Some(GroupConsumerConfiguration {
        offset_reset: GroupOffsetReset::Latest,
        read_isolation: GroupReadIsolation::ReadUncommitted,
        fetch: None,
        limits: None,
        processing_timeout_ms: None,
        membership_start_timeout_ms: None,
        seek_timeout_ms: None,
        close_timeout_ms: None,
        group_instance_id: None,
        classic_assignor: None,
        classic_session_timeout_ms: None,
        classic_rebalance_timeout_ms: None,
        classic_heartbeat_interval_ms: None,
        classic_heartbeat_attempt_timeout_ms: None,
        classic_rejoin_backoff_ms: None,
        classic_rejoin_attempt_timeout_ms: None,
    });
    let error = match scenario.validate() {
        Ok(()) => panic!("configured group capability must be required"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("group_consumer_configuration"));
    scenario
        .requires
        .insert(Capability::GroupConsumerConfiguration);
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate configured group scenario: {error}"));
}

#[test]
fn classic_configuration_is_protocol_specific_and_timing_is_positive() {
    let mut modern = scenario("../../../scenarios/kafka/consumer-protocol-group-round-trip.toml");
    modern
        .requires
        .insert(Capability::GroupConsumerConfiguration);
    *group_configuration(&mut modern) = Some(GroupConsumerConfiguration {
        offset_reset: GroupOffsetReset::Earliest,
        read_isolation: GroupReadIsolation::ReadUncommitted,
        fetch: None,
        limits: None,
        processing_timeout_ms: None,
        membership_start_timeout_ms: None,
        seek_timeout_ms: None,
        close_timeout_ms: None,
        group_instance_id: None,
        classic_assignor: Some(GroupClassicAssignor::CooperativeSticky),
        classic_session_timeout_ms: Some(10_000),
        classic_rebalance_timeout_ms: Some(30_000),
        classic_heartbeat_interval_ms: Some(3_000),
        classic_heartbeat_attempt_timeout_ms: Some(10_000),
        classic_rejoin_backoff_ms: Some(1_000),
        classic_rejoin_attempt_timeout_ms: Some(30_000),
    });
    let error = modern
        .validate()
        .expect_err("KIP-848 group must reject classic timing");
    let message = error.to_string();
    assert!(message.contains("classic_assignor for a non-classic group"));
    assert!(message.contains("classic_session_timeout_ms for a non-classic group"));
    assert!(message.contains("classic_rebalance_timeout_ms for a non-classic group"));
    assert!(message.contains("classic_heartbeat_interval_ms for a non-classic group"));
    assert!(message.contains("classic_heartbeat_attempt_timeout_ms for a non-classic group"));
    assert!(message.contains("classic_rejoin_backoff_ms for a non-classic group"));
    assert!(message.contains("classic_rejoin_attempt_timeout_ms for a non-classic group"));

    let mut classic = scenario("../../../scenarios/kafka/classic-group-round-trip.toml");
    classic
        .requires
        .insert(Capability::GroupConsumerConfiguration);
    *group_configuration(&mut classic) = Some(GroupConsumerConfiguration {
        offset_reset: GroupOffsetReset::Earliest,
        read_isolation: GroupReadIsolation::ReadUncommitted,
        fetch: None,
        limits: None,
        processing_timeout_ms: None,
        membership_start_timeout_ms: None,
        seek_timeout_ms: None,
        close_timeout_ms: None,
        group_instance_id: None,
        classic_assignor: Some(GroupClassicAssignor::Range),
        classic_session_timeout_ms: Some(0),
        classic_rebalance_timeout_ms: Some(0),
        classic_heartbeat_interval_ms: Some(0),
        classic_heartbeat_attempt_timeout_ms: Some(0),
        classic_rejoin_backoff_ms: Some(0),
        classic_rejoin_attempt_timeout_ms: Some(0),
    });
    let error = classic
        .validate()
        .expect_err("zero classic timing must fail");
    let message = error.to_string();
    for field in [
        "classic_session_timeout_ms",
        "classic_rebalance_timeout_ms",
        "classic_heartbeat_interval_ms",
        "classic_heartbeat_attempt_timeout_ms",
        "classic_rejoin_backoff_ms",
        "classic_rejoin_attempt_timeout_ms",
    ] {
        assert!(message.contains(field), "{message}");
    }
}

#[test]
fn group_runtime_timing_is_positive() {
    let mut scenario = scenario("../../../scenarios/kafka/classic-group-round-trip.toml");
    scenario
        .requires
        .insert(Capability::GroupConsumerConfiguration);
    *group_configuration(&mut scenario) = Some(GroupConsumerConfiguration {
        offset_reset: GroupOffsetReset::Earliest,
        read_isolation: GroupReadIsolation::ReadUncommitted,
        fetch: None,
        limits: None,
        processing_timeout_ms: Some(0),
        membership_start_timeout_ms: Some(0),
        seek_timeout_ms: Some(0),
        close_timeout_ms: Some(0),
        group_instance_id: None,
        classic_assignor: None,
        classic_session_timeout_ms: None,
        classic_rebalance_timeout_ms: None,
        classic_heartbeat_interval_ms: None,
        classic_heartbeat_attempt_timeout_ms: None,
        classic_rejoin_backoff_ms: None,
        classic_rejoin_attempt_timeout_ms: None,
    });
    let message = scenario
        .validate()
        .expect_err("zero group runtime timing must fail")
        .to_string();
    for field in [
        "processing_timeout_ms",
        "membership_start_timeout_ms",
        "seek_timeout_ms",
        "close_timeout_ms",
    ] {
        assert!(message.contains(field), "{message}");
    }
}

fn scenario(path: &str) -> Scenario {
    let source = match path {
        "../../../scenarios/kafka/consumer-protocol-group-round-trip.toml" => {
            include_str!("../../../scenarios/kafka/consumer-protocol-group-round-trip.toml")
        }
        "../../../scenarios/kafka/classic-group-round-trip.toml" => {
            include_str!("../../../scenarios/kafka/classic-group-round-trip.toml")
        }
        _ => panic!("unknown group fixture"),
    };
    toml::from_str(source).unwrap_or_else(|error| panic!("parse group scenario: {error}"))
}

fn group_configuration(scenario: &mut Scenario) -> &mut Option<GroupConsumerConfiguration> {
    scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::CreateGroupConsumer { configuration, .. } => Some(configuration),
            _ => None,
        })
        .unwrap_or_else(|| panic!("group creation missing"))
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
