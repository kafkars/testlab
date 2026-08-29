//! Group protocol tests retain configured policy and its capability boundary.

use crate::{
    Capability, GroupConsumerConfiguration, GroupOffsetReset, GroupReadIsolation, Scenario,
    ScenarioAction,
};

#[test]
fn configured_group_policy_round_trips() {
    let action = ScenarioAction::CreateGroupConsumer {
        client_id: id(crate::ClientId::new("client-1")),
        consumer_id: id(crate::ConsumerId::new("consumer-1")),
        group_id: "workers".to_owned(),
        topic: "orders".to_owned(),
        protocol: crate::GroupProtocol::Consumer,
        configuration: Some(GroupConsumerConfiguration {
            offset_reset: GroupOffsetReset::Latest,
            read_isolation: GroupReadIsolation::ReadCommitted,
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

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
