//! Leader-election translation keeps scenario-only state outside the protocol.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

#[test]
fn selected_and_cluster_wide_elections_translate_exactly() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-elect-leaders.toml"
    ))
    .unwrap_or_else(|error| panic!("parse leader-election scenario: {error}"));
    let elections = scenario.steps.iter().filter_map(|step| {
        matches!(step.action, ScenarioAction::ElectLeaders(_)).then_some(&step.action)
    });
    let commands = elections
        .map(|action| {
            crate::session_command_admin_leader_election::translate(action)
                .unwrap_or_else(|| panic!("translate leader-election action"))
        })
        .collect::<Vec<_>>();
    assert_eq!(commands.len(), 2);
    let (AdapterCommand::ElectLeaders(selected), ExpectedEvent::LeadersElected(_)) = &commands[0]
    else {
        panic!("selected leader-election translation");
    };
    assert_eq!(selected.targets.as_ref().map(Vec::len), Some(1));
    let (AdapterCommand::ElectLeaders(all), ExpectedEvent::LeadersElected(_)) = &commands[1] else {
        panic!("cluster-wide leader-election translation");
    };
    assert!(all.targets.is_none());
}

#[test]
fn each_election_topic_is_created_after_the_previous_broker_disruption() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-elect-leaders.toml"
    ))
    .unwrap_or_else(|error| panic!("parse leader-election scenario: {error}"));
    let position = |id: &str| {
        scenario
            .steps
            .iter()
            .position(|step| step.id.as_str() == id)
            .unwrap_or_else(|| panic!("missing scenario step {id}"))
    };
    assert!(
        position("elect-selected-preferred-leader") < position("create-all-election-topic"),
        "the selected-election broker stop must not move the later topic off its preferred leader"
    );
    assert!(position("create-all-election-topic") < position("stop-all-election-topic-leader"));
}
