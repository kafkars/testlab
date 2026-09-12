//! Leader-election schemas pin scenario-only expectations outside the wire command.

use crate::{
    AdapterCommand, AdminLeaderElectionType, ElectLeadersAction, ElectLeadersCommand,
    LeaderElectionSelection, Scenario, ScenarioAction,
};

#[test]
fn checked_in_leader_election_scenario_is_valid() {
    assert_eq!(crate::PROTOCOL_VERSION, 89);
    assert_eq!(crate::SCENARIO_SCHEMA_VERSION, 92);
    assert_eq!(crate::EVIDENCE_SCHEMA_VERSION, 78);
    checked_in_scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate leader-election scenario: {error}"));
}

#[test]
fn scenario_only_transition_requirement_does_not_cross_the_wire() {
    let action = ElectLeadersAction {
        client_id: client(),
        operation_id: operation(),
        election_type: AdminLeaderElectionType::Preferred,
        targets: Some(vec![target()]),
        required_partitions: Vec::new(),
        require_leader_change: true,
        timeout_ms: 20_000,
    };
    let command = ElectLeadersCommand {
        client_id: action.client_id.clone(),
        operation_id: action.operation_id.clone(),
        election_type: action.election_type,
        targets: action.targets.clone(),
        timeout_ms: action.timeout_ms,
    };
    round_trip(&ScenarioAction::ElectLeaders(action));
    round_trip(&AdapterCommand::ElectLeaders(command.clone()));
    let value = serde_json::to_value(command)
        .unwrap_or_else(|error| panic!("serialize leader-election command: {error}"));
    assert!(value.get("required_partitions").is_none());
    assert!(value.get("require_leader_change").is_none());
}

#[test]
fn transition_requirement_needs_one_restored_preferred_target() {
    let mut scenario = checked_in_scenario();
    scenario
        .steps
        .retain(|step| !matches!(step.action, ScenarioAction::RestoreBrokerRole { .. }));
    let problems = match scenario.validate() {
        Ok(()) => panic!("missing restore passed validation"),
        Err(error) => error.problems,
    };
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("prior stop and restore"))
    );
}

fn checked_in_scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-elect-leaders.toml"
    ))
    .unwrap_or_else(|error| panic!("parse leader-election scenario: {error}"))
}

fn target() -> LeaderElectionSelection {
    LeaderElectionSelection {
        topic: "orders".to_owned(),
        partition: 0,
    }
}

fn client() -> crate::ClientId {
    crate::ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> crate::OperationId {
    crate::OperationId::new("admin-elect-leaders")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Eq + std::fmt::Debug,
{
    let json = serde_json::to_vec(value)
        .unwrap_or_else(|error| panic!("serialize leader-election payload: {error}"));
    let decoded = serde_json::from_slice::<T>(&json)
        .unwrap_or_else(|error| panic!("deserialize leader-election payload: {error}"));
    assert_eq!(&decoded, value);
}
