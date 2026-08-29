//! Broker-policy scenario tests pin complete cleanup and checked-in coverage validity.

use crate::{BrokerPolicyState, Scenario, ScenarioAction};

const SCENARIOS: &[&str] = &[
    include_str!("../../../scenarios/kafka/producer-topic-authorization-recovery.toml"),
    include_str!("../../../scenarios/kafka/classic-group-authorization-recovery.toml"),
    include_str!("../../../scenarios/kafka/admin-topic-authorization-recovery.toml"),
    include_str!("../../../scenarios/kafka/transactional-id-authorization-recovery.toml"),
    include_str!("../../../scenarios/kafka/producer-quota-progress.toml"),
    include_str!("../../../scenarios/kafka/consumer-quota-progress.toml"),
];

#[test]
fn checked_in_policy_scenarios_validate() {
    for source in SCENARIOS {
        let scenario: Scenario = toml::from_str(source)
            .unwrap_or_else(|error| panic!("parse broker policy scenario: {error}"));
        scenario
            .validate()
            .unwrap_or_else(|error| panic!("validate {}: {error}", scenario.id));
    }
}

#[test]
fn an_established_policy_must_be_removed() {
    let mut scenario: Scenario = toml::from_str(SCENARIOS[0])
        .unwrap_or_else(|error| panic!("parse broker policy scenario: {error}"));
    scenario.steps.retain(|step| {
        !matches!(
            &step.action,
            ScenarioAction::AlterBrokerPolicy(action)
                if action.state == BrokerPolicyState::Absent
        )
    });

    let error = match scenario.validate() {
        Ok(()) => panic!("an active policy must invalidate the scenario"),
        Err(error) => error,
    };

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("was not removed")),
        "{:?}",
        error.problems
    );
}
