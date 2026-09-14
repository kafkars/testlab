//! Assigned-consumer event scenarios pin both observer methods and exact failure identity.

use crate::{
    AssignedConsumerEventExpectation, AssignedConsumerEventMethod, AssignedConsumerFetchFailure,
    Scenario, ScenarioAction,
};

#[test]
fn authorization_recovery_scenario_is_valid_and_uses_both_observers() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-event-authorization-recovery.toml"
    ))
    .unwrap_or_else(|error| panic!("assigned event scenario: {error}"));
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("assigned event scenario validation: {error}"));

    let observations = scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::ObserveAssignedConsumerEvent(action) => Some(action),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(observations.len(), 2);
    assert_eq!(
        observations[0].method,
        AssignedConsumerEventMethod::NextEvent
    );
    assert_eq!(
        observations[1].method,
        AssignedConsumerEventMethod::TryTakeEvent
    );
    for observation in observations {
        assert!(matches!(
            &observation.expected,
            AssignedConsumerEventExpectation::FetchFailed {
                failure: AssignedConsumerFetchFailure::Broker { code: 29 },
                ..
            }
        ));
    }
}
