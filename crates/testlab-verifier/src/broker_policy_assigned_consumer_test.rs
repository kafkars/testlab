//! Topic-READ policy tests require retained public denial and restored record progress.

#[path = "broker_policy_assigned_consumer_test_fixture.rs"]
mod fixture;

use testlab_schema::{
    AdapterEvent, AssignedConsumerEventObservationKind, AssignedConsumerPositionFailure,
    HistoryPayload,
};

use fixture::{has, history, observations, scenario, violations};

#[test]
fn exact_assigned_denial_and_record_recovery_pass() {
    let scenario = scenario();
    let observations = observations(&scenario);
    assert!(violations(&scenario, &history(&scenario), &observations).is_empty());
}

#[test]
fn wrong_public_code_or_missing_broker_record_fails() {
    let scenario = scenario();
    let mut wrong_history = history(&scenario);
    let HistoryPayload::AdapterEvent { event } = &mut wrong_history[4].payload else {
        panic!("assigned event missing");
    };
    let AdapterEvent::AssignedConsumerEventObserved(observation) = &mut event.event else {
        panic!("assigned event observation missing");
    };
    let AssignedConsumerEventObservationKind::PositionResolutionFailed { failure, .. } =
        &mut observation.event
    else {
        panic!("position failure missing");
    };
    *failure = AssignedConsumerPositionFailure::Broker { code: 30 };
    assert!(has(
        &violations(&scenario, &wrong_history, &observations(&scenario)),
        "POLICY-002"
    ));
    assert!(has(
        &violations(&scenario, &history(&scenario), &[]),
        "POLICY-003"
    ));
}
