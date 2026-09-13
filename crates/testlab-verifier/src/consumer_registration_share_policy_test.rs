//! Share registration policy tests retain the exact successful public builder attempt.

use testlab_schema::{
    AdapterCommand, AdapterEvent, Scenario, ScenarioAction, ShareConsumerFetchConfiguration,
};

#[test]
fn policy_is_exact_bounded_correlated_unique_and_later() {
    let scenario = configured_scenario();
    let exact = history(&scenario, observation(29_500_000_123), 2);
    assert!(super::violations(&scenario, exact.clone()).is_empty());

    assert_policy(&super::violations(
        &scenario,
        vec![super::command(1, "share", command(&scenario))],
    ));

    let mut wrong_rack = observation(29_500_000_123);
    wrong_rack.selected_builder.rack = None;
    assert_policy(&super::violations(
        &scenario,
        history(&scenario, wrong_rack, 2),
    ));

    let mut wrong_fetch = observation(29_500_000_123);
    wrong_fetch
        .selected_builder
        .fetch
        .as_mut()
        .expect("configured Fetch")
        .max_records += 1;
    assert_policy(&super::violations(
        &scenario,
        history(&scenario, wrong_fetch, 2),
    ));

    for membership_timeout_ns in [0, 30_000_000_001] {
        assert_policy(&super::violations(
            &scenario,
            history(&scenario, observation(membership_timeout_ns), 2),
        ));
    }

    let mut wrong_close = observation(29_500_000_123);
    wrong_close.selected_builder.close_timeout_ms += 1;
    assert_policy(&super::violations(
        &scenario,
        history(&scenario, wrong_close, 2),
    ));

    let mut duplicate = exact;
    duplicate.push(super::event(
        3,
        "share",
        AdapterEvent::ShareConsumerCreated(observation(29_000_000_456)),
    ));
    assert_policy(&super::violations(&scenario, duplicate));
    assert_policy(&super::violations(
        &scenario,
        history(&scenario, observation(29_500_000_123), 0),
    ));
}

#[test]
fn omitted_fetch_policy_requires_exact_omission() {
    let mut scenario = super::registration_scenario();
    let share = scenario.steps.remove(1);
    scenario.steps = vec![share];
    let exact = history(&scenario, super::share_observation(), 2);
    assert!(super::violations(&scenario, exact).is_empty());

    let mut unexpected = super::share_observation();
    unexpected.selected_builder.fetch = Some(configuration());
    assert_policy(&super::violations(
        &scenario,
        history(&scenario, unexpected, 2),
    ));
}

fn configured_scenario() -> Scenario {
    let mut scenario = super::registration_scenario();
    let mut share = scenario.steps.remove(1);
    let ScenarioAction::CreateShareConsumer {
        configuration: fetch_configuration,
        ..
    } = &mut share.action
    else {
        unreachable!("Share registration fixture");
    };
    *fetch_configuration = Some(configuration());
    scenario.steps = vec![share];
    scenario
}

fn history(
    scenario: &Scenario,
    observation: testlab_schema::ShareConsumerRegistrationObservation,
    event_sequence: u64,
) -> Vec<testlab_schema::HistoryEntry> {
    vec![
        super::command(1, "share", command(scenario)),
        super::event(
            event_sequence,
            "share",
            AdapterEvent::ShareConsumerCreated(observation),
        ),
    ]
}

fn command(scenario: &Scenario) -> AdapterCommand {
    let ScenarioAction::CreateShareConsumer {
        client_id,
        consumer_id,
        group_id,
        topics,
        rack,
        membership_timeout_ms,
        close_timeout_ms,
        configuration,
    } = &scenario.steps[0].action
    else {
        unreachable!("Share registration fixture");
    };
    AdapterCommand::CreateShareConsumer {
        client_id: client_id.clone(),
        consumer_id: consumer_id.clone(),
        group_id: group_id.clone(),
        topics: topics.clone(),
        rack: rack.clone(),
        membership_timeout_ms: *membership_timeout_ms,
        close_timeout_ms: *close_timeout_ms,
        configuration: *configuration,
    }
}

fn observation(
    membership_start_timeout_ns: u64,
) -> testlab_schema::ShareConsumerRegistrationObservation {
    let mut observation = super::share_observation();
    observation.selected_builder.fetch = Some(configuration());
    observation.selected_builder.membership_start_timeout_ns = membership_start_timeout_ns;
    observation
}

fn configuration() -> ShareConsumerFetchConfiguration {
    ShareConsumerFetchConfiguration {
        max_wait_ms: 250,
        min_bytes: 2,
        max_bytes: 524_288,
        max_records: 3,
        batch_size: 1,
        attempt_timeout_ms: 17_000,
    }
}

fn assert_policy(violations: &[testlab_schema::Violation]) {
    super::assert_contract(violations, "SHARE-016");
}
