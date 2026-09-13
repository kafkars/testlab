//! Assigned-consumer configuration retains policy and capability ownership.

use crate::{
    AssignedConsumerReadIsolation, AssignedConsumerReceiveMethod, Capability,
    ConsumerFetchConfiguration, ConsumerLimitsConfiguration, Scenario, ScenarioAction,
};

#[test]
fn checked_in_read_committed_policy_is_explicit_and_valid() {
    let scenario = scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate assigned-consumer configuration: {error}"));
    let Some(ScenarioAction::CreateAssignedConsumerClient(action)) =
        scenario.steps.first().map(|step| &step.action)
    else {
        panic!("configured assigned-consumer client missing");
    };
    assert_eq!(
        action.configuration.read_isolation,
        AssignedConsumerReadIsolation::ReadCommitted
    );
    assert_eq!(
        action.configuration.fetch,
        Some(ConsumerFetchConfiguration {
            max_wait_ms: 250,
            min_bytes: 1,
            max_bytes: 524_288,
            partition_max_bytes: 262_144,
            attempt_timeout_ms: 20_000,
        })
    );
    assert_eq!(
        action.configuration.limits,
        Some(ConsumerLimitsConfiguration {
            in_flight_fetches: 2,
            buffered_batches: 3,
            buffered_bytes: 2_097_152,
            max_batch_bytes: 524_288,
        })
    );
}

#[test]
fn assigned_consumer_configuration_requires_its_capability() {
    let mut scenario = scenario();
    scenario
        .requires
        .remove(&Capability::AssignedConsumerConfiguration);
    let error =
        scenario.validation_error("assigned-consumer configuration capability must be required");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("assigned_consumer_configuration")),
        "{:?}",
        error.problems
    );
}

#[test]
fn assigned_consumer_fetch_and_limits_must_be_coherent() {
    let mut scenario = scenario();
    let Some(ScenarioAction::CreateAssignedConsumerClient(action)) =
        scenario.steps.first_mut().map(|step| &mut step.action)
    else {
        panic!("configured assigned-consumer client missing");
    };
    action.configuration.fetch = Some(ConsumerFetchConfiguration {
        max_wait_ms: 0,
        min_bytes: 10,
        max_bytes: 5,
        partition_max_bytes: 4_096,
        attempt_timeout_ms: 0,
    });
    action.configuration.limits = Some(ConsumerLimitsConfiguration {
        in_flight_fetches: 0,
        buffered_batches: 0,
        buffered_bytes: 512,
        max_batch_bytes: 2_048,
    });
    let message = scenario
        .validation_error("incoherent assigned-consumer policy must fail")
        .to_string();
    for problem in [
        "fetch.max_wait_ms",
        "fetch.min_bytes must not exceed fetch.max_bytes",
        "fetch.attempt_timeout_ms",
        "limits.in_flight_fetches",
        "limits.buffered_batches",
        "limits.max_batch_bytes must not exceed limits.buffered_bytes",
        "limits.max_batch_bytes must cover fetch.partition_max_bytes",
    ] {
        assert!(message.contains(problem), "{message}");
    }
}

#[test]
fn immediate_batch_receive_requires_its_exact_capability() {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-immediate-batch.toml"
    ))
    .unwrap_or_else(|error| panic!("parse immediate-batch scenario: {error}"));
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate immediate-batch scenario: {error}"));
    assert!(scenario.steps.iter().any(|step| matches!(
        &step.action,
        ScenarioAction::Receive {
            method: AssignedConsumerReceiveMethod::TryTakeBatch,
            ..
        }
    )));

    scenario
        .requires
        .remove(&Capability::AssignedConsumerImmediateBatch);
    let error = scenario.validation_error("immediate batch capability must be required");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("assigned_consumer_immediate_batch")),
        "{:?}",
        error.problems
    );
}

#[test]
fn ordinary_assigned_receive_defaults_to_waiting_recv() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-round-trip.toml"
    ))
    .unwrap_or_else(|error| panic!("parse assigned round trip: {error}"));
    assert!(scenario.steps.iter().any(|step| matches!(
        &step.action,
        ScenarioAction::Receive {
            method: AssignedConsumerReceiveMethod::Recv,
            ..
        }
    )));
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-read-committed.toml"
    ))
    .unwrap_or_else(|error| panic!("parse assigned-consumer configuration: {error}"))
}
