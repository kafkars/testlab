//! Validation evidence for real-Kafka record correlation headers.

use crate::{Scenario, ScenarioAction};

#[test]
fn kafka_records_require_exact_observer_correlation_headers() {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-round-trip.toml"
    ))
    .unwrap_or_else(|error| panic!("parse producer scenario: {error}"));
    let Some(ScenarioAction::Send { record, .. }) =
        scenario.steps.get_mut(3).map(|step| &mut step.action)
    else {
        panic!("producer send action missing");
    };
    record
        .headers
        .retain(|header| header.name != "testlab-sequence");

    let error = match scenario.validate() {
        Ok(()) => panic!("missing observer correlation must fail"),
        Err(error) => error,
    };
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("invalid observer correlation"))
    );
}
