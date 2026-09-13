//! Processing acknowledgement validation binds delay windows to group configuration.

use std::collections::BTreeMap;

use crate::{ConsumerId, Scenario, ScenarioAction};

pub(super) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let processing_timeouts = scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::CreateGroupConsumer {
                consumer_id,
                configuration,
                ..
            } => Some((
                consumer_id,
                configuration
                    .as_ref()
                    .and_then(|value| value.processing_timeout_ms),
            )),
            _ => None,
        })
        .collect::<BTreeMap<&ConsumerId, Option<u64>>>();
    for step in &scenario.steps {
        let ScenarioAction::GroupReceive {
            consumer_id,
            receive_id,
            processing_acknowledgement_delay_ms: delay,
            timeout_ms,
            ..
        } = &step.action
        else {
            continue;
        };
        if *delay == 0 {
            continue;
        }
        let Some(Some(processing_timeout)) = processing_timeouts.get(consumer_id) else {
            problems.push(format!(
                "group receive {receive_id} processing acknowledgement requires an explicit processing_timeout_ms"
            ));
            continue;
        };
        if delay >= processing_timeout {
            problems.push(format!(
                "group receive {receive_id} acknowledgement delay must be shorter than processing_timeout_ms"
            ));
        }
        let total = delay.checked_mul(2);
        if total.is_none_or(|value| value <= *processing_timeout) {
            problems.push(format!(
                "group receive {receive_id} acknowledgement window must exceed processing_timeout_ms"
            ));
        }
        if total.is_none_or(|value| value >= *timeout_ms) {
            problems.push(format!(
                "group receive {receive_id} acknowledgement window must fit inside timeout_ms"
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Capability, Scenario, ScenarioAction};

    fn scenario() -> Scenario {
        toml::from_str(include_str!(
            "../../../scenarios/kafka/classic-group-processing-acknowledgement.toml"
        ))
        .unwrap_or_else(|error| panic!("parse acknowledgement scenario: {error}"))
    }

    #[test]
    fn checked_in_acknowledgement_scenario_is_valid_and_requires_capability() {
        let mut scenario = scenario();
        scenario
            .validate()
            .unwrap_or_else(|error| panic!("validate acknowledgement scenario: {error}"));
        assert!(
            scenario
                .requires
                .remove(&Capability::GroupConsumerAcknowledge)
        );
        assert!(
            scenario
                .validation_error("acknowledgement capability must be explicit")
                .to_string()
                .contains("group_consumer_acknowledge")
        );
    }

    #[test]
    fn acknowledgement_window_must_straddle_processing_timeout() {
        let mut scenario = scenario();
        let delay = scenario
            .steps
            .iter_mut()
            .find_map(|step| match &mut step.action {
                ScenarioAction::GroupReceive {
                    processing_acknowledgement_delay_ms,
                    ..
                } => Some(processing_acknowledgement_delay_ms),
                _ => None,
            })
            .unwrap_or_else(|| panic!("group receive missing"));
        *delay = 2_000;
        assert!(
            scenario
                .validation_error("short acknowledgement window must fail")
                .to_string()
                .contains("must exceed processing_timeout_ms")
        );
    }
}
