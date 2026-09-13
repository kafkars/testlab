//! Topic-description non-broker failures require an adjacent metadata fault.

use crate::{Scenario, ScenarioAction};

pub(super) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    for (index, step) in scenario.steps.iter().enumerate() {
        let ScenarioAction::DescribeTopic(action) = &step.action else {
            continue;
        };
        let Some(code) = action.expected_error_code.as_deref() else {
            continue;
        };
        if code == crate::UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE {
            continue;
        }
        let preceded_by_metadata_fault = index.checked_sub(1).is_some_and(|prior| {
            matches!(
                &scenario.steps[prior].action,
                ScenarioAction::ArmProtocolFault(control) if control.api == crate::KafkaApi::Metadata
            )
        });
        if !preceded_by_metadata_fault {
            problems.push(format!(
                "admin operation {} non-broker error {code} requires an immediately preceding metadata protocol fault",
                action.operation_id
            ));
        }
    }
}
