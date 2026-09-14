//! Validation-only feature updates preserve state and caller-ordered outcomes.

use testlab_schema::{ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let window = index.admin_command_window(action);
    let ScenarioAction::ValidateFeatureUpdates(action) = action else {
        return false;
    };
    let public = one(index.admin_features.validations.get(&action.operation_id));
    let baseline = one(index
        .admin_features
        .observed
        .get(&action.baseline_operation_id));
    let after = one(index.admin_features.observed.get(&action.operation_id));
    let matches = public.is_some_and(|public| {
        baseline.is_some_and(|baseline| {
            after.is_some_and(|after| {
                let outcomes_match = public.value.outcomes.len() == action.updates.len()
                    && public.value.outcomes.iter().zip(&action.updates).all(
                        |(outcome, update)| {
                            outcome.name == update.name && outcome.error_code.is_none()
                        },
                    );
                outcomes_match
                    && public.value.throttle_time_ms <= action.timeout_ms
                    && window.is_some_and(|(command, _)| baseline.history_sequence < command)
                    && public_after_command(window, public.history_sequence)
                    && immediate_after_public(
                        window,
                        public.history_sequence,
                        after.history_sequence,
                    )
                    && baseline.value.features == after.value.features
                    && epoch_did_not_go_backwards(
                        baseline.value.finalized_features_epoch,
                        after.value.finalized_features_epoch,
                    )
            })
        })
    });
    if matches {
        return true;
    }
    violations.push(violation(
        "ADMIN-072",
        format!(
            "admin operation {} expected caller-ordered successful validation-only feature outcomes and unchanged immediate Kafka CLI state",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(baseline.map(|value| {
                format!("broker-state-observation:{}", value.value.observation)
            }))
            .chain(after.map(|value| {
                format!("broker-state-observation:{}", value.value.observation)
            }))
            .collect(),
    ));
    true
}

fn epoch_did_not_go_backwards(baseline: Option<i64>, after: Option<i64>) -> bool {
    match (baseline, after) {
        (Some(baseline), Some(after)) => baseline >= 0 && after >= baseline,
        (None, None) => true,
        _ => false,
    }
}

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
