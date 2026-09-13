//! Partial checkpoint validation binds a processed prefix to prior exact sends.

use std::collections::{BTreeMap, BTreeSet};

use crate::{GroupCheckpointMethod, OperationId, RecordSpec, Scenario, ScenarioAction};

pub(super) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut sends = BTreeMap::new();
    for step in &scenario.steps {
        if let ScenarioAction::GroupReceive {
            receive_id,
            expected_operation_id,
            additional_expected_operation_ids,
            checkpoint_method,
            processed_record_count,
            processing_acknowledgement_delay_ms,
            expected_error_code,
            ..
        } = &step.action
        {
            validate_receive(
                receive_id,
                expected_operation_id,
                additional_expected_operation_ids,
                *checkpoint_method,
                *processed_record_count,
                *processing_acknowledgement_delay_ms,
                expected_error_code.as_deref(),
                &sends,
                problems,
            );
        }
        for (operation_id, record) in sent_operations(&step.action) {
            sends.insert(operation_id, record);
        }
    }
}

#[allow(clippy::too_many_arguments, reason = "one partial receive contract")]
fn validate_receive(
    receive_id: &OperationId,
    first: &OperationId,
    additional: &[OperationId],
    checkpoint_method: GroupCheckpointMethod,
    processed_count: Option<usize>,
    acknowledgement_delay_ms: u64,
    expected_error_code: Option<&str>,
    sends: &BTreeMap<&OperationId, &RecordSpec>,
    problems: &mut Vec<String>,
) {
    if additional.is_empty() && processed_count.is_none() {
        return;
    }
    let expected = std::iter::once(first).chain(additional).collect::<Vec<_>>();
    if expected.len() < 2 {
        problems.push(format!(
            "partial group receive {receive_id} requires at least two expected records"
        ));
    }
    if expected.iter().copied().collect::<BTreeSet<_>>().len() != expected.len() {
        problems.push(format!(
            "partial group receive {receive_id} expected records must be distinct"
        ));
    }
    let records = expected
        .iter()
        .filter_map(|operation_id| {
            if let Some(record) = sends.get(operation_id) {
                return Some(*record);
            }
            problems.push(format!(
                "partial group receive {receive_id} expects missing prior send {operation_id}"
            ));
            None
        })
        .collect::<Vec<_>>();
    if records.len() == expected.len()
        && records.iter().any(|record| {
            record.topic != records[0].topic || record.partition != records[0].partition
        })
    {
        problems.push(format!(
            "partial group receive {receive_id} expected records must share one topic partition"
        ));
    }
    if records
        .windows(2)
        .any(|pair| pair[0].sequence >= pair[1].sequence)
    {
        problems.push(format!(
            "partial group receive {receive_id} expected records must be in sequence order"
        ));
    }
    if processed_count.is_none_or(|count| count == 0 || count >= expected.len()) {
        problems.push(format!(
            "partial group receive {receive_id} processed_record_count must select a nonempty proper prefix"
        ));
    }
    if processed_count.is_some() && checkpoint_method != GroupCheckpointMethod::Checkpoint {
        problems.push(format!(
            "partial group receive {receive_id} requires checkpoint_builder, not into_checkpoint"
        ));
    }
    if acknowledgement_delay_ms != 0 {
        problems.push(format!(
            "partial group receive {receive_id} cannot also select processing acknowledgement"
        ));
    }
    if expected_error_code.is_some() {
        problems.push(format!(
            "partial group receive {receive_id} cannot expect a receive failure"
        ));
    }
}

fn sent_operations(action: &ScenarioAction) -> Vec<(&OperationId, &RecordSpec)> {
    match action {
        ScenarioAction::Send {
            operation_id,
            record,
            ..
        } => vec![(operation_id, record)],
        ScenarioAction::SendBatch { operations, .. }
        | ScenarioAction::ExecuteTransaction { operations, .. } => operations
            .iter()
            .map(|operation| (&operation.operation_id, &operation.record))
            .collect(),
        ScenarioAction::CancelProducerSend(action) => {
            vec![(&action.operation_id, &action.record)]
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use crate::{Capability, GroupCheckpointMethod, Scenario, ScenarioAction};

    fn scenario() -> Scenario {
        toml::from_str(include_str!(
            "../../../scenarios/kafka/classic-group-partial-checkpoint.toml"
        ))
        .unwrap_or_else(|error| panic!("parse partial checkpoint scenario: {error}"))
    }

    #[test]
    fn checked_in_partial_checkpoint_is_valid_and_capability_gated() {
        let mut scenario = scenario();
        scenario
            .validate()
            .unwrap_or_else(|error| panic!("validate partial checkpoint scenario: {error}"));
        assert!(
            scenario
                .requires
                .remove(&Capability::GroupConsumerPartialCheckpoint)
        );
        assert!(
            scenario
                .validate()
                .expect_err("partial checkpoint capability must be explicit")
                .to_string()
                .contains("group_consumer_partial_checkpoint")
        );
    }

    #[test]
    fn processed_count_must_leave_an_uncommitted_suffix() {
        let mut scenario = scenario();
        let count = scenario
            .steps
            .iter_mut()
            .find_map(|step| match &mut step.action {
                ScenarioAction::GroupReceive {
                    processed_record_count,
                    ..
                } => processed_record_count.as_mut(),
                _ => None,
            })
            .unwrap_or_else(|| panic!("partial group receive missing"));
        *count = 2;
        assert!(
            scenario
                .validate()
                .expect_err("full processed count must fail")
                .to_string()
                .contains("nonempty proper prefix")
        );
    }

    #[test]
    fn partial_checkpoint_rejects_full_batch_conversion() {
        let mut scenario = scenario();
        let method = scenario
            .steps
            .iter_mut()
            .find_map(|step| match &mut step.action {
                ScenarioAction::GroupReceive {
                    checkpoint_method,
                    processed_record_count: Some(_),
                    ..
                } => Some(checkpoint_method),
                _ => None,
            })
            .unwrap_or_else(|| panic!("partial group receive missing"));
        *method = GroupCheckpointMethod::IntoCheckpoint;
        assert!(
            scenario
                .validate()
                .expect_err("full conversion must fail for a partial checkpoint")
                .to_string()
                .contains("requires checkpoint_builder")
        );
    }
}
