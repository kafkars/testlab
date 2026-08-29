//! Share validation tests pin ordered batch identities and disposition cardinality.

use crate::{Scenario, ScenarioAction, ShareDisposition};

fn mixed_release() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/share-group-mixed-release.toml"
    ))
    .unwrap_or_else(|error| panic!("parse mixed release scenario: {error}"))
}

#[test]
fn checked_in_mixed_share_batch_is_valid() {
    mixed_release()
        .validate()
        .unwrap_or_else(|error| panic!("validate mixed release scenario: {error}"));
}

#[test]
fn share_batch_requires_one_disposition_per_expected_record() {
    let mut scenario = mixed_release();
    let Some(ScenarioAction::ShareAcknowledge { dispositions, .. }) =
        scenario.steps.get_mut(6).map(|step| &mut step.action)
    else {
        panic!("mixed acknowledgement missing");
    };
    dispositions.pop();

    let error = match scenario.validate() {
        Ok(()) => panic!("short disposition list must fail"),
        Err(error) => error,
    };
    assert!(
        error.problems.iter().any(|problem| {
            problem.contains("must provide one disposition per expected record")
        })
    );
}

#[test]
fn share_batch_rejects_repeated_expected_operations() {
    let mut scenario = mixed_release();
    let Some(ScenarioAction::ShareReceive {
        expected_operation_ids,
        ..
    }) = scenario.steps.get_mut(5).map(|step| &mut step.action)
    else {
        panic!("mixed receive missing");
    };
    expected_operation_ids[1] = expected_operation_ids[0].clone();

    let error = match scenario.validate() {
        Ok(()) => panic!("repeated expected operation must fail"),
        Err(error) => error,
    };
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("repeats expected send"))
    );
}

#[test]
fn share_dispositions_remain_record_ordered_values() {
    assert_ne!(ShareDisposition::Accept, ShareDisposition::Release);
    assert_ne!(ShareDisposition::Release, ShareDisposition::Reject);
}
