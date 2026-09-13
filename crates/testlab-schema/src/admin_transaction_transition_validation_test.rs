//! Transaction Admin transition tests reject unstable or invented identities.

use crate::{Scenario, ScenarioAction};

#[test]
fn initialized_closed_transaction_discovery_is_valid() {
    assert!(problems(fixture()).is_empty());
    assert!(problems(fence_fixture()).is_empty());
}

#[test]
fn fencing_rejects_an_open_transactional_owner() {
    let mut scenario = fence_fixture();
    scenario.steps.remove(3);
    let problems = problems(scenario);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("every producer") && problem.contains("closed")),
        "{problems:?}"
    );
}

#[test]
fn discovery_rejects_an_open_transactional_owner() {
    let mut scenario = fixture();
    scenario.steps.remove(3);
    let problems = problems(scenario);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("every producer") && problem.contains("closed")),
        "{problems:?}"
    );
}

#[test]
fn listing_rejects_an_identity_not_initialized_by_the_fixture() {
    let mut scenario = fixture();
    let action = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::ListTransactions(action) => Some(action),
            _ => None,
        })
        .unwrap_or_else(|| panic!("transaction listing action"));
    action.expected_transactions[0].transactional_id = "invented-transaction".to_owned();
    let problems = problems(scenario);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("must list every")),
        "{problems:?}"
    );
}

#[test]
fn filtered_listing_requires_an_earlier_unfiltered_baseline() {
    let mut scenario = fixture();
    let action = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::ListTransactions(action) if !action.state_filters.is_empty() => {
                Some(action)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("filtered transaction listing action"));
    action.baseline_operation_id = Some(
        crate::OperationId::new("missing-baseline")
            .unwrap_or_else(|error| panic!("operation id: {error}")),
    );
    let problems = problems(scenario);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("earlier unfiltered")),
        "{problems:?}"
    );
}

fn fixture() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-transaction-discovery.toml"
    ))
    .unwrap_or_else(|error| panic!("transaction discovery fixture: {error}"))
}

fn fence_fixture() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-fence-producers.toml"
    ))
    .unwrap_or_else(|error| panic!("producer fencing fixture: {error}"))
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the test helper owns each constructed scenario fixture"
)]
fn problems(scenario: Scenario) -> Vec<String> {
    let mut problems = Vec::new();
    super::validate(&scenario, &mut problems);
    problems
}
