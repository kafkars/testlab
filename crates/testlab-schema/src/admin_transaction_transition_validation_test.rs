//! Transaction discovery transition tests reject unstable or invented identities.

use crate::{Scenario, ScenarioAction};

#[test]
fn initialized_closed_transaction_discovery_is_valid() {
    assert!(problems(fixture()).is_empty());
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

fn fixture() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-transaction-discovery.toml"
    ))
    .unwrap_or_else(|error| panic!("transaction discovery fixture: {error}"))
}

fn problems(scenario: Scenario) -> Vec<String> {
    let mut problems = Vec::new();
    super::validate(&scenario, &mut problems);
    problems
}
