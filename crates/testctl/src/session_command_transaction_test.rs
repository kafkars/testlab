//! Transaction command translation strips harness-only source expectations.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

#[test]
fn transactional_transform_translates_exact_output_identity_set() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/transactional-offset-classic.toml"
    ))
    .unwrap_or_else(|error| panic!("parse transactional offset scenario: {error}"));
    let action = &scenario.steps[6].action;
    let ScenarioAction::ExecuteTransactionalTransform(expected) = action else {
        panic!("commit transform missing");
    };

    let Some((AdapterCommand::ExecuteTransactionalTransform(command), event)) =
        crate::session_command::translate(action)
    else {
        panic!("transform translation missing");
    };
    assert_eq!(command.transaction_id, expected.transaction_id);
    assert_eq!(command.consumer_id, expected.consumer_id);
    assert_eq!(command.operations, expected.operations);
    assert!(matches!(event, ExpectedEvent::TransactionCompleted {
        transaction_id,
        operation_ids,
    } if transaction_id == expected.transaction_id
        && operation_ids.contains(&expected.operations[0].operation_id)));
}
