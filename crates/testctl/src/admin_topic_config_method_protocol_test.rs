//! Incremental configuration commands carry methods and operands, not final state.

use super::*;

#[test]
fn append_and_subtract_send_only_the_exact_operation_operand() {
    for (method, operand, expected_value) in [
        (
            testlab_schema::TopicConfigMutationMethod::Append,
            "compact",
            "delete,compact",
        ),
        (
            testlab_schema::TopicConfigMutationMethod::Subtract,
            "delete",
            "compact",
        ),
    ] {
        let mut action = mutation_action();
        for selected in &mut action.topics {
            selected.method = method;
            selected.operation_value = Some(operand.to_owned());
            selected.value = expected_value.to_owned();
        }
        let command = translated(action);
        assert!(command.topics.iter().all(
            |selected| selected.method == method && selected.value.as_deref() == Some(operand)
        ));
        let encoded = serde_json::to_string(&command)
            .unwrap_or_else(|error| panic!("encode incremental method: {error}"));
        assert!(!encoded.contains(expected_value), "{encoded}");
    }
}

#[test]
fn delete_sends_its_method_without_the_expected_default() {
    let mut action = mutation_action();
    for selected in &mut action.topics {
        selected.method = testlab_schema::TopicConfigMutationMethod::Delete;
        selected.value = "delete".to_owned();
    }
    let command = translated(action);
    assert!(command.topics.iter().all(|selected| {
        selected.method == testlab_schema::TopicConfigMutationMethod::Delete
            && selected.value.is_none()
    }));
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode incremental delete: {error}"));
    assert!(!encoded.contains("\"value\""), "{encoded}");
}

fn translated(
    action: testlab_schema::AlterTopicConfigsAction,
) -> testlab_schema::AlterTopicConfigsCommand {
    let Some((AdapterCommand::AlterTopicConfigs(command), _)) =
        crate::session_command_admin_config::translate(&ScenarioAction::AlterTopicConfigs(action))
    else {
        panic!("incremental method translation");
    };
    command
}
