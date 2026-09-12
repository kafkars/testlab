//! Configuration methods keep operands distinct from independently expected state.

use super::*;

#[test]
fn checked_in_legacy_restore_uses_both_exact_public_surfaces() {
    let restored: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-legacy-config-replacement.toml"
    ))
    .unwrap_or_else(|error| panic!("parse legacy restoration: {error}"));
    restored
        .validate()
        .unwrap_or_else(|error| panic!("validate legacy restoration: {error}"));
    for api in [
        crate::TopicConfigMutationApi::LegacyTopic,
        crate::TopicConfigMutationApi::LegacyResource,
    ] {
        assert!(restored.steps.iter().any(|step| matches!(
            &step.action,
            ScenarioAction::AlterTopicConfigs(action)
                if action.api == api && action.topics.iter().all(|topic| {
                    topic.method == crate::TopicConfigMutationMethod::RestoreDefault
                        && topic.command_value().is_none()
                })
        )));
    }
}

#[test]
fn validation_rejects_incompatible_methods_and_operand_shapes() {
    for (label, api, method, operation_value, expected) in [
        (
            "restore-incremental",
            crate::TopicConfigMutationApi::Topic,
            crate::TopicConfigMutationMethod::RestoreDefault,
            None,
            "configuration method is incompatible with its API",
        ),
        (
            "delete-legacy",
            crate::TopicConfigMutationApi::LegacyTopic,
            crate::TopicConfigMutationMethod::Delete,
            None,
            "configuration method is incompatible with its API",
        ),
        (
            "append-without-operand",
            crate::TopicConfigMutationApi::Topic,
            crate::TopicConfigMutationMethod::Append,
            None,
            "operation_value is required only for append or subtract",
        ),
        (
            "set-with-operand",
            crate::TopicConfigMutationApi::Topic,
            crate::TopicConfigMutationMethod::Set,
            Some("compact"),
            "operation_value is required only for append or subtract",
        ),
    ] {
        let mut candidate = action();
        candidate.operation_id = operation(label);
        candidate.api = api;
        candidate.topics[0].method = method;
        candidate.topics[0].operation_value = operation_value.map(str::to_owned);
        assert_problem(&problems(candidate), expected);
    }
}

fn problems(action: AlterTopicConfigsAction) -> Vec<String> {
    let mut problems = Vec::new();
    crate::admin_config_action_validation::validate(
        &ScenarioAction::AlterTopicConfigs(action),
        &BTreeMap::from([(client(), false)]),
        &mut BTreeSet::new(),
        &mut problems,
    );
    problems
}
