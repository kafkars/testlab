//! Broker ACL policy verification pins the required denial window.

use crate::broker_policy::{PolicyWindow, active};
use crate::broker_policy_control::references;
use crate::index::HistoryIndex;
use crate::support::violation;
use testlab_schema::{
    BrokerAclOperation, BrokerAclResource, BrokerPolicy, Scenario, ScenarioAction, Violation,
};
#[path = "broker_policy_acl_command.rs"]
mod command_match;
pub(crate) fn verify(
    scenario: &Scenario,
    policy: &BrokerPolicy,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
    observations: &[testlab_schema::BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let BrokerPolicy::Acl {
        resource,
        operation,
    } = policy
    else {
        return;
    };
    let denial = denial(scenario, resource, *operation, window, index);
    if denial.is_none() {
        violations.push(violation(
            "POLICY-002",
            format!("active deny ACL {resource:?} {operation:?} lacked one exact public denial"),
            None,
            references(&window.present),
        ));
    }
    let recovery = crate::broker_policy_recovery::verify(
        scenario,
        resource,
        *operation,
        window,
        index,
        observations,
    );
    if recovery.is_none() {
        violations.push(violation(
            "POLICY-003",
            format!("removed deny ACL {resource:?} {operation:?} lacked restored public progress"),
            None,
            references(&window.absent),
        ));
    }
}
fn denial(
    scenario: &Scenario,
    resource: &BrokerAclResource,
    operation: BrokerAclOperation,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
) -> Option<u64> {
    match (resource, operation) {
        (BrokerAclResource::Topic { name }, BrokerAclOperation::Read) => {
            crate::broker_policy_assigned_consumer::denial(scenario, name, window, index)
        }
        (BrokerAclResource::Topic { name }, BrokerAclOperation::Write) => scenario
            .steps
            .iter()
            .find_map(|step| producer_denial(&step.action, name, window, scenario, index)),
        (BrokerAclResource::Topic { name }, BrokerAclOperation::Create) => scenario
            .steps
            .iter()
            .find_map(|step| command_denial(&step.action, name, window, index)),
        (BrokerAclResource::Group { name }, BrokerAclOperation::Read) => scenario
            .steps
            .iter()
            .find_map(|step| group_denial(&step.action, name, window, scenario, index)),
        (BrokerAclResource::TransactionalId { name }, BrokerAclOperation::Write) => scenario
            .steps
            .iter()
            .find_map(|step| command_denial(&step.action, name, window, index)),
        _ => None,
    }
}
fn producer_denial(
    action: &ScenarioAction,
    topic: &str,
    window: &PolicyWindow<'_>,
    scenario: &Scenario,
    index: &HistoryIndex,
) -> Option<u64> {
    let ScenarioAction::Send {
        operation_id,
        record,
        ..
    } = action
    else {
        return None;
    };
    let assertion = scenario.assertions.iter().find(|value| {
        value.operation_id == *operation_id
            && value.expected_error_code.as_deref()
                == Some(testlab_schema::PRODUCER_TOPIC_AUTHORIZATION_ERROR_CODE)
    })?;
    if record.topic != topic
        || assertion.visibility != testlab_schema::VisibilityExpectation::Absent
    {
        return None;
    }
    let (command, _) = exact_command(action, index)?;
    let error = index.operation_errors.get(operation_id)?.as_slice();
    let [error] = error else { return None };
    (active(window, command)
        && active(window, error.history_sequence)
        && error.code == testlab_schema::PRODUCER_TOPIC_AUTHORIZATION_ERROR_CODE)
        .then_some(error.history_sequence)
}
fn group_denial(
    action: &ScenarioAction,
    group_id: &str,
    window: &PolicyWindow<'_>,
    scenario: &Scenario,
    index: &HistoryIndex,
) -> Option<u64> {
    let ScenarioAction::GroupReceive {
        consumer_id,
        expected_error_code: Some(code),
        ..
    } = action
    else {
        return None;
    };
    if code != testlab_schema::GROUP_AUTHORIZATION_ERROR_CODE
        || consumer_group(scenario, consumer_id)? != group_id
    {
        return None;
    }
    command_failure(action, code, window, index)
}
fn command_denial(
    action: &ScenarioAction,
    target: &str,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
) -> Option<u64> {
    let expected = match action {
        ScenarioAction::CreateTopic(action)
            if action.topic == target
                && action.expected_error_code.as_deref()
                    == Some(testlab_schema::ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE) =>
        {
            testlab_schema::ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE
        }
        ScenarioAction::CreateTransactionalProducer {
            transactional_id,
            expected_error_code: Some(code),
            ..
        } if transactional_id == target
            && code == testlab_schema::TRANSACTIONAL_ID_AUTHORIZATION_ERROR_CODE =>
        {
            testlab_schema::TRANSACTIONAL_ID_AUTHORIZATION_ERROR_CODE
        }
        _ => return None,
    };
    command_failure(action, expected, window, index)
}
fn command_failure(
    action: &ScenarioAction,
    expected: &str,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
) -> Option<u64> {
    let commands = matching_commands(action, index)
        .into_iter()
        .filter(|(sequence, _)| active(window, *sequence))
        .collect::<Vec<_>>();
    let [(command_sequence, command_id)] = commands.as_slice() else {
        return None;
    };
    let mut failures = index
        .command_failures
        .iter()
        .filter(|failure| failure.command_id == **command_id);
    let failure = failures.next()?;
    (failures.next().is_none()
        && failure.code == expected
        && active(window, *command_sequence)
        && active(window, failure.history_sequence))
    .then_some(failure.history_sequence)
}

pub(crate) fn exact_command<'a>(
    action: &ScenarioAction,
    index: &'a HistoryIndex,
) -> Option<(u64, &'a testlab_schema::CommandId)> {
    let commands = matching_commands(action, index);
    let [(sequence, command_id)] = commands.as_slice() else {
        return None;
    };
    Some((*sequence, command_id))
}

pub(crate) fn matching_commands<'a>(
    action: &ScenarioAction,
    index: &'a HistoryIndex,
) -> Vec<(u64, &'a testlab_schema::CommandId)> {
    index
        .commands
        .iter()
        .filter(|(_, _, command)| command_match::matches(action, command))
        .map(|(sequence, command_id, _)| (*sequence, command_id))
        .collect()
}

pub(crate) fn consumer_group<'a>(
    scenario: &'a Scenario,
    consumer_id: &testlab_schema::ConsumerId,
) -> Option<&'a str> {
    scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::CreateGroupConsumer {
            consumer_id: actual,
            group_id,
            ..
        } if actual == consumer_id => Some(group_id.as_str()),
        _ => None,
    })
}
