//! ACL policy verification requires exact public denial followed by restored progress.

use testlab_schema::{
    AdapterCommand, BrokerAclOperation, BrokerAclResource, BrokerPolicy, Scenario, ScenarioAction,
    Violation,
};

use crate::broker_policy::{PolicyWindow, active};
use crate::broker_policy_control::references;
use crate::index::HistoryIndex;
use crate::support::violation;

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
    let (command_sequence, command_id) = exact_command(action, index)?;
    let mut failures = index
        .command_failures
        .iter()
        .filter(|failure| failure.command_id == *command_id);
    let failure = failures.next()?;
    (failures.next().is_none()
        && failure.code == expected
        && active(window, command_sequence)
        && active(window, failure.history_sequence))
    .then_some(failure.history_sequence)
}

pub(crate) fn exact_command<'a>(
    action: &ScenarioAction,
    index: &'a HistoryIndex,
) -> Option<(u64, &'a testlab_schema::CommandId)> {
    let commands = index
        .commands
        .iter()
        .filter(|(_, _, command)| command_matches(action, command))
        .collect::<Vec<_>>();
    let [(sequence, command_id, _)] = commands.as_slice() else {
        return None;
    };
    Some((*sequence, command_id))
}

fn command_matches(action: &ScenarioAction, command: &AdapterCommand) -> bool {
    match (action, command) {
        (
            ScenarioAction::Send {
                producer_id,
                operation_id,
                record,
            },
            AdapterCommand::Send {
                producer_id: actual_producer,
                operation_id: actual_operation,
                record: actual_record,
            },
        ) => {
            producer_id == actual_producer
                && operation_id == actual_operation
                && record == actual_record
        }
        (
            ScenarioAction::GroupReceive {
                consumer_id,
                receive_id,
                timeout_ms,
                ..
            },
            AdapterCommand::GroupReceive {
                consumer_id: actual_consumer,
                receive_id: actual_receive,
                timeout_ms: actual_timeout,
            },
        ) => {
            consumer_id == actual_consumer
                && receive_id == actual_receive
                && timeout_ms == actual_timeout
        }
        (ScenarioAction::CreateTopic(action), AdapterCommand::CreateTopic(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.topic == command.topic
                && action.partitions == command.partitions
                && action.replication_factor == command.replication_factor
                && action.validate_only == command.validate_only
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::CreateTransactionalProducer {
                client_id,
                producer_id,
                transactional_id,
                transaction_timeout_ms,
                initialization_timeout_ms,
                ..
            },
            AdapterCommand::CreateTransactionalProducer {
                client_id: actual_client,
                producer_id: actual_producer,
                transactional_id: actual_transactional,
                transaction_timeout_ms: actual_transaction_timeout,
                initialization_timeout_ms: actual_initialization_timeout,
            },
        ) => {
            client_id == actual_client
                && producer_id == actual_producer
                && transactional_id == actual_transactional
                && transaction_timeout_ms == actual_transaction_timeout
                && initialization_timeout_ms == actual_initialization_timeout
        }
        _ => false,
    }
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
