//! Client-quota verification joins exact public results to immediate Kafka CLI state.

use testlab_schema::{ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_client_quota::Indexed;
use crate::support::violation;

pub(crate) fn verify_client_quota_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let window = index.admin_command_window(action);
    match action {
        ScenarioAction::AlterClientQuota(action) => verify_alter(action, index, window, violations),
        ScenarioAction::DescribeClientQuota(action) => {
            verify_describe(action, index, window, violations);
        }
        _ => return false,
    }
    true
}

fn verify_alter(
    action: &testlab_schema::AlterClientQuotaAction,
    index: &HistoryIndex,
    window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let (public, expected_rate, contract, expected) = if action.validate_only {
        (
            index
                .admin_client_quotas
                .validated
                .get(&action.operation_id),
            action.expected_current_bytes_per_second,
            "ADMIN-086",
            "exact public validation-only client-quota completion and immediate unchanged independent state",
        )
    } else {
        (
            index.admin_client_quotas.altered.get(&action.operation_id),
            action.bytes_per_second,
            "ADMIN-034",
            "exact public client-quota alteration and immediate independent resulting state",
        )
    };
    let independent = index.admin_client_quotas.observed.get(&action.operation_id);
    let completion = one(public);
    let public_matches = completion.is_some_and(|value| {
        value.value.operation_id == action.operation_id
            && value.value.user == action.user
            && value.value.direction == action.direction
            && public_after_command(window, value.history_sequence)
    });
    let independent_matches = completion.is_some_and(|value| {
        observation_matches(
            independent,
            &action.operation_id,
            &action.user,
            action.direction,
            expected_rate,
            window,
            value.history_sequence,
        )
    });
    report(
        public_matches && independent_matches,
        contract,
        &action.operation_id,
        expected,
        public,
        independent,
        violations,
    );
}

fn verify_describe(
    action: &testlab_schema::DescribeClientQuotaAction,
    index: &HistoryIndex,
    window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public = index
        .admin_client_quotas
        .described
        .get(&action.operation_id);
    let independent = index.admin_client_quotas.observed.get(&action.operation_id);
    let completion = one(public);
    let public_matches = completion.is_some_and(|value| {
        value.value.operation_id == action.operation_id
            && value.value.user == action.user
            && value.value.direction == action.direction
            && value.value.bytes_per_second == action.expected_bytes_per_second
            && public_after_command(window, value.history_sequence)
    });
    let independent_matches = completion.is_some_and(|value| {
        observation_matches(
            independent,
            &action.operation_id,
            &action.user,
            action.direction,
            Some(action.expected_bytes_per_second),
            window,
            value.history_sequence,
        )
    });
    report(
        public_matches && independent_matches,
        contract(action.strict),
        &action.operation_id,
        "exact public client-quota value and immediate independent matching state",
        public,
        independent,
        violations,
    );
}

pub(crate) const fn contract(strict: bool) -> &'static str {
    if strict { "ADMIN-033" } else { "ADMIN-090" }
}

#[allow(
    clippy::too_many_arguments,
    reason = "client-quota identity and temporal correlation remain explicit"
)]
fn observation_matches(
    actual: Option<&Vec<Indexed<testlab_schema::BrokerClientQuotaState>>>,
    operation_id: &testlab_schema::OperationId,
    user: &str,
    direction: testlab_schema::BrokerQuotaDirection,
    rate: Option<u64>,
    window: Option<AdminCommandWindow>,
    public_sequence: u64,
) -> bool {
    actual.is_some_and(|actual| {
        let [actual] = actual.as_slice() else {
            return false;
        };
        actual.value.operation_id == *operation_id
            && actual.value.user == user
            && actual.value.direction == direction
            && actual.value.bytes_per_second == rate
            && immediate_after_public(window, public_sequence, actual.history_sequence)
    })
}

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    values.filter(|values| values.len() == 1)?.first()
}

#[allow(
    clippy::too_many_arguments,
    reason = "client-quota evidence has public and independent streams"
)]
fn report<T>(
    valid: bool,
    contract: &str,
    operation_id: &testlab_schema::OperationId,
    expected: &str,
    public: Option<&Vec<Indexed<T>>>,
    independent: Option<&Vec<Indexed<testlab_schema::BrokerClientQuotaState>>>,
    violations: &mut Vec<Violation>,
) {
    if valid {
        return;
    }
    let evidence = public
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .chain(
            independent
                .into_iter()
                .flatten()
                .map(|value| format!("broker-state-observation:{}", value.value.observation)),
        )
        .collect();
    violations.push(violation(
        contract,
        format!("admin operation {operation_id} expected {expected}"),
        Some(operation_id.clone()),
        evidence,
    ));
}
