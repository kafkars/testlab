//! User SCRAM verification joins exact public results to immediate Kafka CLI state.

use testlab_schema::{ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_user_scram::Indexed;
use crate::support::violation;

const RESOURCE_NOT_FOUND: i16 = 91;

pub(crate) fn verify_user_scram_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let window = index.admin_command_window(action);
    match action {
        ScenarioAction::AlterUserScramCredential(action) => {
            verify_alter(action, index, window, violations);
        }
        ScenarioAction::DescribeUserScramCredential(action) => {
            verify_describe(action, index, window, violations);
        }
        _ => return false,
    }
    true
}

fn verify_alter(
    action: &testlab_schema::AlterUserScramCredentialAction,
    index: &HistoryIndex,
    window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public = index.admin_user_scram.altered.get(&action.operation_id);
    let independent = index.admin_user_scram.observed.get(&action.operation_id);
    let completion = one(public);
    let public_matches = completion.is_some_and(|value| {
        value.value.operation_id == action.operation_id
            && value.value.user == action.user
            && value.value.mechanism == action.mechanism
            && public_after_command(window, value.history_sequence)
    });
    let independent_matches = completion.is_some_and(|value| {
        observation_matches(
            independent,
            action,
            action.iterations,
            window,
            value.history_sequence,
        )
    });
    report(
        public_matches && independent_matches,
        "ADMIN-036",
        &action.operation_id,
        "exact public user SCRAM alteration and immediate independent resulting state",
        public,
        independent,
        violations,
    );
}

fn verify_describe(
    action: &testlab_schema::DescribeUserScramCredentialAction,
    index: &HistoryIndex,
    window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public = index.admin_user_scram.described.get(&action.operation_id);
    let independent = index.admin_user_scram.observed.get(&action.operation_id);
    let completion = one(public);
    let absence_code = action
        .expected_iterations
        .is_none()
        .then_some(RESOURCE_NOT_FOUND);
    let public_matches = completion.is_some_and(|value| {
        value.value.operation_id == action.operation_id
            && value.value.user == action.user
            && value.value.mechanism == action.mechanism
            && value.value.iterations == action.expected_iterations
            && value.value.absence_broker_code == absence_code
            && public_after_command(window, value.history_sequence)
    });
    let independent_matches = completion.is_some_and(|value| {
        observation_matches(
            independent,
            action,
            action.expected_iterations,
            window,
            value.history_sequence,
        )
    });
    report(
        public_matches && independent_matches,
        "ADMIN-035",
        &action.operation_id,
        "exact public user SCRAM metadata and immediate independent matching state",
        public,
        independent,
        violations,
    );
}

fn observation_matches<A: UserScramAction>(
    actual: Option<&Vec<Indexed<testlab_schema::BrokerUserScramCredentialState>>>,
    action: &A,
    iterations: Option<u32>,
    window: Option<AdminCommandWindow>,
    public_sequence: u64,
) -> bool {
    actual.is_some_and(|actual| {
        let [actual] = actual.as_slice() else {
            return false;
        };
        actual.value.operation_id == *action.operation_id()
            && actual.value.user == action.user()
            && actual.value.mechanism == action.mechanism()
            && actual.value.iterations == iterations
            && immediate_after_public(window, public_sequence, actual.history_sequence)
    })
}

trait UserScramAction {
    fn operation_id(&self) -> &testlab_schema::OperationId;
    fn user(&self) -> &str;
    fn mechanism(&self) -> testlab_schema::ScramCredentialMechanism;
}

impl UserScramAction for testlab_schema::AlterUserScramCredentialAction {
    fn operation_id(&self) -> &testlab_schema::OperationId {
        &self.operation_id
    }

    fn user(&self) -> &str {
        &self.user
    }

    fn mechanism(&self) -> testlab_schema::ScramCredentialMechanism {
        self.mechanism
    }
}

impl UserScramAction for testlab_schema::DescribeUserScramCredentialAction {
    fn operation_id(&self) -> &testlab_schema::OperationId {
        &self.operation_id
    }

    fn user(&self) -> &str {
        &self.user
    }

    fn mechanism(&self) -> testlab_schema::ScramCredentialMechanism {
        self.mechanism
    }
}

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    values.filter(|values| values.len() == 1)?.first()
}

#[allow(
    clippy::too_many_arguments,
    reason = "user SCRAM evidence has public and independent streams"
)]
fn report<T>(
    valid: bool,
    contract: &str,
    operation_id: &testlab_schema::OperationId,
    expected: &str,
    public: Option<&Vec<Indexed<T>>>,
    independent: Option<&Vec<Indexed<testlab_schema::BrokerUserScramCredentialState>>>,
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
