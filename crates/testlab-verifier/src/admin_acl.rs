//! ACL verification joins exact public outcomes to immediate independent CLI observations.

use testlab_schema::{
    AdminAclCreationOutcome, AdminAclDeleteMatch, AdminAclDeletionOutcome, ScenarioAction,
    Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_acl::Indexed;
use crate::support::violation;

pub(crate) fn verify_acl_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let window = index.admin_command_window(action);
    match action {
        ScenarioAction::CreateAcls(action) => verify_create(action, index, window, violations),
        ScenarioAction::DescribeAcls(action) => verify_describe(action, index, window, violations),
        ScenarioAction::DeleteAcls(action) => verify_delete(action, index, window, violations),
        _ => return false,
    }
    true
}

fn verify_create(
    action: &testlab_schema::CreateAclsAction,
    index: &HistoryIndex,
    window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public = index.admin_acls.created.get(&action.operation_id);
    let independent = index.admin_acls.observed.get(&action.operation_id);
    let completion = one(public);
    let expected = action
        .bindings
        .iter()
        .cloned()
        .map(|binding| AdminAclCreationOutcome {
            binding,
            error_code: None,
        })
        .collect::<Vec<_>>();
    let public_matches = completion.is_some_and(|value| {
        value.value.operation_id == action.operation_id
            && value.value.outcomes == expected
            && public_after_command(window, value.history_sequence)
    });
    let independent_matches = completion.is_some_and(|value| {
        observations_match(
            &action.bindings,
            independent,
            &action.operation_id,
            true,
            window,
            value.history_sequence,
        )
    });
    report(
        public_matches && independent_matches,
        "ADMIN-030",
        &action.operation_id,
        "caller-ordered successful ACL creation outcomes and immediate independent presence",
        public,
        independent,
        violations,
    );
}

fn verify_describe(
    action: &testlab_schema::DescribeAclsAction,
    index: &HistoryIndex,
    window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public = index.admin_acls.described.get(&action.operation_id);
    let independent = index.admin_acls.observed.get(&action.operation_id);
    let completion = one(public);
    let public_matches = completion.is_some_and(|value| {
        value.value.operation_id == action.operation_id
            && value.value.bindings.as_slice() == std::slice::from_ref(&action.binding)
            && public_after_command(window, value.history_sequence)
    });
    let independent_matches = completion.is_some_and(|value| {
        observations_match(
            std::slice::from_ref(&action.binding),
            independent,
            &action.operation_id,
            true,
            window,
            value.history_sequence,
        )
    });
    report(
        public_matches && independent_matches,
        "ADMIN-031",
        &action.operation_id,
        "one exact public ACL description and immediate independent presence",
        public,
        independent,
        violations,
    );
}

fn verify_delete(
    action: &testlab_schema::DeleteAclsAction,
    index: &HistoryIndex,
    window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public = index.admin_acls.deleted.get(&action.operation_id);
    let independent = index.admin_acls.observed.get(&action.operation_id);
    let completion = one(public);
    let expected = action
        .bindings
        .iter()
        .cloned()
        .map(|binding| AdminAclDeletionOutcome {
            filter: binding.clone(),
            error_code: None,
            matches: vec![AdminAclDeleteMatch {
                binding,
                error_code: None,
            }],
        })
        .collect::<Vec<_>>();
    let public_matches = completion.is_some_and(|value| {
        value.value.operation_id == action.operation_id
            && value.value.outcomes == expected
            && public_after_command(window, value.history_sequence)
    });
    let independent_matches = completion.is_some_and(|value| {
        observations_match(
            &action.bindings,
            independent,
            &action.operation_id,
            false,
            window,
            value.history_sequence,
        )
    });
    report(
        public_matches && independent_matches,
        "ADMIN-032",
        &action.operation_id,
        "caller-ordered exact ACL deletion matches and immediate independent absence",
        public,
        independent,
        violations,
    );
}

fn observations_match(
    expected: &[testlab_schema::LiteralAclBinding],
    actual: Option<&Vec<Indexed<testlab_schema::BrokerAclState>>>,
    operation_id: &testlab_schema::OperationId,
    present: bool,
    window: Option<AdminCommandWindow>,
    public_sequence: u64,
) -> bool {
    actual.is_some_and(|actual| {
        actual.len() == expected.len()
            && actual.windows(2).all(|pair| {
                pair[1].value.observation == pair[0].value.observation.saturating_add(1)
            })
            && actual.iter().zip(expected).all(|(actual, expected)| {
                actual.value.operation_id == *operation_id
                    && actual.value.binding == *expected
                    && actual.value.present == present
                    && immediate_after_public(window, public_sequence, actual.history_sequence)
            })
    })
}

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    values.filter(|values| values.len() == 1)?.first()
}

#[allow(
    clippy::too_many_arguments,
    reason = "ACL evidence has public and independent streams"
)]
fn report<T>(
    valid: bool,
    contract: &str,
    operation_id: &testlab_schema::OperationId,
    expected: &str,
    public: Option<&Vec<Indexed<T>>>,
    independent: Option<&Vec<Indexed<testlab_schema::BrokerAclState>>>,
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
