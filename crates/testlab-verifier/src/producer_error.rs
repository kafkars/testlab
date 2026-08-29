//! Producer error verification binds declarative expectations to exact public codes.

use testlab_schema::{OperationAssertion, OperationId, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(
    operation_id: &OperationId,
    assertion: Option<&OperationAssertion>,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let Some(expected) = assertion.and_then(|value| value.expected_error_code.as_deref()) else {
        return;
    };
    let errors = index.operation_errors.get(operation_id);
    let exact = errors.is_some_and(|values| {
        values.len() == 1 && values.first().is_some_and(|value| value.code == expected)
    });
    if exact {
        return;
    }
    let observed = errors
        .into_iter()
        .flatten()
        .map(|value| value.code.as_str())
        .collect::<Vec<_>>();
    violations.push(violation(
        "PROD-009",
        format!("expected exact operation error {expected}, observed {observed:?}"),
        Some(operation_id.clone()),
        errors
            .into_iter()
            .flatten()
            .map(|value| format!("history:{}", value.history_sequence))
            .collect(),
    ));
}
