//! Public client failures are semantic evidence, not harness invalidity.

use testlab_schema::Violation;

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify_client_failures(index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for failure in &index.command_failures {
        violations.push(violation(
            "CLIENT-001",
            format!(
                "public client command failed with code {}: {}",
                failure.code, failure.diagnostic
            ),
            None,
            vec![format!("history:{}", failure.history_sequence)],
        ));
    }
}
