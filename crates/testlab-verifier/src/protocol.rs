//! Protocol verification binds the sealed adapter identity to one handshake event.

use testlab_schema::{AdapterDescriptor, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify_protocol(
    adapter: &AdapterDescriptor,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let evidence = index
        .ready
        .iter()
        .map(|(sequence, _)| format!("history:{sequence}"))
        .collect::<Vec<_>>();
    if index.ready.len() != 1 {
        violations.push(violation(
            "PROTO-001",
            format!(
                "expected exactly one ready event, observed {}",
                index.ready.len()
            ),
            None,
            evidence,
        ));
        return;
    }
    if &index.ready[0].1 != adapter {
        violations.push(violation(
            "PROTO-001",
            "ready descriptor differs from the sealed adapter identity".to_owned(),
            None,
            evidence,
        ));
    }
}
