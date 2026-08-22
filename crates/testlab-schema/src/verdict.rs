//! Deterministic verdicts distinguish semantic failure from invalid evidence.

use serde::{Deserialize, Serialize};

use crate::{ContractId, OperationId};

/// Final status for one sealed test attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerdictStatus {
    /// Valid evidence and no contract violation.
    Passed,
    /// Valid evidence with one or more contract violations.
    Failed,
    /// Environment, process, protocol, or harness failure prevents a claim.
    Invalid,
}

/// Deterministic run verdict.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Verdict {
    /// Final status.
    pub status: VerdictStatus,
    /// Stable contract violations or invalidity reasons.
    pub violations: Vec<Violation>,
}

impl Verdict {
    /// Creates a passing verdict.
    pub fn passed() -> Self {
        Self {
            status: VerdictStatus::Passed,
            violations: Vec::new(),
        }
    }

    /// Creates a valid semantic failure.
    pub fn failed(violations: Vec<Violation>) -> Self {
        Self {
            status: VerdictStatus::Failed,
            violations,
        }
    }

    /// Creates an invalid run.
    pub fn invalid(violations: Vec<Violation>) -> Self {
        Self {
            status: VerdictStatus::Invalid,
            violations,
        }
    }

    /// Returns whether the run produced passing evidence.
    pub fn is_passed(&self) -> bool {
        self.status == VerdictStatus::Passed
    }
}

/// One stable semantic or validity violation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Violation {
    /// Contract that was not satisfied.
    pub contract_id: ContractId,
    /// Bounded deterministic explanation.
    pub message: String,
    /// Operation under test when applicable.
    pub operation_id: Option<OperationId>,
    /// Stable evidence references.
    pub evidence: Vec<String>,
}
