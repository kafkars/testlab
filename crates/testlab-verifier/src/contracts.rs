//! Stable contract identifiers keep verdicts independent from prose wording.

/// Returns every contract identifier this repository cut may emit.
pub fn known_contract_ids() -> &'static [&'static str] {
    &[
        "HARNESS-001",
        "CAP-001",
        "PROTO-001",
        "PROTO-002",
        "PROD-001",
        "PROD-002",
        "PROD-003",
        "PROD-004",
        "PROD-005",
        "PROD-006",
        "PROD-007",
        "PROD-008",
        "LIFE-001",
        "LIFE-002",
        "LIFE-003",
        "LIFE-004",
        "LIFE-005",
        "LIFE-006",
        "LIFE-007",
    ]
}
