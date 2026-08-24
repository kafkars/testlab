//! Pure deterministic verification maps public history and observations to contracts.

mod admin;
mod admin_discovery;
mod admin_group;
mod client_failure;
mod consumer;
mod contracts;
mod index;
mod lifecycle;
mod observations;
mod protocol;
mod share;
mod support;
mod transaction;
mod verify;

pub use contracts::known_contract_ids;
pub use verify::verify;

#[cfg(test)]
mod admin_discovery_test;
#[cfg(test)]
mod admin_group_test;
#[cfg(test)]
mod admin_group_verdict_test;
#[cfg(test)]
mod admin_test;
#[cfg(test)]
mod contract_test;
#[cfg(test)]
mod share_test;
#[cfg(test)]
mod transaction_fence_test;
#[cfg(test)]
mod transaction_test;
#[cfg(test)]
mod verify_fixture;
#[cfg(test)]
mod verify_integrity_test;
#[cfg(test)]
mod verify_test;
