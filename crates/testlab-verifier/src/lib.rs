//! Pure deterministic verification maps public history and observations to contracts.

mod client_failure;
mod consumer;
mod contracts;
mod index;
mod lifecycle;
mod protocol;
mod support;
mod verify;

pub use contracts::known_contract_ids;
pub use verify::verify;

#[cfg(test)]
mod contract_test;
#[cfg(test)]
mod verify_fixture;
#[cfg(test)]
mod verify_integrity_test;
#[cfg(test)]
mod verify_test;
