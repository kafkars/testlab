//! Reference adapter demonstrates the external protocol without client internals.

mod broker_client;
mod session;
mod state;

pub use session::{AdapterError, run_stdio};

#[cfg(test)]
mod session_test;
