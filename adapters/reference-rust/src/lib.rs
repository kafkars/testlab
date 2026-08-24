//! Reference adapter demonstrates the external protocol without client internals.

mod broker_client;
mod session;
mod session_send;
mod session_unsupported;
mod state;

pub use session::{AdapterError, run_stdio};

#[cfg(test)]
mod session_test;
#[cfg(test)]
mod session_unsupported_test;
