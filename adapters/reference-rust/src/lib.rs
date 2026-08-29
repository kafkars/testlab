//! Reference adapter demonstrates the external protocol without client internals.

mod broker_client;
mod session;
mod session_descriptor;
mod session_end;
mod session_error;
mod session_send;
mod session_unsupported;
mod state;

pub use session::run_stdio;
pub use session_error::AdapterError;

#[cfg(test)]
mod session_test;
#[cfg(test)]
mod session_unsupported_test;
