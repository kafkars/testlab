//! Kafkars adapter translates only the packaged public client surface.

mod adapter_error;
mod connection_security;
mod normalize;
mod protocol;
mod state;

pub use adapter_error::AdapterError;
pub use protocol::run_stdio;

#[cfg(test)]
mod connection_security_test;
#[cfg(test)]
mod normalize_test;
#[cfg(test)]
mod protocol_test;
