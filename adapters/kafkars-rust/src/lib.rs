//! Kafkars adapter translates only the packaged public client surface.

mod adapter_error;
mod assigned_consumers;
mod connection_security;
mod group_consumers;
mod normalize;
mod protocol;
mod protocol_admin;
mod protocol_consumer;
mod protocol_group;
mod protocol_lifecycle;
mod protocol_send;
mod state;
mod state_error;
mod transaction_execute;
mod transaction_fence;
mod transactional_producers;

pub use adapter_error::AdapterError;
pub use protocol::run_stdio;

#[cfg(test)]
mod connection_security_test;
#[cfg(test)]
mod normalize_test;
#[cfg(test)]
mod protocol_admin_test;
#[cfg(test)]
mod protocol_test;
