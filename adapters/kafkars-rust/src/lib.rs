//! Kafkars adapter translates only the packaged public client surface.

mod adapter_error;
mod admission_retry;
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
#[cfg(kafkars_share_candidate)]
mod protocol_share;
#[cfg(kafkars_share_candidate)]
mod share_consumers;
#[cfg(kafkars_share_candidate)]
mod share_consumers_acknowledge;
#[cfg(kafkars_share_candidate)]
mod share_consumers_close;
#[cfg(kafkars_share_candidate)]
mod share_consumers_receive;
mod state;
mod state_error;
mod state_share;
mod transaction_execute;
mod transaction_fence;
mod transactional_producers;

pub use adapter_error::AdapterError;
pub use protocol::run_stdio;

#[cfg(test)]
mod admission_retry_test;
#[cfg(test)]
mod connection_security_test;
#[cfg(test)]
mod normalize_test;
#[cfg(test)]
mod protocol_test;
