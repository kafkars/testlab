//! Independent model-broker fixture proves testlab's evidence path.

mod server;
mod state;
mod wire;

pub use server::{BrokerError, RunningBroker};
pub use wire::{ModelBrokerRequest, ModelBrokerResponse, ModelBrokerResponseStatus};

#[cfg(test)]
mod server_test;
