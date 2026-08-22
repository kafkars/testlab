//! Testctl owns scenario execution, subject supervision, and sealed evidence.

mod app;
mod catalog;
mod catalog_io;
mod evidence;
mod evidence_io;
mod process;
mod process_io;
mod protocol_session;
mod recorder;
mod run_error;
mod runner;
mod runner_protocol;
mod session;
mod time;

pub use app::run_cli;
pub use run_error::AppError;

#[cfg(test)]
mod catalog_test;
#[cfg(test)]
mod recorder_test;
#[cfg(test)]
mod runner_test;
