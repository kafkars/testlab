//! External environment effects remain separate from harness verdict logic.

mod terminal;
mod terminal_capture;

pub use terminal::{TerminalOutput, TerminalRequest, run_terminal};

#[cfg(test)]
mod terminal_test;
