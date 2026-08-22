//! Kafkars adapter process startup delegates all protocol work to the library.

use std::process::ExitCode;

fn main() -> ExitCode {
    match testlab_kafkars_adapter::run_stdio() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
