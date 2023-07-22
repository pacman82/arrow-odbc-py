use std::ptr::null_mut;

use crate::{try_, ArrowOdbcError};

/// Activates logging to standard error from Rust
///
/// Log levels:
///
/// * 0 - Error
/// * 1 - Warn
/// * 2 - Info
/// * 3 - Debug
/// * 4 or higher - Trace
#[no_mangle]
pub extern "C" fn arrow_odbc_log_to_stderr(level: u32) -> *mut ArrowOdbcError {
    let level = match level {
        0 => log::Level::Error,
        1 => log::Level::Warn,
        2 => log::Level::Info,
        3 => log::Level::Debug,
        _ => log::Level::Trace,
    };

    let mut log_backend = stderrlog::new();
    log_backend.verbosity(level);
    try_!(log_backend.init());

    null_mut() // means Ok(())
}
