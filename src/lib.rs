//! Defines C bindings for `arrow-odbc` to enable using it from Python.
mod connection;
mod error;
mod logging;
mod parameter;
mod pool;
mod reader;
mod writer;

use std::sync::OnceLock;

use arrow_odbc::odbc_api::Environment;

pub use self::{
    connection::{arrow_odbc_connection_make, ArrowOdbcConnection},
    error::{arrow_odbc_error_free, arrow_odbc_error_message, ArrowOdbcError},
    logging::arrow_odbc_log_to_stderr,
    reader::{arrow_odbc_reader_free, arrow_odbc_reader_next, ArrowOdbcReader},
    writer::{
        arrow_odbc_writer_free, arrow_odbc_writer_make, arrow_odbc_writer_write_batch,
        ArrowOdbcWriter,
    },
};

/// Using an ODBC environment with static lifetime eases our work with concurrent fetching, as we
/// can work with safe code and without scoped threads.
static ENV: OnceLock<Environment> = OnceLock::new();
