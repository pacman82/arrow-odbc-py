//! Defines C bindings for `arrow-odbc` to enable using it from Python.
mod connection;
mod error;
mod logging;
mod parameter;
mod pool;
mod reader;
mod writer;

pub use self::{
    connection::{ArrowOdbcConnection, arrow_odbc_connection_make},
    error::{ArrowOdbcError, arrow_odbc_error_free, arrow_odbc_error_message},
    logging::arrow_odbc_log_to_stderr,
    reader::{ArrowOdbcReader, arrow_odbc_reader_free, arrow_odbc_reader_next},
    writer::{
        ArrowOdbcWriter, arrow_odbc_writer_free, arrow_odbc_writer_make,
        arrow_odbc_writer_write_batch,
    },
};
