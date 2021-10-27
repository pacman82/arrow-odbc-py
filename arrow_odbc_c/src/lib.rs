//! Defines C bindings for `arrow-odbc` to enable using it from Python.

mod error;

use std::{ptr::{NonNull, null_mut}, slice, str};

use arrow_odbc::odbc_api::{Connection, Environment};
use lazy_static::lazy_static;

pub use error::{arrow_odbc_error_free, arrow_odbc_error_message, ArrowOdbcError};

lazy_static! {
    static ref ENV: Environment = Environment::new().unwrap();
}

/// Opaque type to transport connection to an ODBC Datasource over language boundry
pub struct OdbcConnection(Connection<'static>);

/// Allocate and open an ODBC connection using the specified connection string. In case of an error
/// this function returns a NULL pointer.
///
/// # Safety
///
/// `connection_string_buf` must point to a valid utf-8 encoded string. `connection_string_len` must
/// hold the length of text in `connection_string_buf`.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_connect_with_connection_string(
    connection_string_buf: *const u8,
    connection_string_len: usize,
    error_out: *mut *mut ArrowOdbcError,
) -> *mut OdbcConnection {
    let connection_string = slice::from_raw_parts(connection_string_buf, connection_string_len);
    let connection_string = str::from_utf8(connection_string).unwrap();

    let connection = try_odbc!(
        ENV.connect_with_connection_string(connection_string),
        error_out
    );

    Box::into_raw(Box::new(OdbcConnection(connection)))
}

/// Frees the resources associated with an OdbcConnection
///
/// # Safety
///
/// `connection` must point to a valid OdbcConnection.
#[no_mangle]
pub unsafe extern "C" fn odbc_connection_free(connection: NonNull<OdbcConnection>) {
    Box::from_raw(connection.as_ptr());
}

pub struct ArrowOdbcReader;

/// Creates an Arrow ODBC reader instance
///
/// # Safety
///
/// * `connection` must point to a valid OdbcConnection.
/// * `query_buf` must point to a valid utf-8 string
/// * `query_len` describes the len of `query_buf` in bytes.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_make(
    connection: NonNull<OdbcConnection>,
    query_buf: *const u8,
    query_len: usize,
    batch_size: usize,
    error_out: *mut *mut ArrowOdbcError,
) -> * mut ArrowOdbcReader {
    let query = slice::from_raw_parts(query_buf, query_len);
    let query = str::from_utf8(query).unwrap();

    let maybe_cursor = try_odbc!(connection.as_ref().0.execute(query, ()), error_out);
    if let Some(cursor) = maybe_cursor {
        Box::into_raw(Box::new(ArrowOdbcReader))
    } else {
        null_mut()
    }
}

/// Frees the resources associated with an ArrowOdbcReader
///
/// # Safety
///
/// `connection` must point to a valid ArrowOdbcReader.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_free(connection: NonNull<ArrowOdbcReader>) {
    Box::from_raw(connection.as_ptr());
}