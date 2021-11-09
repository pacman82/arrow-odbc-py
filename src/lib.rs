//! Defines C bindings for `arrow-odbc` to enable using it from Python.

mod error;
mod reader;

use std::{ptr::null_mut, slice, str};

use arrow_odbc::odbc_api::{Connection, Environment};
use lazy_static::lazy_static;

pub use error::{arrow_odbc_error_free, arrow_odbc_error_message, ArrowOdbcError};
pub use reader::{
    arrow_odbc_reader_free, arrow_odbc_reader_make, arrow_odbc_reader_next, ArrowOdbcReader,
};

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
    connection_out: *mut *mut OdbcConnection,
) -> *mut ArrowOdbcError {
    let connection_string = slice::from_raw_parts(connection_string_buf, connection_string_len);
    let connection_string = str::from_utf8(connection_string).unwrap();

    let connection = try_!(ENV.connect_with_connection_string(connection_string));

    *connection_out = Box::into_raw(Box::new(OdbcConnection(connection)));
    null_mut()
}
