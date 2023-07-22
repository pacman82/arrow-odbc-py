//! Defines C bindings for `arrow-odbc` to enable using it from Python.
mod error;
mod logging;
mod parameter;
mod reader;
mod writer;

use std::{borrow::Cow, ptr::null_mut, slice, str};

use arrow_odbc::odbc_api::{escape_attribute_value, Connection, ConnectionOptions, Environment};
use lazy_static::lazy_static;

pub use error::{arrow_odbc_error_free, arrow_odbc_error_message, ArrowOdbcError};
pub use logging::arrow_odbc_log_to_stderr;
pub use reader::{
    arrow_odbc_reader_free, arrow_odbc_reader_make, arrow_odbc_reader_next, ArrowOdbcReader,
};
pub use writer::{
    arrow_odbc_writer_free, arrow_odbc_writer_make, arrow_odbc_writer_write_batch, ArrowOdbcWriter,
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
/// `user` and or `password` are optional and are allowed to be `NULL`.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_connect_with_connection_string(
    connection_string_buf: *const u8,
    connection_string_len: usize,
    user: *const u8,
    user_len: usize,
    password: *const u8,
    password_len: usize,
    login_timeout_sec_ptr: *const u32,
    connection_out: *mut *mut OdbcConnection,
) -> *mut ArrowOdbcError {
    let connection_string = slice::from_raw_parts(connection_string_buf, connection_string_len);
    let mut connection_string = Cow::Borrowed(str::from_utf8(connection_string).unwrap());

    append_attribute("UID", &mut connection_string, user, user_len);
    append_attribute("PWD", &mut connection_string, password, password_len);

    let login_timeout_sec = if login_timeout_sec_ptr.is_null() {
        None
    } else {
        Some(*login_timeout_sec_ptr)
    };

    let connection = try_!(ENV.connect_with_connection_string(
        &connection_string,
        ConnectionOptions { login_timeout_sec }
    ));

    *connection_out = Box::into_raw(Box::new(OdbcConnection(connection)));
    null_mut()
}

/// Append attribute like user and value to connection string
unsafe fn append_attribute(
    attribute_name: &'static str,
    connection_string: &mut Cow<str>,
    ptr: *const u8,
    len: usize,
) {
    // Attribute is optional and not set. Nothing to append.
    if ptr.is_null() {
        return;
    }

    let bytes = slice::from_raw_parts(ptr, len);
    let text = str::from_utf8(bytes).unwrap();
    let escaped = escape_attribute_value(text);
    *connection_string = format!("{connection_string}{attribute_name}={escaped};").into()
}
