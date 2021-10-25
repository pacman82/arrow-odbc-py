use std::{ptr::null_mut, slice, str};

use arrow_odbc::odbc_api::Environment;
use lazy_static::lazy_static;

lazy_static! {
    static ref ENV: Environment = Environment::new().unwrap();
}

/// Opaque type to transport connection to an ODBC Datasource over language boundry
#[no_mangle]
pub struct OdbcConnection();

/// Allocate and open an ODBC connection using the specified connection string. In case of an error
/// this function returns a NULL pointer.
///
/// # Safety
///
/// `connection_string_buf` must point to a valid utf-8 encoded string. `connection_string_len` must
/// hold the length of text in `connection_string_buf`.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_connect_with_connection_string (
    connection_string_buf: *const u8,
    connection_string_len: usize,
) -> * mut OdbcConnection {
    let connection_string = slice::from_raw_parts(connection_string_buf, connection_string_len);
    let connection_string = str::from_utf8(connection_string).unwrap();

    match ENV.connect_with_connection_string(connection_string) {
        Ok(_) => todo!(),
        Err(error) => null_mut(),
    }
}
