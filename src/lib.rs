use std::{ffi::CString, os::raw::c_char, ptr::null_mut, slice, str};

use arrow_odbc::odbc_api::{self, Environment};
use lazy_static::lazy_static;

lazy_static! {
    static ref ENV: Environment = Environment::new().unwrap();
}

/// Opaque type to transport connection to an ODBC Datasource over language boundry
pub struct OdbcConnection();

pub struct Error{
    message: CString
}

impl Error {
    pub fn new(source: odbc_api::Error) -> Error {
        let mut bytes = source.to_string().into_bytes();
        // Terminating Nul will be appended by `new`.
        let message = CString::new(bytes).unwrap();
        Error { message }
    }
}

/// Deallocates the resources associated with an error.
/// 
/// # Safety
/// 
/// Error must be a valid non null pointer to an Error.
#[no_mangle]
pub unsafe extern "C" fn odbc_error_free(error: *mut Error){
    Box::from_raw(error);
}

/// Deallocates the resources associated with an error.
/// 
/// # Safety
/// 
/// Error must be a valid non null pointer to an Error.
#[no_mangle]
pub unsafe extern "C" fn odbc_error_message(error: *const Error) -> * const c_char {
    let error = &*error;
    error.message.as_ptr()
}

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
    error_out: *mut *mut Error
) -> * mut OdbcConnection {
    let connection_string = slice::from_raw_parts(connection_string_buf, connection_string_len);
    let connection_string = str::from_utf8(connection_string).unwrap();

    match ENV.connect_with_connection_string(connection_string) {
        Ok(_) => {
            *error_out = null_mut();
            todo!()
        },
        Err(error) => {
            *error_out = Box::into_raw(Box::new(Error::new(error)));
            null_mut()
        }
    }
}
