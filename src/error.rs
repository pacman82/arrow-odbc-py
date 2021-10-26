use std::{ffi::CString, os::raw::c_char};
use arrow_odbc::odbc_api;

/// Handle to an error emmitted by arrow odbc
pub struct Error{
    message: CString
}

impl Error {
    pub fn new(source: odbc_api::Error) -> Error {
        let bytes = source.to_string().into_bytes();
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