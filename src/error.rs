use std::{ffi::CString, os::raw::c_char, ptr::NonNull};
use arrow_odbc::odbc_api;

/// Handle to an error emmitted by arrow odbc
pub struct ArrowOdbcError{
    message: CString
}

impl ArrowOdbcError {
    pub fn new(source: odbc_api::Error) -> ArrowOdbcError {
        let bytes = source.to_string().into_bytes();
        // Terminating Nul will be appended by `new`.
        let message = CString::new(bytes).unwrap();
        ArrowOdbcError { message }
    }
}

/// Deallocates the resources associated with an error.
/// 
/// # Safety
/// 
/// Error must be a valid non null pointer to an Error.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_error_free(error: NonNull<ArrowOdbcError>){
    Box::from_raw(error.as_ptr());
}

/// A zero terminated string describing the error
/// 
/// # Safety
/// 
/// Error must be a valid non null pointer to an Error.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_error_message(error: *const ArrowOdbcError) -> * const c_char {
    let error = &*error;
    error.message.as_ptr()
}