use arrow_odbc::odbc_api;
use std::{ffi::CString, os::raw::c_char, ptr::NonNull};

/// Handle to an error emmitted by arrow odbc
pub struct ArrowOdbcError {
    message: CString,
}

impl ArrowOdbcError {

    pub fn from_odbc_error(source: odbc_api::Error) -> ArrowOdbcError {
        let bytes = source.to_string().into_bytes();
        // Terminating Nul will be appended by `new`.
        let message = CString::new(bytes).unwrap();
        ArrowOdbcError { message }
    }

    /// Moves the instance to the heap and return a pointer to it.
    pub fn into_raw(self) -> *mut ArrowOdbcError {
        Box::into_raw(Box::new(self))
    }
}

/// Deallocates the resources associated with an error.
///
/// # Safety
///
/// Error must be a valid non null pointer to an Error.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_error_free(error: NonNull<ArrowOdbcError>) {
    Box::from_raw(error.as_ptr());
}

/// A zero terminated string describing the error
///
/// # Safety
///
/// Error must be a valid non null pointer to an Error.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_error_message(error: *const ArrowOdbcError) -> *const c_char {
    let error = &*error;
    error.message.as_ptr()
}

#[macro_export]
macro_rules! try_odbc {
    ($call:expr, $error_out:ident) => {
        match $call {
            Ok(value) => {
                *$error_out = null_mut();
                value
            }
            Err(error) => {
                *$error_out = ArrowOdbcError::from_odbc_error(error).into_raw();
                // Early return in case of error
                return null_mut();
            }
        }
    };
}
