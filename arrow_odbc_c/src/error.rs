use std::{ffi::CString, fmt::Display, os::raw::c_char, ptr::NonNull};

/// Handle to an error emmitted by arrow odbc
pub struct ArrowOdbcError {
    message: CString,
}

impl ArrowOdbcError {
    pub fn new(source: impl Display) -> ArrowOdbcError {
        let bytes = source.to_string();
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
macro_rules! success_or_null {
    ($call:expr, $error_out:ident) => {
        match $call {
            Ok(value) => {
                *$error_out = null_mut();
                value
            }
            Err(error) => {
                *$error_out = ArrowOdbcError::new(error).into_raw();
                // Early return in case of error
                return null_mut();
            }
        }
    };
}

#[macro_export]
macro_rules! try_unit {
    ($call:expr, $error_out:ident) => {
        match $call {
            Ok(value) => {
                *$error_out = null_mut();
                value
            }
            Err(error) => {
                *$error_out = ArrowOdbcError::new(error).into_raw();
                // Early return in case of error
                return;
            }
        }
    };
}
