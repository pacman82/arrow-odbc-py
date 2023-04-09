use std::{ffi::CString, fmt::Display, os::raw::c_char, ptr::NonNull};

/// Handle to an error emmitted by arrow odbc
pub struct ArrowOdbcError {
    message: CString,
}

impl ArrowOdbcError {
    pub fn new(source: impl Display) -> ArrowOdbcError {
        let mut raw_string = source.to_string();
        // Check the raw error message for interior `Nul`s. We can not put them in a CString, since
        // CString use `Nul` to represent their end. In case the error contains interior nuls, just
        // display the error message up to this point.
        let truncated_len = raw_string.find('\0').unwrap_or(raw_string.len());
        raw_string.truncate(truncated_len);
        // Terminating Nul will be appended by `new`.
        let message = CString::new(raw_string).unwrap();
        ArrowOdbcError { message }
    }

    /// Moves the instance to the heap and return a pointer to it.
    pub fn into_raw(self) -> *mut ArrowOdbcError {
        Box::into_raw(Box::new(self))
    }
}

impl<T> From<T> for ArrowOdbcError
where
    T: Display,
{
    fn from(source: T) -> ArrowOdbcError {
        ArrowOdbcError::new(source)
    }
}

/// Deallocates the resources associated with an error.
///
/// # Safety
///
/// Error must be a valid non null pointer to an Error.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_error_free(error: NonNull<ArrowOdbcError>) {
    drop(Box::from_raw(error.as_ptr()));
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
macro_rules! try_ {
    ($call:expr) => {
        match $call {
            Ok(value) => value,
            Err(error) => {
                // Early return in case of error
                let error = Into::<ArrowOdbcError>::into(error);
                return error.into_raw();
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::ArrowOdbcError;

    #[test]
    fn should_truncate_strings_with_interior_nul() {
        let message = "Hello\0World!";

        let error = ArrowOdbcError::new(message);

        assert_eq!("Hello", error.message.into_string().unwrap())
    }
}
