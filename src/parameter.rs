use std::slice;

use arrow_odbc::odbc_api::parameter::VarCharSlice;

/// Opaque type holding a parameter intended to be bound to a placeholder (`?`) in an SQL query.
pub struct ArrowOdbcParameter<'a>(VarCharSlice<'a>);

impl<'a> ArrowOdbcParameter<'a> {
    fn from_opt_str(value: Option<&'a [u8]>) -> Self {
        let inner = if let Some(slice) = value {
            VarCharSlice::new(slice)
        } else {
            VarCharSlice::NULL
        };
        Self(inner)
    }
}

impl<'a> ArrowOdbcParameter<'a> {
    pub fn unwrap(self) -> VarCharSlice<'a> {
        self.0
    }
}

/// # Safety
///
/// `char_buf` may be `NULL`, but if it is not, it must contain a valid utf-8 sequence not shorter
/// than `char_len`. This function does not take ownership of the parameter. The parameter must at
/// least be valid until the call make reader is finished.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_parameter_string_make(
    char_buf: *const u8,
    char_len: usize,
) -> *mut ArrowOdbcParameter<'static> {
    let opt = if char_buf.is_null() {
        None
    } else {
        Some(slice::from_raw_parts(char_buf, char_len))
    };

    let param = ArrowOdbcParameter::from_opt_str(opt);
    Box::into_raw(Box::new(param))
}
