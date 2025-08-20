use crate::reader::into_text_encoding;
use arrow_odbc::odbc_api::parameter::{InputParameter, VarCharBox, VarWCharBox};
use std::slice;
use widestring::Utf16String;

/// Opaque type holding a parameter intended to be bound to a placeholder (`?`) in an SQL query.
pub struct ArrowOdbcParameter(Box<dyn InputParameter>);

impl ArrowOdbcParameter {
    fn from_opt_str(value: Option<&[u8]>, use_utf16: bool) -> Self {
        let inner = if use_utf16 {
            Self::utf16_text(value)
        } else {
            Self::utf8_text(value)
        };
        Self(Box::new(inner))
    }

    fn utf16_text(value: Option<&[u8]>) -> Box<dyn InputParameter> {
        let vcb = if let Some(byte_slice) = value {
            let str_slice = str::from_utf8(byte_slice).unwrap();
            let utf16_vec = Utf16String::from_str(str_slice).into_vec();
            VarWCharBox::from_vec(utf16_vec)
        } else {
            VarWCharBox::null()
        };
        Box::new(vcb)
    }

    fn utf8_text(value: Option<&[u8]>) -> Box<dyn InputParameter> {
        let vcb = if let Some(slice) = value {
            VarCharBox::from_vec(slice.to_vec())
        } else {
            VarCharBox::null()
        };
        Box::new(vcb)
    }
}

impl ArrowOdbcParameter {
    pub fn unwrap(self) -> Box<dyn InputParameter> {
        self.0
    }
}

/// # Safety
///
/// `char_buf` may be `NULL`, but if it is not, it must contain a valid utf-8 sequence not shorter
/// than `char_len`. This function does not take ownership of the parameter. The parameter must at
/// least be valid until the call make reader is finished.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_parameter_string_make(
    char_buf: *const u8,
    char_len: usize,
    text_encoding: u8,
) -> *mut ArrowOdbcParameter {
    let text_encoding = into_text_encoding(text_encoding);

    let opt = if char_buf.is_null() {
        None
    } else {
        Some(unsafe { slice::from_raw_parts(char_buf, char_len) })
    };

    let param = ArrowOdbcParameter::from_opt_str(opt, text_encoding.use_utf16());
    Box::into_raw(Box::new(param))
}
