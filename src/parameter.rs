use crate::reader::into_text_encoding;
use arrow_odbc::odbc_api::{IntoParameter, parameter::InputParameter};
use std::slice;
use widestring::U16String;

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
        let arg = value
            .map(|bytes| {
                let str_slice = str::from_utf8(bytes).unwrap();
                U16String::from_str(str_slice)
            })
            .into_parameter();
        Box::new(arg)
    }

    fn utf8_text(value: Option<&[u8]>) -> Box<dyn InputParameter> {
        let arg = value
            .map(|bytes| {
                let str_slice = str::from_utf8(bytes).unwrap();
                str_slice.to_owned()
            })
            .into_parameter();
        Box::new(arg)
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

#[cfg(test)]
mod tests {
    use arrow_odbc::odbc_api::buffers::Indicator;
    use widestring::Utf16Str;

    use crate::parameter::ArrowOdbcParameter;

    #[test]
    fn construct_utf16_parameter() {
        // Given a chinese greeting in utf-8
        let text = "您好";

        // When constructing a parameter with utf-16 encoding
        let use_utf16 = true;
        let param = ArrowOdbcParameter::from_opt_str(Some(text.as_bytes()), use_utf16);

        // Then
        let param = param.unwrap();
        let indicator = unsafe { Indicator::from_isize(*param.indicator_ptr()) };
        assert_eq!(Indicator::Length(4), indicator); // Size of the encoded value in bytes
        let value = param.value_ptr();
        let value_slice = unsafe { std::slice::from_raw_parts(value as *const u16, 2) };
        let value = Utf16Str::from_slice(value_slice).expect("Must be valid UTF-16");
        let value = value.to_string();
        assert_eq!(text, value);
    }
}
