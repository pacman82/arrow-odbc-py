use std::{
    borrow::Cow,
    ptr::{null_mut, NonNull},
    slice, str,
};

use arrow_odbc::odbc_api::{escape_attribute_value, Connection, ConnectionOptions, Environment};
use log::debug;

use crate::{try_, ArrowOdbcError, ENV};

/// Opaque type to transport connection to an ODBC Datasource over language boundry
pub struct ArrowOdbcConnection(Option<Connection<'static>>);

impl ArrowOdbcConnection {
    pub fn new(connection: Connection<'static>) -> Self {
        ArrowOdbcConnection(Some(connection))
    }

    /// Take the inner connection out of its wrapper
    pub fn take(&mut self) -> Connection<'static> {
        self.0.take().unwrap()
    }
}

/// Frees the resources associated with an ArrowOdbcConnection
///
/// # Safety
///
/// `reader` must point to a valid ArrowOdbcConnection.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_connection_free(connection: NonNull<ArrowOdbcConnection>) {
    drop(Box::from_raw(connection.as_ptr()));
}

/// Allocate and open an ODBC connection using the specified connection string. In case of an error
/// this function returns a NULL pointer.
///
/// # Safety
///
/// `connection_string_buf` must point to a valid utf-8 encoded string. `connection_string_len` must
/// hold the length of text in `connection_string_buf`.
/// `user` and or `password` are optional and are allowed to be `NULL`.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_connection_make(
    connection_string_buf: *const u8,
    connection_string_len: usize,
    user: *const u8,
    user_len: usize,
    password: *const u8,
    password_len: usize,
    login_timeout_sec_ptr: *const u32,
    packet_size_ptr: *const u32,
    connection_out: *mut *mut ArrowOdbcConnection,
) -> *mut ArrowOdbcError {
    let env = if let Some(env) = ENV.get() {
        // Use existing environment
        env
    } else {
        // ODBC Environment does not exist yet, create it.
        let env = try_!(Environment::new());
        ENV.get_or_init(|| env)
    };

    let connection_string = slice::from_raw_parts(connection_string_buf, connection_string_len);
    let mut connection_string = Cow::Borrowed(str::from_utf8(connection_string).unwrap());

    append_attribute("UID", &mut connection_string, user, user_len);
    append_attribute("PWD", &mut connection_string, password, password_len);

    let login_timeout_sec = if login_timeout_sec_ptr.is_null() {
        None
    } else {
        Some(*login_timeout_sec_ptr)
    };

    let packet_size = if packet_size_ptr.is_null() {
        None
    } else {
        Some(*packet_size_ptr)
    };

    let connection = try_!(env.connect_with_connection_string(
        &connection_string,
        ConnectionOptions {
            login_timeout_sec,
            packet_size
        }
    ));

    // Log dbms name to ease debugging of issues.
    let dbms_name = try_!(connection.database_management_system_name());
    debug!("Database managment system name as reported by ODBC: {dbms_name}");

    *connection_out = Box::into_raw(Box::new(ArrowOdbcConnection::new(connection)));
    null_mut()
}

/// Append attribute like user and value to connection string
unsafe fn append_attribute(
    attribute_name: &'static str,
    connection_string: &mut Cow<str>,
    ptr: *const u8,
    len: usize,
) {
    // Attribute is optional and not set. Nothing to append.
    if ptr.is_null() {
        return;
    }

    let bytes = slice::from_raw_parts(ptr, len);
    let text = str::from_utf8(bytes).unwrap();
    let escaped = escape_attribute_value(text);
    *connection_string = format!("{connection_string}{attribute_name}={escaped};").into()
}
