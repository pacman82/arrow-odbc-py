use std::{
    borrow::Cow,
    ptr::{NonNull, null_mut},
    slice, str,
    sync::{Arc, Mutex},
};

use arrow_odbc::odbc_api::{
    self, Connection, ConnectionOptions, SharedConnection, environment, escape_attribute_value,
};
use log::debug;

use crate::{ArrowOdbcError, ArrowOdbcReader, parameter::ArrowOdbcParameter, try_};

/// Opaque type to transport connection to an ODBC Datasource over language boundry
pub struct ArrowOdbcConnection(SharedConnection<'static>);

impl ArrowOdbcConnection {
    pub fn new(connection: Connection<'static>) -> Self {
        ArrowOdbcConnection(Arc::new(Mutex::new(connection)))
    }

    /// Take the inner connection out of its wrapper
    pub fn inner(&self) -> SharedConnection<'static> {
        self.0.clone()
    }

    pub fn set_autocommit(&self, enabled: bool) -> Result<(), odbc_api::Error> {
        let connection = self.0.lock().unwrap();
        connection.set_autocommit(enabled)
    }

    pub fn commit(&self) -> Result<(), odbc_api::Error> {
        let connection = self.0.lock().unwrap();
        connection.commit()
    }

    pub fn rollback(&self) -> Result<(), odbc_api::Error> {
        let connection = self.0.lock().unwrap();
        connection.rollback()
    }
}

/// Frees the resources associated with an ArrowOdbcConnection
///
/// # Safety
///
/// `connection` must point to a valid ArrowOdbcConnection.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_connection_free(connection: NonNull<ArrowOdbcConnection>) {
    drop(unsafe { Box::from_raw(connection.as_ptr()) });
}

/// Allocate and open an ODBC connection using the specified connection string. In case of an error
/// this function returns a NULL pointer.
///
/// # Safety
///
/// `connection_string_buf` must point to a valid utf-8 encoded string. `connection_string_len` must
/// hold the length of text in `connection_string_buf`.
/// `user` and or `password` are optional and are allowed to be `NULL`.
#[unsafe(no_mangle)]
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
    let env = try_!(environment());

    let connection_string =
        unsafe { slice::from_raw_parts(connection_string_buf, connection_string_len) };
    let mut connection_string = Cow::Borrowed(str::from_utf8(connection_string).unwrap());

    unsafe { append_attribute("UID", &mut connection_string, user, user_len) };
    unsafe { append_attribute("PWD", &mut connection_string, password, password_len) };

    let login_timeout_sec = if login_timeout_sec_ptr.is_null() {
        None
    } else {
        Some(unsafe { *login_timeout_sec_ptr })
    };

    let packet_size = if packet_size_ptr.is_null() {
        None
    } else {
        Some(unsafe { *packet_size_ptr })
    };

    let connection = try_!(env.connect_with_connection_string(
        &connection_string,
        ConnectionOptions {
            login_timeout_sec,
            packet_size
        }
    ));

    // Log dbms name to ease debugging of issues. Since ODBC calls are expensive, only query name
    // if we are actually logging the message.
    if log::max_level() <= log::LevelFilter::Debug {
        let dbms_name = try_!(connection.database_management_system_name());
        debug!("Database managment system name as reported by ODBC: {dbms_name}");
    }

    unsafe { *connection_out = Box::into_raw(Box::new(ArrowOdbcConnection::new(connection))) };
    null_mut()
}

/// Append attribute like user and value to connection string
unsafe fn append_attribute(
    attribute_name: &'static str,
    connection_string: &mut Cow<str>,
    ptr: *const u8,
    len: usize,
) {
    if ptr.is_null() {
        // In case the attribute in NULL there is nothing to append
        return;
    }

    let attribute_value = unsafe { slice::from_raw_parts(ptr, len) };
    let attribute_value =
        str::from_utf8(attribute_value).expect("Python side must always encode in UTF-8");

    let escaped = escape_attribute_value(attribute_value);
    *connection_string = format!("{connection_string}{attribute_name}={escaped};").into()
}

/// Set autocommit mode. Default is `true`.
///
/// # Safety
///
/// `connection` must point to a valid ArrowOdbcConnection.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_connection_set_autocommit(
    connection: NonNull<ArrowOdbcConnection>,
    enabled: bool,
) -> *mut ArrowOdbcError {
    let connection = unsafe { connection.as_ref() };
    try_!(connection.set_autocommit(enabled));
    null_mut()
}

/// Commit the current transaction. This is only meaningful if autocommit mode is disabled.
///
/// # Safety
///
/// `connection` must point to a valid ArrowOdbcConnection.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_connection_commit(
    connection: NonNull<ArrowOdbcConnection>,
) -> *mut ArrowOdbcError {
    let connection = unsafe { connection.as_ref() };
    try_!(connection.commit());
    null_mut()
}

/// Rollback the current transaction. This is only meaningful if autocommit mode is disabled.
///
/// # Safety
///
/// `connection` must point to a valid ArrowOdbcConnection.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_connection_rollback(
    connection: NonNull<ArrowOdbcConnection>,
) -> *mut ArrowOdbcError {
    let connection = unsafe { connection.as_ref() };
    try_!(connection.rollback());
    null_mut()
}

/// Creates an Arrow ODBC reader instance.
///
/// Executes the SQL Query and moves the reader into cursor state.
///
/// # Safety
///
/// * `connection` must point to a valid OdbcConnection. This function takes ownership of the
///   connection, even in case of an error. So The connection must not be freed explicitly
///   afterwards.
/// * `reader` must point to a valid reader in empty state.
/// * `query_buf` must point to a valid utf-8 string
/// * `query_len` describes the len of `query_buf` in bytes.
/// * `parameters` must contain only valid pointers. This function takes ownership of all of them
///   independent if the function succeeds or not. Yet it does not take ownership of the array
///   itself.
/// * `parameters_len` number of elements in parameters.
/// * `max_text_size` optional upper bound for the size of text columns. Use `0` to indicate that no
///   uppper bound applies.
/// * `max_binary_size` optional upper bound for the size of binary columns. Use `0` to indicate
///   that no uppper bound applies.
/// * `fallibale_allocations`: `TRUE` if allocations should return an error, `FALSE` if it is fine
///   to abort the process. Enabling might have a performance overhead, so it might be desirable to
///   disable it, if you know there is enough memory available.
/// * `schema`: Optional input arrow schema. NULL means no input schema is supplied. Should a
///   schema be supplied `schema` Rust will take ownership of it an the `schema` will be
///   overwritten with an empty one. This means the Python code, must only deallocate the memory
///   directly pointed to by `schema`, but not freeing the resources of the passed schema.
/// * `query_timout_sec`: Optional query timeout in seconds. If `NULL` no timeout is applied.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_connection_execute(
    connection: NonNull<ArrowOdbcConnection>,
    mut reader: NonNull<ArrowOdbcReader>,
    query_buf: *const u8,
    query_len: usize,
    parameters: *const *mut ArrowOdbcParameter,
    parameters_len: usize,
    query_timeout_sec: *const usize,
) -> *mut ArrowOdbcError {
    let connection = unsafe { connection.as_ref() }.inner();
    // Transtlate C Args into more idiomatic rust representations
    let query = unsafe { slice::from_raw_parts(query_buf, query_len) };
    let query = str::from_utf8(query).unwrap();

    let parameters = if parameters.is_null() {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(parameters, parameters_len) }
            .iter()
            .map(|&p| unsafe { Box::from_raw(p) }.unwrap())
            .collect()
    };

    let query_timeout_sec = if query_timeout_sec.is_null() {
        None
    } else {
        Some(unsafe { *query_timeout_sec })
    };

    try_!(unsafe { reader.as_mut() }.promote_to_cursor(
        connection,
        query,
        &parameters[..],
        query_timeout_sec
    ));

    null_mut() // Ok(())
}
