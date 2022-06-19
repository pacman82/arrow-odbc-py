use std::ptr::{NonNull, null_mut};

use crate::{OdbcConnection, ArrowOdbcError};

/// Consumes the batches of an Arrow ODBC reader and inserts them into a table
///
/// Takes ownership of connection even in case of an error.
///
/// # Safety
///
/// * `connection` must point to a valid OdbcConnection. This function takes ownership of the
///   connection, even in case of an error. So The connection must not be freed explicitly
///   afterwards.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_insert_into_table(
    connection: NonNull<OdbcConnection>,
) -> *mut ArrowOdbcError {
    let connection = *Box::from_raw(connection.as_ptr());

    null_mut()
}