use std::{
    ptr::{null, null_mut, NonNull},
    slice, str,
};

use arrow_odbc::{
    odbc_api::{CursorImpl, StatementConnection},
    OdbcReader,
};

use crate::{try_odbc, ArrowOdbcError, OdbcConnection};

pub struct ArrowOdbcReader(OdbcReader<CursorImpl<StatementConnection<'static>>>);

/// Creates an Arrow ODBC reader instance.
///
/// Takes ownership of connection, also in case of an error.
///
/// # Safety
///
/// * `connection` must point to a valid OdbcConnection.
/// * `query_buf` must point to a valid utf-8 string
/// * `query_len` describes the len of `query_buf` in bytes.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_make(
    connection: NonNull<OdbcConnection>,
    query_buf: *const u8,
    query_len: usize,
    batch_size: usize,
    error_out: *mut *mut ArrowOdbcError,
) -> *mut ArrowOdbcReader {
    let query = slice::from_raw_parts(query_buf, query_len);
    let query = str::from_utf8(query).unwrap();

    let connection = *Box::from_raw(connection.as_ptr());

    let maybe_cursor = try_odbc!(connection.0.into_cursor(query, ()), error_out);
    if let Some(cursor) = maybe_cursor {
        let reader = try_odbc!(OdbcReader::new(cursor, batch_size), error_out);
        Box::into_raw(Box::new(ArrowOdbcReader(reader)))
    } else {
        null_mut()
    }
}

/// Frees the resources associated with an ArrowOdbcReader
///
/// # Safety
///
/// `connection` must point to a valid ArrowOdbcReader.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_free(connection: NonNull<ArrowOdbcReader>) {
    Box::from_raw(connection.as_ptr());
}

pub struct ArrowOdbcBatch;

#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_next(
    mut reader: NonNull<ArrowOdbcReader>,
    error_out: *mut *mut ArrowOdbcError,
) -> *mut ArrowOdbcBatch {
    if let Some(result) = reader.as_mut().0.next() {
        let batch = try_odbc!(result, error_out);
        let mut batch = ArrowOdbcBatch;
        &mut batch as *mut ArrowOdbcBatch
    } else {
        null_mut()
    }
}
