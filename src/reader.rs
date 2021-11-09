use std::{
    ffi::c_void,
    ptr::{null_mut, NonNull},
    slice, str,
};

use arrow_odbc::{
    arrow::{
        array::{Array, StructArray},
        ffi::FFI_ArrowSchema,
        record_batch::RecordBatchReader,
    },
    odbc_api::{CursorImpl, StatementConnection},
    OdbcReader,
};

use crate::{try_, ArrowOdbcError, OdbcConnection};

/// Opaque type holding all the state associated with an ODBC reader implementation in Rust. This
/// type also has ownership of the ODBC Connection handle.
pub struct ArrowOdbcReader(OdbcReader<CursorImpl<StatementConnection<'static>>>);

/// Creates an Arrow ODBC reader instance.
///
/// Takes ownership of connection even in case of an error. `reader_out` is assigned a NULL pointer
/// in case the query does not return a result set.
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
    reader_out: *mut *mut ArrowOdbcReader,
) -> *mut ArrowOdbcError {
    let query = slice::from_raw_parts(query_buf, query_len);
    let query = str::from_utf8(query).unwrap();

    let connection = *Box::from_raw(connection.as_ptr());

    let maybe_cursor = try_!(connection.0.into_cursor(query, ()));
    if let Some(cursor) = maybe_cursor {
        let reader = try_!(OdbcReader::new(cursor, batch_size));
        *reader_out = Box::into_raw(Box::new(ArrowOdbcReader(reader)))
    } else {
        *reader_out = null_mut()
    }
    null_mut()
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

/// # Safety
/// 
/// * `reader` must be valid non-null reader, allocated by [`arrow_odbc_reader_make`].
/// * `array_out` and `schema_out` must both point to valid pointers, which themselves may be null.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_next(
    mut reader: NonNull<ArrowOdbcReader>,
    array_out: *mut *mut c_void,
    schema_out: *mut *mut c_void,
) -> *mut ArrowOdbcError {
    if let Some(result) = reader.as_mut().0.next() {
        let batch = try_!(result);
        let struct_array: StructArray = batch.into();
        let (ffi_array_ptr, ffi_schema_ptr) = try_!(struct_array.to_raw());
        *array_out = ffi_array_ptr as *mut c_void;
        *schema_out = ffi_schema_ptr as *mut c_void;
    } else {
        *array_out = null_mut();
        *schema_out = null_mut();
    }
    null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_schema(
    mut reader: NonNull<ArrowOdbcReader>,
    out_schema: *mut c_void,
) -> *mut ArrowOdbcError {
    let out_schema: *mut FFI_ArrowSchema = out_schema as *mut FFI_ArrowSchema;

    let reader = &mut reader.as_mut().0;
    let schema_ref = reader.schema();
    let schema = (*schema_ref).clone();
    let schema_ffi = try_!(schema.try_into());
    *out_schema = schema_ffi;
    null_mut()
}
