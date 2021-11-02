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

use crate::{ArrowOdbcError, OdbcConnection, success_or_null, try_unit};

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

    let maybe_cursor = success_or_null!(connection.0.into_cursor(query, ()), error_out);
    if let Some(cursor) = maybe_cursor {
        let reader = success_or_null!(OdbcReader::new(cursor, batch_size), error_out);
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

#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_next(
    mut reader: NonNull<ArrowOdbcReader>,
    array_out: *mut *mut c_void,
    schema_out: *mut *mut c_void,
    error_out: *mut *mut ArrowOdbcError,
) {
    if let Some(result) = reader.as_mut().0.next() {
        let batch = try_unit!(result, error_out);
        let struct_array: StructArray = batch.into();
        let (ffi_array_ptr, ffi_schema_ptr) = try_unit!(struct_array.to_raw(), error_out);
        *array_out = ffi_array_ptr as *mut c_void;
        *schema_out = ffi_schema_ptr as *mut c_void; 
    } else {
        *array_out = null_mut();
        *schema_out = null_mut();
    }
}

#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_schema(
    mut reader: NonNull<ArrowOdbcReader>,
    out_schema: *mut c_void,
    error_out: *mut *mut ArrowOdbcError,
) {
    let out_schema: *mut FFI_ArrowSchema = out_schema as *mut FFI_ArrowSchema;

    let reader = &mut reader.as_mut().0;
    let schema_ref = reader.schema();
    let schema = (*schema_ref).clone();
    match schema.try_into() {
        Ok(schema_ffi) => {
            *out_schema = schema_ffi;
            *error_out = null_mut();
        }
        Err(e) => *error_out = ArrowOdbcError::new(e).into_raw(),
    }
}
