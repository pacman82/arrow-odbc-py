use std::{
    ffi::c_void,
    mem::swap,
    os::raw::c_int,
    ptr::{null_mut, NonNull},
    slice, str,
    sync::Arc,
};

use arrow_odbc::{
    arrow::{
        array::{Array, StructArray},
        ffi::{FFI_ArrowArray, FFI_ArrowSchema},
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
/// * `connection` must point to a valid OdbcConnection. This function takes ownership of the
///   connection, even in case of an error. So The connection must not be freed explicitly
///   afterwards.
/// * `query_buf` must point to a valid utf-8 string
/// * `query_len` describes the len of `query_buf` in bytes.
/// * `reader_out` in case of success this will point to an instance of `ArrowOdbcReader`.
///   Ownership is transferred to the caller.
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
/// `reader` must point to a valid ArrowOdbcReader.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_free(reader: NonNull<ArrowOdbcReader>) {
    Box::from_raw(reader.as_ptr());
}

/// # Safety
///
/// * `reader` must be valid non-null reader, allocated by [`arrow_odbc_reader_make`].
/// * `array_out` and `schema_out` must both point to valid pointers, which themselves may be null.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_next(
    mut reader: NonNull<ArrowOdbcReader>,
    array: *mut c_void,
    schema: *mut c_void,
    has_next_out: *mut c_int,
) -> *mut ArrowOdbcError {
    let schema = schema as *mut FFI_ArrowSchema;
    let array = array as *mut FFI_ArrowArray;

    if let Some(result) = reader.as_mut().0.next() {
        *array = FFI_ArrowArray::empty();
        *schema = FFI_ArrowSchema::empty();

        let batch = try_!(result);
        let struct_array: StructArray = batch.into();

        let (ffi_array_ptr, ffi_schema_ptr) = try_!(struct_array.to_raw());

        // In order to avoid memory leaks we must convert both pointers returned by the  `to_raw`
        // method. So we must back to `Arc` again, so they are freed at the end of this function
        // call in order to avoid memory leaks. Furthermore it is the callers responsibility to
        // provide us with the FFI_Arrow* structures to fill, and the caller maintains ownership
        // over them.

        let mut arc_schema = Arc::from_raw(ffi_schema_ptr);
        let source_schema = Arc::get_mut(&mut arc_schema).unwrap();
        swap(&mut *schema, source_schema);

        let mut arc_array = Arc::from_raw(ffi_array_ptr);
        let source_array = Arc::get_mut(&mut arc_array).unwrap();
        swap(&mut *array, source_array);

        *has_next_out = 1;
    } else {
        *has_next_out = 0;
    }
    null_mut()
}

/// Retrieve the associated schema from a reader.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_schema(
    mut reader: NonNull<ArrowOdbcReader>,
    out_schema: *mut c_void,
) -> *mut ArrowOdbcError {
    let out_schema: *mut FFI_ArrowSchema = out_schema as *mut FFI_ArrowSchema;

    let reader = &mut reader.as_mut().0;
    let schema_ref = reader.schema();
    let schema = &*schema_ref;
    let schema_ffi = try_!(schema.try_into());
    *out_schema = schema_ffi;
    null_mut()
}
