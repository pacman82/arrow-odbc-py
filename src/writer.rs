use std::{
    ffi::c_void,
    ptr::{self, NonNull, null_mut},
    slice, str,
};

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow_odbc::{
    OdbcWriter,
    arrow::{array::StructArray, datatypes::Schema, record_batch::RecordBatch},
    odbc_api::StatementConnection,
};

use crate::{ArrowOdbcConnection, ArrowOdbcError, try_};

/// Opaque type holding all the state associated with an ODBC writer implementation in Rust. This
/// type also has ownership of the ODBC Connection handle.
pub struct ArrowOdbcWriter(OdbcWriter<StatementConnection<'static>>);

/// Frees the resources associated with an ArrowOdbcWriter
///
/// # Safety
///
/// `writer` must point to a valid ArrowOdbcReader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_writer_free(writer: NonNull<ArrowOdbcWriter>) {
    drop(unsafe { Box::from_raw(writer.as_ptr()) });
}

/// Creates an Arrow ODBC writer instance.
///
/// Takes ownership of connection even in case of an error.
///
/// # Safety
///
/// * `connection` must point to a valid OdbcConnection. This function takes ownership of the
///   connection, even in case of an error. So The connection must not be freed explicitly
///   afterwards.
/// * `table_buf` must point to a valid utf-8 string
/// * `table_len` describes the len of `table_buf` in bytes.
/// * `schema` pointer to an arrow schema.
/// * `writer_out` in case of success this will point to an instance of `ArrowOdbcWriter`. Ownership
///   is transferred to the caller.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_writer_make(
    mut connection: NonNull<ArrowOdbcConnection>,
    table_buf: *const u8,
    table_len: usize,
    chunk_size: usize,
    schema: *const c_void,
    writer_out: *mut *mut ArrowOdbcWriter,
) -> *mut ArrowOdbcError {
    let connection = unsafe { connection.as_mut().take() };

    let table = unsafe { slice::from_raw_parts(table_buf, table_len) };
    let table = str::from_utf8(table).unwrap();

    let schema = schema as *const FFI_ArrowSchema;
    let schema: Schema = try_!(unsafe { &*schema }.try_into());

    let writer = try_!(OdbcWriter::from_connection(
        connection, &schema, table, chunk_size
    ));
    let writer_ptr = Box::into_raw(Box::new(ArrowOdbcWriter(writer)));
    unsafe {
        *writer_out = writer_ptr;
    }

    null_mut() // Ok(())
}

/// # Safety
///
/// * `writer` must be valid non-null writer, allocated by [`arrow_odbc_writer_make`].
/// * `batch` must be a valid pointer to an arrow batch
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_writer_write_batch(
    mut writer: NonNull<ArrowOdbcWriter>,
    array_ptr: *mut c_void,
    schema_ptr: *mut c_void,
) -> *mut ArrowOdbcError {
    let array_ptr = array_ptr as *mut FFI_ArrowArray;
    let schema_ptr = schema_ptr as *mut FFI_ArrowSchema;
    let array = unsafe { ptr::replace(array_ptr, FFI_ArrowArray::empty()) };
    let schema = unsafe { ptr::replace(schema_ptr, FFI_ArrowSchema::empty()) };

    // Dereference batch
    let array_data = try_!(unsafe { from_ffi(array, &schema) });
    let struct_array = StructArray::from(array_data);
    let record_batch = RecordBatch::from(&struct_array);

    // Dereference writer
    let writer = unsafe { &mut writer.as_mut().0 };

    try_!(writer.write_batch(&record_batch));
    null_mut() // Ok(())
}

/// # Safety
///
/// * `writer` must be valid non-null writer, allocated by [`arrow_odbc_writer_make`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_writer_flush(
    mut writer: NonNull<ArrowOdbcWriter>,
) -> *mut ArrowOdbcError {
    // Dereference writer
    let writer = unsafe { &mut writer.as_mut().0 };

    try_!(writer.flush());
    null_mut()
}
