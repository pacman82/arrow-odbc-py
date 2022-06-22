use std::{
    ffi::c_void,
    ptr::{null_mut, NonNull},
    slice, str,
};

use arrow_odbc::{odbc_api::StatementConnection, OdbcWriter, arrow::{ffi::{FFI_ArrowSchema, FFI_ArrowArray, ArrowArray}, datatypes::Schema, record_batch::RecordBatch, array::StructArray}};

use crate::{ArrowOdbcError, OdbcConnection, try_};

/// Opaque type holding all the state associated with an ODBC writer implementation in Rust. This
/// type also has ownership of the ODBC Connection handle.
pub struct ArrowOdbcWriter(OdbcWriter<StatementConnection<'static>>);

/// Frees the resources associated with an ArrowOdbcWriter
///
/// # Safety
///
/// `writer` must point to a valid ArrowOdbcReader.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_writer_free(writer: NonNull<ArrowOdbcWriter>) {
    Box::from_raw(writer.as_ptr());
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
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_writer_make(
    connection: NonNull<OdbcConnection>,
    table_buf: *const u8,
    table_len: usize,
    chunk_size: usize,
    schema: *const c_void,
    writer_out: *mut *mut ArrowOdbcWriter,
) -> *mut ArrowOdbcError {
    let connection = *Box::from_raw(connection.as_ptr());
    let connection = connection.0;

    let table = slice::from_raw_parts(table_buf, table_len);
    let table = str::from_utf8(table).unwrap();

    let schema = schema as * const FFI_ArrowSchema;
    let schema: Schema = try_!((&*schema).try_into());

    let writer = try_!(OdbcWriter::from_connection(connection, &schema, table, chunk_size));
    *writer_out = Box::into_raw(Box::new(ArrowOdbcWriter(writer)));

    null_mut() // Ok(())
}

/// # Safety
///
/// * `writer` must be valid non-null writer, allocated by [`arrow_odbc_writer_make`].
/// * `batch` must be a valid pointer to an arrow batch
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_writer_write_batch(
    mut writer: NonNull<ArrowOdbcWriter>,
    array_ptr: *mut c_void,
    schema_ptr: *mut c_void,
) -> *mut ArrowOdbcError {
    // Dereference batch
    let ffi_array_ptr = array_ptr as * mut FFI_ArrowArray;
    let ffi_schema_ptr = schema_ptr as * mut FFI_ArrowSchema;
    let arrow_array = ArrowArray::try_from_raw(ffi_array_ptr, ffi_schema_ptr);

    // Dereference writer
    let writer = &mut writer.as_mut().0;

    null_mut() // Ok(())
}