mod arrow_odbc_reader;

use std::{
    ffi::c_void,
    mem::swap,
    os::raw::c_int,
    ptr::{NonNull, null_mut},
    slice, str,
    sync::Arc,
};

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_odbc::{OdbcReaderBuilder, TextEncoding};

use crate::{ArrowOdbcConnection, ArrowOdbcError, parameter::ArrowOdbcParameter, try_};

pub use self::arrow_odbc_reader::ArrowOdbcReader;

/// Creates an Arrow ODBC reader instance.
///
/// Executes the SQL Query and moves the reader into cursor state.
///
/// # Safety
///
/// * `reader` must point to a valid reader in empty state.
/// * `connection` must point to a valid OdbcConnection. This function takes ownership of the
///   connection, even in case of an error. So The connection must not be freed explicitly
///   afterwards.
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
pub unsafe extern "C" fn arrow_odbc_reader_query(
    mut reader: NonNull<ArrowOdbcReader>,
    mut connection: NonNull<ArrowOdbcConnection>,
    query_buf: *const u8,
    query_len: usize,
    parameters: *const *mut ArrowOdbcParameter,
    parameters_len: usize,
    query_timeout_sec: *const usize,
) -> *mut ArrowOdbcError {
    let connection = unsafe { connection.as_mut() }.take();
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

/// Creates an empty Arrow ODBC reader instance. Useful for passing ownership of the reader in
/// Python code. The previous owner can use this to express the move by holding an empty instance.
///
/// # Parameters
///
/// * `reader_out` will point to an instance of `ArrowOdbcReader`. Ownership is transferred to the
///   caller.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_reader_make(reader_out: *mut *mut ArrowOdbcReader) {
    let reader = ArrowOdbcReader::empty();
    unsafe { *reader_out = Box::into_raw(Box::new(reader)) };
}

/// Frees the resources associated with an ArrowOdbcReader
///
/// # Safety
///
/// `reader` must point to a valid ArrowOdbcReader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_reader_free(reader: NonNull<ArrowOdbcReader>) {
    drop(unsafe { Box::from_raw(reader.as_ptr()) });
}

/// # Safety
///
/// * `reader` must be valid non-null reader, allocated by [`arrow_odbc_reader_make`].
/// * `array` and `schema` must both point to valid, but unitialized memory. The memory must be
///   allocated in the python code, so it can also be deallocated there and the python part can take
///   ownership of the whole thing.
/// * In case an error is returned `array` and `schema` remain unchanged.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_reader_next(
    mut reader: NonNull<ArrowOdbcReader>,
    array: *mut c_void,
    schema: *mut c_void,
    has_next_out: *mut c_int,
) -> *mut ArrowOdbcError {
    let schema = schema as *mut FFI_ArrowSchema;
    let array = array as *mut FFI_ArrowArray;

    // In case of an error fail early, before we change the output paramters.
    let batch = try_!(unsafe { reader.as_mut().next_batch() });

    if let Some((mut ffi_array, mut ffi_schema)) = batch {
        // Create two empty instances, so array and schema now point to valid instances.
        unsafe { *array = FFI_ArrowArray::empty() };
        unsafe { *schema = FFI_ArrowSchema::empty() };
        // Now that the instances are valid it safe to use them as references rather than pointers
        // (references must always be valid)
        let array = unsafe { &mut *array };
        let schema = unsafe { &mut *schema };

        swap(array, &mut ffi_array);
        swap(schema, &mut ffi_schema);

        unsafe { *has_next_out = 1 };
    } else {
        unsafe { *has_next_out = 0 };
    }
    null_mut()
}

/// # Safety
///
/// * `reader` must point to a valid non-null reader, allocated by [`arrow_odbc_reader_make`].
/// * `has_more_results` must point to a valid boolean.
/// * `schema`: Optional input arrow schema. NULL means no input schema is supplied. Should a
///   schema be supplied `schema` Rust will take ownership of it an the `schema` will be
///   overwritten with an empty one. This means the Python code, must only deallocate the memory
///   directly pointed to by `schema`, but not freeing the resources of the passed schema.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_reader_more_results(
    mut reader: NonNull<ArrowOdbcReader>,
    has_more_results: *mut bool,
) -> *mut ArrowOdbcError {
    // Move cursor to the next result set.
    unsafe { *has_more_results = try_!(reader.as_mut().more_results()) };
    null_mut()
}

/// # Safety
///
/// * `reader` must point to a valid non-null reader, allocated by [`arrow_odbc_reader_make`].
/// * `has_more_results` must point to a valid boolean.
/// * `schema`: Optional input arrow schema. NULL means no input schema is supplied. Should a
///   schema be supplied `schema` Rust will take ownership of it an the `schema` will be
///   overwritten with an empty one. This means the Python code, must only deallocate the memory
///   directly pointed to by `schema`, but not freeing the resources of the passed schema.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_reader_bind_buffers(
    mut reader: NonNull<ArrowOdbcReader>,
    max_num_rows_per_batch: usize,
    max_bytes_per_batch: usize,
    max_text_size: usize,
    max_binary_size: usize,
    fallibale_allocations: bool,
    fetch_concurrently: bool,
    payload_text_encoding: u8,
    schema: *mut c_void,
) -> *mut ArrowOdbcError {
    let schema = unsafe { take_schema(schema) };

    let reader_builder = reader_builder_from_c_args(
        max_text_size,
        max_binary_size,
        max_num_rows_per_batch,
        max_bytes_per_batch,
        fallibale_allocations,
        payload_text_encoding,
        schema,
    );
    // Move cursor to the next result set.
    try_!(unsafe { reader.as_mut() }.promote_to_reader(reader_builder));

    if fetch_concurrently {
        try_!(unsafe { reader.as_mut() }.into_concurrent());
    }

    null_mut()
}

/// Retrieve the associated schema from a reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_reader_schema(
    mut reader: NonNull<ArrowOdbcReader>,
    out_schema: *mut c_void,
) -> *mut ArrowOdbcError {
    let out_schema = out_schema as *mut FFI_ArrowSchema;

    let schema_ffi = try_!(unsafe { reader.as_mut() }.schema());
    unsafe { *out_schema = schema_ffi };
    null_mut()
}

/// # Safety
///
/// * `reader` must point to a valid non-null reader, allocated by [`arrow_odbc_reader_make`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_reader_into_concurrent(
    mut reader: NonNull<ArrowOdbcReader>,
) -> *mut ArrowOdbcError {
    try_!(unsafe { reader.as_mut() }.into_concurrent());
    null_mut()
}

fn into_text_encoding(encoding: u8) -> TextEncoding {
    match encoding {
        0 => TextEncoding::Auto,
        1 => TextEncoding::Utf8,
        2 => TextEncoding::Utf16,
        _ => panic!("Python side of arrow odbc must only pass values 0, 1 or 2 for encoding."),
    }
}

fn reader_builder_from_c_args(
    max_text_size: usize,
    max_binary_size: usize,
    max_num_rows_per_batch: usize,
    max_bytes_per_batch: usize,
    fallibale_allocations: bool,
    payload_text_encoding: u8,
    schema: Option<FFI_ArrowSchema>,
) -> OdbcReaderBuilder {
    let mut builder = OdbcReaderBuilder::new();
    builder
        .with_fallibale_allocations(fallibale_allocations)
        .with_max_num_rows_per_batch(max_num_rows_per_batch)
        .with_max_bytes_per_batch(if max_bytes_per_batch == 0 {
            usize::MAX
        } else {
            max_bytes_per_batch
        });
    if max_text_size != 0 {
        builder.with_max_text_size(max_text_size);
    };
    if max_binary_size != 0 {
        builder.with_max_binary_size(max_binary_size);
    };
    if let Some(ffi_schema) = schema {
        builder.with_schema(Arc::new((&ffi_schema).try_into().unwrap()));
    }
    builder.with_payload_text_encoding(into_text_encoding(payload_text_encoding));
    builder
}

/// Takes ownership of the supplied schema. Evaluates to `None` if Schema is NULL. The memory
/// pointed to be `schema` must still be cleared by the caller. Since it is not known how `schema`
/// has been allocated. Yet its contend have been replaced with that of an empty schema.
unsafe fn take_schema(schema: *mut c_void) -> Option<FFI_ArrowSchema> {
    if schema.is_null() {
        None
    } else {
        let schema = schema as *mut FFI_ArrowSchema;
        let schema = unsafe { &mut *schema };
        let mut tmp_schema = FFI_ArrowSchema::empty();
        swap(schema, &mut tmp_schema);
        Some(tmp_schema)
    }
}
