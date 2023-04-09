mod arrow_odbc_reader;

use std::{
    ffi::c_void,
    mem::swap,
    os::raw::c_int,
    ptr::{null_mut, NonNull},
    slice, str,
};

use arrow::ffi::{ArrowArray, ArrowArrayRef, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_odbc::{
    arrow::array::{Array, StructArray},
    BufferAllocationOptions,
};

use crate::{parameter::ArrowOdbcParameter, try_, ArrowOdbcError, OdbcConnection};

pub use self::arrow_odbc_reader::ArrowOdbcReader;

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
/// * `reader_out` in case of success this will point to an instance of `ArrowOdbcReader`.
///   Ownership is transferred to the caller.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_make(
    connection: NonNull<OdbcConnection>,
    query_buf: *const u8,
    query_len: usize,
    batch_size: usize,
    parameters: *const *mut ArrowOdbcParameter,
    parameters_len: usize,
    max_text_size: usize,
    max_binary_size: usize,
    fallibale_allocations: bool,
    reader_out: *mut *mut ArrowOdbcReader,
) -> *mut ArrowOdbcError {
    let query = slice::from_raw_parts(query_buf, query_len);
    let query = str::from_utf8(query).unwrap();

    let connection = *Box::from_raw(connection.as_ptr());

    let parameters = if parameters.is_null() {
        Vec::new()
    } else {
        slice::from_raw_parts(parameters, parameters_len)
            .iter()
            .map(|&p| Box::from_raw(p).unwrap())
            .collect()
    };

    let max_text_size = if max_text_size == 0 {
        None
    } else {
        Some(max_text_size)
    };

    let max_binary_size = if max_binary_size == 0 {
        None
    } else {
        Some(max_binary_size)
    };

    let buffer_allocation_options = BufferAllocationOptions {
        max_text_size,
        max_binary_size,
        fallibale_allocations,
    };

    let maybe_cursor = try_!(connection.0.into_cursor(query, &parameters[..]));
    if let Some(cursor) = maybe_cursor {
        let reader = try_!(ArrowOdbcReader::new(
            cursor,
            batch_size,
            buffer_allocation_options
        ));
        *reader_out = Box::into_raw(Box::new(reader))
    } else {
        *reader_out = null_mut()
    }
    null_mut() // Ok(())
}

/// Frees the resources associated with an ArrowOdbcReader
///
/// # Safety
///
/// `reader` must point to a valid ArrowOdbcReader.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_free(reader: NonNull<ArrowOdbcReader>) {
    drop(Box::from_raw(reader.as_ptr()));
}

/// # Safety
///
/// * `reader` must be valid non-null reader, allocated by [`arrow_odbc_reader_make`].
/// * `array` and `schema` must both point to valid, but unitialized memory. The memory must be
///   allocated in the python code, so it can also be deallocated there and the python part can take
///   ownership of the whole thing.
/// * In case an error is returned `array` and `schema` remain unchanged.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_next(
    mut reader: NonNull<ArrowOdbcReader>,
    array: *mut c_void,
    schema: *mut c_void,
    has_next_out: *mut c_int,
) -> *mut ArrowOdbcError {
    let schema = schema as *mut FFI_ArrowSchema;
    let array = array as *mut FFI_ArrowArray;

    if let Some(batch) = try_!(reader.as_mut().next_batch()) {
        // In case of an error fail early, before we change the output paramters.
        let struct_array: StructArray = batch.into();
        let arrow_array = try_!(ArrowArray::try_new(struct_array.data().clone()));

        // Create two empty instances, so array and schema now point to valid instances.
        *array = FFI_ArrowArray::empty();
        *schema = FFI_ArrowSchema::empty();
        // Now that the instances are valid it safe to use them as references rather than pointers
        // (references must always be valid)
        let array = &mut *array;
        let schema = &mut *schema;

        let array_data = arrow_array.to_data().unwrap();

        let mut ffi_array = FFI_ArrowArray::new(&array_data);
        let mut ffi_schema = FFI_ArrowSchema::try_from(array_data.data_type()).unwrap();

        swap(array, &mut ffi_array);
        swap(schema, &mut ffi_schema);

        *has_next_out = 1;
    } else {
        *has_next_out = 0;
    }
    null_mut()
}

/// # Safety
///
/// * `reader_in` must point to a valid non-null reader, allocated by [`arrow_odbc_reader_make`].
///   This function takes ownership of reader_in and will free the associated resources, even in
///   case of error. After the function call `reader_in` is invalid and must not be dereferenced.
/// * `reader_out` In case another result set exists, `reader_out` will point to a valid instance of
///   [`ArrowOdbcReader`]. Otherwise, it will point to `NULL`.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_more_results(
    reader_in: NonNull<ArrowOdbcReader>,
    reader_out: *mut *mut ArrowOdbcReader,
    batch_size: usize,
    max_text_size: usize,
    max_binary_size: usize,
    fallibale_allocations: bool,
) -> *mut ArrowOdbcError {
    // In case we return early, we want the caller to know that reader_out is invalid.
    *reader_out = null_mut();

    // Dereference reader and take ownership of it.
    let reader = Box::from_raw(reader_in.as_ptr());

    let max_text_size = if max_text_size == 0 {
        None
    } else {
        Some(max_text_size)
    };
    let max_binary_size = if max_binary_size == 0 {
        None
    } else {
        Some(max_binary_size)
    };
    let buffer_allocation_options = BufferAllocationOptions {
        max_text_size,
        max_binary_size,
        fallibale_allocations,
    };

    // Move cursor to the next result set.
    let next = try_!(reader.more_results(batch_size, buffer_allocation_options));

    if let Some(reader) = next {
        *reader_out = Box::into_raw(Box::new(reader));
    }

    null_mut()
}

/// Retrieve the associated schema from a reader.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_reader_schema(
    reader: NonNull<ArrowOdbcReader>,
    out_schema: *mut c_void,
) -> *mut ArrowOdbcError {
    let out_schema = out_schema as *mut FFI_ArrowSchema;

    let schema_ffi = try_!(reader.as_ref().schema());
    *out_schema = schema_ffi;
    null_mut()
}
