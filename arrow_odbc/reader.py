from typing import List, Optional, Tuple
from cffi.api import FFI  # type: ignore

from pyarrow.cffi import ffi as arrow_ffi  # type: ignore
from pyarrow import RecordBatch, Schema, Array

from arrow_odbc.connect import to_bytes_and_len, connect_to_database  # type: ignore

from ._native import ffi, lib  # type: ignore
from .error import raise_on_error


class BatchReader:
    """
    Iterates over Arrow batches from an ODBC data source
    """

    def __init__(self, handle):
        """
        Low level constructor, users should rather invoke
        `read_arrow_batches_from_odbc` in order to create instances of
        `BatchReader`.
        """

        # We take owners of the corresponding reader written in Rust and keep it alive until `self`
        # is deleted
        self.handle = handle
        # Expose schema as attribute
        # https://github.com/apache/arrow/blob/5ead37593472c42f61c76396dde7dcb8954bde70/python/pyarrow/tests/test_cffi.py
        schema_out = arrow_ffi.new("struct ArrowSchema *")
        error = lib.arrow_odbc_reader_schema(self.handle, schema_out)
        raise_on_error(error)
        ptr_schema = int(ffi.cast("uintptr_t", schema_out))
        self.schema = Schema._import_from_c(ptr_schema)

    def __del__(self):
        # Free the resources associated with this handle.
        lib.arrow_odbc_reader_free(self.handle)

    def __iter__(self):
        # Implement iterable protocol so reader can be used in for loops.
        return self

    def __next__(self) -> RecordBatch:
        # Implment iterator protocol

        # In case of an error this is going to be a non null handle to the error
        array = arrow_ffi.new("struct ArrowArray *")
        schema = arrow_ffi.new("struct ArrowSchema *")

        has_next_out = ffi.new("int*")

        error = lib.arrow_odbc_reader_next(self.handle, array, schema, has_next_out)
        raise_on_error(error)

        if has_next_out[0] == 0:
            raise StopIteration()
        else:
            array_ptr = int(ffi.cast("uintptr_t", array))
            schema_ptr = int(ffi.cast("uintptr_t", schema))
            struct_array = Array._import_from_c(array_ptr, schema_ptr)
            return RecordBatch.from_struct_array(struct_array)


def read_arrow_batches_from_odbc(
    query: str,
    batch_size: int,
    connection_string: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    parameters: Optional[List[Optional[str]]] = None,
    max_text_size: Optional[int] = None,
    max_binary_size: Optional[int] = None,
    falliable_allocations: bool = True,
) -> Optional[BatchReader]:
    """
    Execute the query and read the result as an iterator over Arrow batches.

    :param query: The SQL statement yielding the result set which is converted into arrow record
        batches.
    :param batch_size: The maxmium number rows within each batch.
    :param connection_string: ODBC Connection string used to connect to the data source. To find a
        connection string for your data source try https://www.connectionstrings.com/.
    :param user: Allows for specifying the user seperatly from the connection string if it is not
        already part of it. The value will eventually be escaped and attached to the connection
        string as `UID`.
    :param password: Allows for specifying the password seperatly from the connection string if it
        is not already part of it. The value will eventually be escaped and attached to the
        connection string as `PWD`.
    :param parameters: ODBC allows you to use a question mark as placeholder marker (``?``) for
        positional parameters. This argument takes a list of parameters those number must match the
        number of placholders in the SQL statement. Using this instead of literals helps you avoid
        SQL injections or may otherwise simplify your code. Currently all parameters are passed as
        VARCHAR strings. You can use `None` to pass `NULL`.
    :param max_text_size: An upper limit for the size of buffers bound to variadic text columns of
        the data source. This limit does not (directly) apply to the size of the created arrow
        buffers, but rather applies to the buffers used for the data in transit. Use this option if
        you have e.g. VARCHAR(MAX) fields in your database schema. In such a case without an upper
        limit, the ODBC driver of your data source is asked for the maximum size of an element, and
        is likely to answer with either 0 or a value which is way larger than any actual entry in
        the column If you can not adapt your database schema, this limit might be what you are
        looking for. On windows systems the size is double words (16Bit), as windows utilizes an
        UTF-16 encoding. So this translates to roughly the size in letters. On non windows systems
        this is the size in bytes and the datasource is assumed to utilize an UTF-8 encoding.
        ``None`` means no upper limit is set and the maximum element size, reported by ODBC is used
        to determine buffer sizes.
    :param max_binary_size: An upper limit for the size of buffers bound to variadic binary columns
        of the data source. This limit does not (directly) apply to the size of the created arrow
        buffers, but rather applies to the buffers used for the data in transit. Use this option if
        you have e.g. VARBINARY(MAX) fields in your database schema. In such a case without an upper
        limit, the ODBC driver of your data source is asked for the maximum size of an element, and
        is likely to answer with either 0 or a value which is way larger than any actual entry in
        the column. If you can not adapt your database schema, this limit might be what you are
        looking for. This is the maximum size in bytes of the binary column.
    :param falliable_allocations: If ``True`` an recoverable error is raised in case there is not
        enough memory to allocate the buffers. This option may incurr a performance penalty which
        scales with the batch size parameter (but not with the amount of actual data in the source).
        In case you can test your query against the schema you can safely set this to ``False``. The
        required memory will not depend on the amount of data in the data source. Default is
        ``True`` though, safety first.
    :return: In case the query does not produce a result set (e.g. in case of an INSERT statement),
        ``None`` is returned. Should the statement return a result set a ``BatchReader`` is
        returned, which implements the iterator protocol and iterates over individual arrow batches.
    """
    query_bytes = query.encode("utf-8")

    connection = connect_to_database(connection_string, user, password)

    # Connecting to the database has been successful. Note that connection does not truly take
    # ownership of the connection. If it runs out of scope (e.g. due to a raised exception) the
    # connection would not be closed and its associated resources would not be freed.
    # However, this is fine since everything from here on out until we call arrow_odbc_reader_make
    # is infalliable. arrow_odbc_reader_make will truly take ownership of the connection. Even if it
    # should fail, it will be closed correctly.

    if parameters is None:
        parameters_array = FFI.NULL
        parameters_len = 0
        encoded_parameters = []
    else:
        parameters_array = ffi.new("ArrowOdbcParameter *[]", len(parameters))
        parameters_len = len(parameters)
        # Must be kept alive. Within Rust code we only allocate an additional
        # indicator the string payload is just referenced.
        encoded_parameters = [to_bytes_and_len(p) for p in parameters]

    if max_text_size is None:
        max_text_size = 0

    if max_binary_size is None:
        max_binary_size = 0

    for p_index in range(0, parameters_len):
        (p_bytes, p_len) = encoded_parameters[p_index]
        parameters_array[p_index] = lib.arrow_odbc_parameter_string_make(p_bytes, p_len)

    reader_out = ffi.new("ArrowOdbcReader **")

    error = lib.arrow_odbc_reader_make(
        connection,
        query_bytes,
        len(query_bytes),
        batch_size,
        parameters_array,
        parameters_len,
        max_text_size,
        max_binary_size,
        falliable_allocations,
        reader_out,
    )

    # See if we managed to execute the query successfully and return an
    # error if not
    raise_on_error(error)

    reader = reader_out[0]
    if reader == ffi.NULL:
        # The query ran successfully but did not produce a result set
        return None
    else:
        return BatchReader(reader)
