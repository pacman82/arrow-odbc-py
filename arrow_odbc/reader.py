from typing import Optional

from pyarrow.cffi import ffi as arrow_ffi # type: ignore
from pyarrow import DataType, Array # type: ignore

from .arrow_odbc import ffi, lib
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

        # Must keep connection alive, for the lifetime of the reader
        self.handle = handle
        # Use this member to cache the schema, since it is constant for all
        # batches.
        self.schema_ = None

    def __del__(self):
        # Free the resources associated with this handle.
        lib.arrow_odbc_reader_free(self.handle)

    def __iter__(self):
        # Implement iterable protocol so reader can be used in for loops.
        return self

    def __next__(self):
        # Implment iterator protocol

        # In case of an error this is going to be a non null handle to the error
        array_out = ffi.new("void **")
        schema_out = ffi.new("void **")
        error = lib.arrow_odbc_reader_next(self.handle, array_out, schema_out)
        raise_on_error(error)

        if array_out[0] == ffi.NULL:
            raise StopIteration()
        else:
            array_ptr = int(ffi.cast("uintptr_t", array_out[0]))
            schema_ptr = int(ffi.cast("uintptr_t", schema_out[0]))
            return Array._import_from_c(array_ptr, schema_ptr)

    def schema(self):
        """
        The arrow schema of the batches returned by this reader.
        """
        if self.schema_ is None:
            # https://github.com/apache/arrow/blob/5ead37593472c42f61c76396dde7dcb8954bde70/python/pyarrow/tests/test_cffi.py
            schema_out = arrow_ffi.new("struct ArrowSchema *")
            error = lib.arrow_odbc_reader_schema(self.handle, schema_out)
            raise_on_error(error)
            ptr_schema = int(ffi.cast("uintptr_t", schema_out))
            self.schema_ = DataType._import_from_c(ptr_schema)
        return self.schema_


def read_arrow_batches_from_odbc(
    query: str, batch_size: int, connection_string: str
) -> Optional[BatchReader]:
    """
    Execute the query and read the result as an iterator over Arrow batches.

    In case the query does not produce a result set (e.g. in case of an INSERT
    statement), None is returned instead of a BatchReader.

    To find out your connection string try:
    <https://www.connectionstrings.com/>
    """

    query_bytes = query.encode("utf-8")

    connection_string_bytes = connection_string.encode("utf-8")

    connection_out = ffi.new("OdbcConnection **")

    # Open connection to ODBC Data Source
    error = lib.arrow_odbc_connect_with_connection_string(
        connection_string_bytes, len(connection_string_bytes), connection_out
    )
    # See if we connected successfully and return an error if not
    raise_on_error(error)

    reader_out = ffi.new("ArrowOdbcReader **")

    connection = connection_out[0]
    error = lib.arrow_odbc_reader_make(
        connection, query_bytes, len(query_bytes), batch_size, reader_out
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
