from typing import Optional

from ._arrow_odbc_c import ffi, lib
from ._arrow_odbc_c import lib  # type: ignore
from .error import make_error_out, raise_on_error


class BatchReader:
    """
    Iterates over Arrow batches from an ODBC data source
    """

    def __init__(self, handle):
        # Must keep connection alive, for the lifetime of the reader
        self.handle = handle

    def __del__(self):
        lib.arrow_odbc_reader_free(self.handle)

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration()


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

    # In case of an error this is going to be a non null handle to the error
    error_out = make_error_out()

    connection_string_bytes = connection_string.encode("utf-8")

    # In case of an error this is going to be a non null handle to the error
    error_out = make_error_out()

    # Open connection to ODBC Data Source
    connection = lib.arrow_odbc_connect_with_connection_string(
        connection_string_bytes, len(connection_string_bytes), error_out
    )
    # See if we connected successfully and return an error if not
    raise_on_error(error_out)

    reader = lib.arrow_odbc_reader_make(
        connection, query_bytes, len(query_bytes), batch_size, error_out
    )

    # See if we managed to execute the query successfully and return an
    # error if not
    raise_on_error(error_out)

    if reader == ffi.NULL:
        # The query ran successfully but did not produce a result set
        return None
    else:
        return BatchReader(reader)
