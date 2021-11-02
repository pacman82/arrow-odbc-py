from __future__ import annotations

from typing import Any, Optional

from ._arrow_odbc_c import ffi, lib
from .error import make_error_out, raise_on_error
from .reader import BatchReader


class Connection:
    """
    An ODBC connection. Can be used to create Arrow Odbc readers.
    """

    def __init__(self, native_connection):
        self.native_connection = native_connection

    @classmethod
    def from_connection_string(cls: Any, connection_string: str) -> Connection:
        """
        Allocates a connection handle and establishes connections to a driver
        and a data source.

        To find out your connection string try:
        <https://www.connectionstrings.com/>
        """
        connection_string_bytes = connection_string.encode("utf-8")

        # In case of an error this is going to be a non null handle to the error
        error_out = make_error_out()

        # Open connection to ODBC Data Source
        native_connection = lib.arrow_odbc_connect_with_connection_string(
            connection_string_bytes, len(connection_string_bytes), error_out
        )
        # See if we connected successfully and return an error if not
        raise_on_error(error_out)

        # Create self
        self = cls(native_connection)
        return self

    def read_arrow_batches(self, query: str, batch_size=100) -> Optional[BatchReader]:
        """
        Execute the query and read the result as an iterator over Arrow batches.

        In case the query does not produce a result set (e.g. in case of an INSERT statement), None
        is returned instead of a BatchReader.
        """

        query_bytes = query.encode("utf-8")

        # In case of an error this is going to be a non null handle to the error
        error_out = make_error_out()

        reader = lib.arrow_odbc_reader_make(
            self.native_connection, query_bytes, len(query_bytes), batch_size, error_out
        )

        # See if we managed to execute the query successfully and return an
        # error if not
        raise_on_error(error_out)

        if reader == ffi.NULL:
            # The query ran successfully but did not produce a result set
            return None
        else:
            return BatchReader(reader)
