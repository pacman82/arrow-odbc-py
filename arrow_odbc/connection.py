from __future__ import annotations

from typing import Any
from ._arrow_odbc_c import lib, ffi
from .error import Error

class Connection:
    """
    An ODBC connection. Can be used to create Arrow Odbc readers.
    """

    def __init__(self, native_connection):
        self.native_connection = native_connection

    def __del__(self):
        lib.odbc_connection_free(self.native_connection)

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
        error_out = ffi.new("ArrowOdbcError **")

        # Open connection to ODBC Data Source
        native_connection = lib.arrow_odbc_connect_with_connection_string(
            connection_string_bytes, len(connection_string_bytes), error_out
        )
        # See if we connected successfully and return an error if not
        if error_out[0] != ffi.NULL:
            raise Error(error_out[0])

        # Create self
        self = cls(native_connection)
        return self

    def read_arrow_batches(self, query: str, batch_size=100):
        query_bytes = query.encode("utf-8")

        # In case of an error this is going to be a non null handle to the error
        error_out = ffi.new("ArrowOdbcError **")

        lib.arrow_odbc_reader_make(
            self.native_connection, query_bytes, len(query_bytes), batch_size, error_out
        )

        # See if we connected successfully and return an error if not
        if error_out[0] != ffi.NULL:
            raise Error(error_out[0])
