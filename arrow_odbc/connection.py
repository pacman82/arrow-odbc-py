from typing import Type, TypeVar

from ._arrow_odbc_c import lib, ffi
from .error import OdbcError

T = TypeVar("T")


class Connection:
    """
    An ODBC connection. Can be used to create Arrow Odbc readers.
    """

    def __init__(self):
        pass

    @classmethod
    def from_connection_string(cls: Type[T], connection_string: str) -> T:
        """
        Allocates a connection handle and establishes connections to a driver
        and a data source.

        To find out your connection string try:
        <https://www.connectionstrings.com/>
        """
        connection_string_bytes = connection_string.encode("utf-8")

        # In case of an error this is going to be a non null handle to the error
        error_out = ffi.new("Error **")

        native_connection = lib.arrow_odbc_connect_with_connection_string(
            connection_string_bytes, len(connection_string_bytes), error_out
        )
        if native_connection == ffi.NULL:
            raise OdbcError(error_out[0])
        self = cls()
        return self
