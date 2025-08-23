from typing import Any, Optional, Tuple
from cffi.api import FFI  # type: ignore

from .arrow_odbc import ffi, lib  # type: ignore
from arrow_odbc.error import raise_on_error


def to_bytes_and_len(value: Optional[str]) -> Tuple[bytes, int]:
    if value is None:
        value_bytes = FFI.NULL
        value_len = 0
    else:
        value_bytes = value.encode("utf-8")
        value_len = len(value_bytes)

    return (value_bytes, value_len)


class Connection:
    """
    A strong reference to an ODBC connection.
    """
    def __init__(self, handle: Any) -> None:
        self.handle = handle

    def _arrow_odbc_connection(self) -> Any:
        """
        Give access to the inner ArrowOdbcConnection handle
        """
        return self.handle

    def __del__(self):
        if self.handle:
            # Free the resources associated with this handle.
            lib.arrow_odbc_connection_free(self.handle)


def connect(
    connection_string: str,
    user: Optional[str],
    password: Optional[str],
    login_timeout_sec: Optional[int],
    packet_size: Optional[int],
) -> Connection:
    """
    Opens a connection to an ODBC data source.

    In case you want to use connection pooling, call ``enable_odbc_connection_pooling()`` before
    calling this function.

    Example:

    .. code-block:: python

        from arrow_odbc import connect, enable_odbc_connection_pooling

        # Let the ODBC driver manager take care of connection pooling for us
        enable_odbc_connection_pooling()

        # Connect to the data source
        connection_string=
            "Driver={ODBC Driver 18 for SQL Server};" \
            "Server=localhost;" \
            "TrustServerCertificate=yes;"
        connection = connect(
            connection_string=connection_string,
            user="SA",
            password="My@Test@Password"
        )

    :param connection_string: ODBC Connection string used to connect to the data source. To find a
        connection string for your data source try https://www.connectionstrings.com/.
    :param user: Allows for specifying the user seperatly from the connection string if it is not
        already part of it. The value will eventually be escaped and attached to the connection
        string as `UID`.
    :param password: Allows for specifying the password seperatly from the connection string if it
        is not already part of it. The value will eventually be escaped and attached to the
        connection string as `PWD`.
    :param login_timeout_sec: Number of seconds to wait for a login request to complete before
        returning to the application. The default is driver-dependent. If ``0``, the timeout is
        disabled and a connection attempt will wait indefinitely. If the specified timeout exceeds
        the maximum login timeout in the data source, the driver substitutes that value and uses
        that instead.
    :param packet_size: Specifying the network packet size in bytes. Many ODBC drivers do not
        support this option. If the specified size exceeds the maximum packet size or is smaller
        than the minimum packet size, the driver substitutes that value and returns SQLSTATE 01S02
        (Option value changed).You may want to enable logging to standard error using
        ``log_to_stderr``.
    :return: A ``Connection`` is returned.
    """
    connection_string_bytes = connection_string.encode("utf-8")

    (user_bytes, user_len) = to_bytes_and_len(user)
    (password_bytes, password_len) = to_bytes_and_len(password)
    # We use a pointer to pass the login time, so NULL can represent None
    if login_timeout_sec is None:
        login_timeout_sec_ptr = FFI.NULL
    else:
        login_timeout_sec_ptr = ffi.new("uint32_t *")
        login_timeout_sec_ptr[0] = login_timeout_sec
    if packet_size is None:
        packet_size_ptr = FFI.NULL
    else:
        packet_size_ptr = ffi.new("uint32_t *")
        packet_size_ptr[0] = packet_size
    connection_out = ffi.new("ArrowOdbcConnection **")
    # Open connection to ODBC Data Source
    error = lib.arrow_odbc_connection_make(
        connection_string_bytes,
        len(connection_string_bytes),
        user_bytes,
        user_len,
        password_bytes,
        password_len,
        login_timeout_sec_ptr,
        packet_size_ptr,
        connection_out,
    )
    # See if we connected successfully and return an error if not
    raise_on_error(error)
    # Dereference output pointer. This gives us an `ArrowOdbcConnection *`. We take ownership of
    # the ArrowOdbcConnection and must take care to free it.
    handle = connection_out[0]
    return Connection(handle=handle)
