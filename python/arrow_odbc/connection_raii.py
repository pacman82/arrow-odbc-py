from typing import Any, Optional

from cffi import FFI

from .arrow_odbc import lib, ffi  # type: ignore
from .buffer import to_bytes_and_len
from .error import raise_on_error


class ConnectionRaii:
    """
    A strong reference to an ODBC connection.

    RAII stands for Resource Acquisition Is Initialization. It is an idiom stemming from the C++
    programming language that ensures resources are properly released when they are no longer
    needed. We use it here to indicate that the sole purpose of this class is to manage the lifetime
    of a strong refrence to an ODBC connection.

    The user of this library should not interact with this class directly, but rather use the
    ``Connection`` class in the ``arrow_odbc.connect`` module.
    """

    def __init__(self, handle: Any) -> None:
        self.handle = handle

    def arrow_odbc_connection(self) -> Any:
        """
        Give access to the inner ArrowOdbcConnection handle
        """
        return self.handle

    def __del__(self):
        if self.handle:
            # Free the resources associated with this handle.
            lib.arrow_odbc_connection_free(self.handle)

    @classmethod
    def connect(
        cls,
        connection_string: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        login_timeout_sec: Optional[int] = None,
        packet_size: Optional[int] = None,
    ) -> "ConnectionRaii":
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
        return ConnectionRaii(handle=handle)
