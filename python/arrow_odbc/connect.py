from typing import Any, Optional, Tuple
from cffi.api import FFI  # type: ignore

from pyarrow.cffi import ffi as arrow_ffi  # type: ignore

from .arrow_odbc import ffi, lib  # type: ignore
from arrow_odbc.error import raise_on_error


def to_bytes_and_len(value: Optional[str]) -> Tuple[bytes, int]:
    if value is None:
        value_bytes = FFI.NULL
        value_len = 0
    else:
        value_bytes = value.encode("utf-8")
        value_len = len(value)

    return (value_bytes, value_len)


def connect_to_database(
    connection_string: str,
    user: Optional[str],
    password: Optional[str],
    login_timeout_sec: Optional[int],
    packet_size: Optional[int],
) -> Any:
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

    connection_out = ffi.new("OdbcConnection **")

    # Open connection to ODBC Data Source
    error = lib.arrow_odbc_connect_with_connection_string(
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
    # Dereference output pointer. This gives us an `OdbcConnection *`
    return connection_out[0]
