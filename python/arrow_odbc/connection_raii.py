# pyright: reportAttributeAccessIssue=false

from collections.abc import Sequence
from typing import Any

from cffi import FFI

from .arrow_odbc import ffi, lib  # type: ignore
from .buffer import to_bytes_and_len
from .error import raise_on_error
from .reader import BatchReaderRaii
from .text_encoding import TextEncoding


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

    def set_autocommit(self, autocommit: bool) -> None:
        error = lib.arrow_odbc_connection_set_autocommit(self.handle, autocommit)
        raise_on_error(error)

    def rollback(self) -> None:
        error = lib.arrow_odbc_connection_rollback(self.handle)
        raise_on_error(error)

    def commit(self) -> None:
        error = lib.arrow_odbc_connection_commit(self.handle)
        raise_on_error(error)

    def query(
        self,
        reader: BatchReaderRaii,
        query: str,
        parameters: Sequence[str | None] | None,
        text_encoding: TextEncoding,
        query_timeout_sec: int | None,
    ) -> None:
        query_bytes = query.encode("utf-8")

        if parameters is None:
            parameters_array = FFI.NULL
            parameters_len = 0
            encoded_parameters = []
        else:
            # Check precondition in order to save users some debugging, in case they directly pass a
            # non-string argument and do not use a type linter.
            if not all([p is None or hasattr(p, "encode") for p in parameters]):
                raise TypeError(
                    "read_arrow_batches_from_odbc only supports string arguments for SQL query "
                    "parameters"
                )

            parameters_array = ffi.new("ArrowOdbcParameter *[]", len(parameters))
            parameters_len = len(parameters)
            # Must be kept alive. Within Rust code we only allocate an additional indicator the
            # string payload is just referenced.
            encoded_parameters = [to_bytes_and_len(p) for p in parameters]

        text_encoding_int = text_encoding.value

        for p_index in range(0, parameters_len):
            (p_bytes, p_len) = encoded_parameters[p_index]
            parameters_array[p_index] = lib.arrow_odbc_parameter_string_make(
                p_bytes, p_len, text_encoding_int
            )

        if query_timeout_sec is None:
            query_timeout_sec_pointer = ffi.NULL
        else:
            query_timeout_sec_pointer = ffi.new("uintptr_t *")
            query_timeout_sec_pointer[0] = query_timeout_sec

        error = lib.arrow_odbc_reader_query(
            reader.handle,
            self.handle,
            query_bytes,
            len(query_bytes),
            parameters_array,
            parameters_len,
            query_timeout_sec_pointer,
        )

        raise_on_error(error)

    def __del__(self):
        if self.handle:
            # Free the resources associated with this handle.
            lib.arrow_odbc_connection_free(self.handle)

    @classmethod
    def connect(
        cls,
        connection_string: str,
        user: str | None,
        password: str | None,
        login_timeout_sec: int | None,
        packet_size: int | None,
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
