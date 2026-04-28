from cffi import FFI

from .arrow_odbc import ffi, lib


class Error(Exception):
    """
    An error emmited by the arrow-odbc-py bindings.
    """

    def __init__(self, handle: "FFI.CData"):
        raw = ffi.string(lib.arrow_odbc_error_message(handle))
        # ffi.string returns bytes or str. For us it will always return bytes.3
        assert isinstance(raw, bytes)
        message = raw.decode("utf-8")
        lib.arrow_odbc_error_free(handle)
        super().__init__(message)

    def message(self) -> str:
        """
        A string describing the error.
        """
        assert isinstance(self.args[0], str)
        return self.args[0]


def raise_on_error(error_out: "FFI.CData"):
    """
    Raises if the argument points to an error
    """
    if error_out != ffi.NULL:
        raise Error(error_out)
