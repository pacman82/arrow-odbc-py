from .arrow_odbc import lib, ffi  # type: ignore


class Error(Exception):
    """
    An error emmited by the arrow-odbc-py bindings.
    """

    def __init__(self, handle):
        self.handle = handle

    def __del__(self):
        lib.arrow_odbc_error_free(self.handle)

    def message(self) -> str:
        """
        A string describing the error.
        """
        return ffi.string(lib.arrow_odbc_error_message(self.handle)).decode("utf-8")

    def __str__(self) -> str:
        return self.message()


def raise_on_error(error_out):
    """
    Raises if the argument points to an error
    """
    if error_out != ffi.NULL:
        raise Error(error_out)
