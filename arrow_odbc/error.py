from ._arrow_odbc_c import lib, ffi # type: ignore

class Error(Exception):
    """
    An error emmited by the ODBC API.
    """

    def __init__(self, handle):
        self.handle = handle

    def __del__(self):
        lib.arrow_odbc_error_free(self.handle)

    def message(self) -> str:
        return ffi.string(lib.arrow_odbc_error_message(self.handle)).decode("utf-8")

    def __str__(self) -> str:
        return self.message()


def make_error_out():
    """
    Pass the result into function the C-binding function calls in order to raise
    on errors.
    """

    # In case of an error this is going to be a non null handle to the error
    return ffi.new("ArrowOdbcError **")


def raise_on_error(error_out):
    """
    Raises if the argument points to an error
    """
    if error_out[0] != ffi.NULL:
            raise Error(error_out[0])