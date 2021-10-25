from ._arrow_odbc_c import lib, ffi


class OdbcError(Exception):
    """
    An error emmited by the ODBC API.
    """

    def __init__(self, handle):
        self.handle = handle

    def message(self) -> str:
        return ffi.string(lib.odbc_error_message(self.handle)).decode("utf-8")

    def __str__(self) -> str:
        return f"{self.message()}"

    def __del__(self):
        lib.odbc_error_free(self.handle)
