from .arrow_odbc import lib  # type: ignore
from .error import raise_on_error


def enable_odbc_connection_pooling():
    """
    Activates the connection pooling of the ODBC driver manager for the entire process. Best called
    before creating the ODBC environment, i.e. before you first insert or read rows with arrow-odbc.
    This is useful in scenarios there you frequently read or write rows and the overhead of creating
    a connection for each query is significant.
    """
    error = lib.arrow_odbc_enable_connection_pooling()
    raise_on_error(error)
