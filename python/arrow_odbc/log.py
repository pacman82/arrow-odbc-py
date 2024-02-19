from .arrow_odbc import lib  # type: ignore
from .error import raise_on_error


def log_to_stderr(level: int = 1):
    """
    Activate logging from native code directly to standard error. In particular these logs contain
    diagnostic information emitted by ODBC. Call this method only once in your application

    :param level: Specifies the log level with which the standard error logger in rust is
        initialized.

        * 0 - Error
        * 1 - Warning,
        * 2 - Info
        * 3 - Debug
        * 4 - Trace

        All diagnostics emitted by ODBC are usually warning. In case of an exeception multiple
        records with severity error could also be emitted.
    """
    error = lib.arrow_odbc_log_to_stderr(level)
    raise_on_error(error)
