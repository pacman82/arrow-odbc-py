from pytest import raises

from arrow_odbc import Connection, OdbcError


def test_should_report_error_on_invalid_connection_string():
    """
    We want to forward the original ODBC errors to the end user. Of course foo
    is not a valid connection string. Therefore we want to see the creation of
    this connection fail, but with a nice error.
    """
    with raises(
        OdbcError, match="Data source name not found and no default driver specified"
    ):
        connection = Connection.from_connection_string("foo")
