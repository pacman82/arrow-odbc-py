from pytest import raises

from arrow_odbc import Connection, Error

MSSQL = "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;"

def test_should_report_error_on_invalid_connection_string():
    """
    We want to forward the original ODBC errors to the end user. Of course foo
    is not a valid connection string. Therefore we want to see the creation of
    this connection fail, but with a nice error.
    """
    with raises(
        Error, match="Data source name not found and no default driver specified"
    ):
        connection = Connection.from_connection_string("foo")


def test_should_report_error_on_invalid_query():
    """
    We want the user to know why a query failed.
    """
    query = "SELECT * FROM Foo" # This table does not exist in the datasource

    connection = Connection.from_connection_string(MSSQL)
    with raises(
        Error, match="Invalid object name 'Foo'"
    ):
        connection.read_arrow_batches(query, batch_size=100)
