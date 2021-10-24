from pytest import raises

from arrow_odbc import Connection

def test_should_report_error_on_invalid_connection_string():
    '''
    We want to forward the original ODBC errors to the end user. Of course foo is not a valid
    connection string. Therefor we want to see the creation of this connection fail.
    '''
    with raises(Exception):
        connection = Connection.from_connection_string("foo")
