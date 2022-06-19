from typing import Optional
from arrow_odbc.connect import connect_to_database

from ._native import ffi, lib  # type: ignore

def insert_into_table(
    connection_string: str, user: Optional[str] = None, password: Optional[str] = None
):
    """
    Consume the batches in the reader and insert them into a table on the database.

    :param connection_string: ODBC Connection string used to connect to the data source. To find a
        connection string for your data source try https://www.connectionstrings.com/.
    :param user: Allows for specifying the user seperatly from the connection string if it is not
        already part of it. The value will eventually be escaped and attached to the connection
        string as `UID`.
    :param password: Allows for specifying the password seperatly from the connection string if it
        is not already part of it. The value will eventually be escaped and attached to the
        connection string as `PWD`.
    """
    connection_out = connect_to_database(connection_string, user, password)
    connection = connection_out[0]

    # Connecting to the database has been successful. Note that connection_out does not truly take
    # ownership of the connection. If it runs out of scope (e.g. due to a raised exception) the
    # connection would not be closed and its associated resources would not be freed.

    lib.arrow_odbc_insert_into_table(connection)

    pass
