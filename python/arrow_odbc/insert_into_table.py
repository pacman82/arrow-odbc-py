from typing import Any

from .connect import connect


def insert_into_table(
    reader: Any,
    chunk_size: int,
    table: str,
    connection_string: str,
    user: str | None = None,
    password: str | None = None,
    login_timeout_sec: int | None = None,
    packet_size: int | None = None,
):
    """
    Consume the batches in the reader and insert them into a table on the database.

    Example:

    .. code-block:: python

        from arrow_odbc import insert_into_table
        import pyarrow as pa
        import pandas


        def dataframe_to_table(df):
            table = pa.Table.from_pandas(df)
            reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
            insert_into_table(
                connection_string=connection_string,
                user="SA",
                password="My@Test@Password",
                chunk_size=1000,
                table="MyTable",
                reader=reader,
            )

    :param reader: Reader is used to iterate over record batches. It must expose a `schema`
        attribute, referencing an Arrow schema. Each field in the schema must correspond to a
        column in the table with identical name. The iterator must yield individual arrow tables. In
        case a column name contains a non-alphanumeric character different from ``@``, ``$``,
        ``#``, or ``_``, the name will be escaped using double quotes. However if the name already
        is already escaped, i.e. it is enclosed in either rectangular brackets ([, ]), double quotes
        (") or backticks (`), it will be used as is. This is to allow for users to apply their own
        quoting if needed, e.g. in case reserved keywords are used as column names.
    :param chunk_size: Number of records to insert in each roundtrip to the database. Independent of
        batch size (i.e. number of rows in an individual record batch).
    :param table: Name of a database table to insert into. Used to generate the insert statement for
        the bulk writer.
    :param connection_string: ODBC Connection string used to connect to the data source. To find a
        connection string for your data source try https://www.connectionstrings.com/.
    :param user: Allows for specifying the user seperatly from the connection string if it is not
        already part of it. The value will eventually be escaped and attached to the connection
        string as `UID`.
    :param password: Allows for specifying the password seperatly from the connection string if it
        is not already part of it. The value will eventually be escaped and attached to the
        connection string as `PWD`.
    :param login_timeout_sec: Number of seconds to wait for a login request to complete before
        returning to the application. The default is driver-dependent. If ``0``, the timeout is
        disabled and a connection attempt will wait indefinitely. If the specified timeout exceeds
        the maximum login timeout in the data source, the driver substitutes that value and uses
        that instead.
    :param packet_size: Specifying the network packet size in bytes. Many ODBC drivers do not
        support this option. If the specified size exceeds the maximum packet size or is smaller
        than the minimum packet size, the driver substitutes that value and returns SQLSTATE 01S02
        (Option value changed).You may want to enable logging to standard error using
        ``log_to_stderr``.
    """
    connection = connect(connection_string, user, password, login_timeout_sec, packet_size)
    connection.insert_into_table(reader=reader, table=table, chunk_size=chunk_size)


def from_table_to_db(
    source: Any,
    target: str,
    connection_string: str,
    chunk_size: int = 1000,
    user: str | None = None,
    password: str | None = None,
    login_timeout_sec: int | None = None,
):
    """
    Reads an arrow table and inserts its contents into a relational table on the database.

    This is a convinience wrapper around ``insert_into_table`` which converts an arrow table into a
    record batch reader for you.

    Example:

    .. code-block:: python

        from arrow_odbc import from_table_to_db
        import pyarrow as pa
        import pandas


        def dataframe_to_table(df):
            table = pa.Table.from_pandas(df)
            from_table_to_db(
                source=table
                connection_string=connection_string,
                user="SA",
                password="My@Test@Password",
                chunk_size=1000,
                table="MyTable",
            )

    :param source: PyArrow table with content to be inserted into the target table on the database.
        Each column of the table must correspond to a column in the target table with identical
        name.
    :param target: Name of the database table to insert into.
    :param connection_string: ODBC Connection string used to connect to the data source. To find a
        connection string for your data source try https://www.connectionstrings.com/.
    :param chunk_size: Number of records to insert in each roundtrip to the database. The number
        will be automatically reduced to the number of rows, if the table is small, in order to save
        memory.
    :param user: Allows for specifying the user seperatly from the connection string if it is not
        already part of it. The value will eventually be escaped and attached to the connection
        string as `UID`.
    :param password: Allows for specifying the password seperatly from the connection string if it
        is not already part of it. The value will eventually be escaped and attached to the
        connection string as `PWD`.
    :param login_timeout_sec: Number of seconds to wait for a login request to complete before
        returning to the application. The default is driver-dependent. If ``0``, the timeout is
        disabled and a connection attempt will wait indefinitely. If the specified timeout exceeds
        the maximum login timeout in the data source, the driver substitutes that value and uses
        that instead.
    """
    connection = connect(
        connection_string=connection_string,
        user=user,
        password=password,
        login_timeout_sec=login_timeout_sec,
    )
    connection.from_table_to_db(
        source=source,
        target=target,
        chunk_size=chunk_size,
    )
