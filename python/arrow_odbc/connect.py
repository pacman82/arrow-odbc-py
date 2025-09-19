from typing import Any, Callable, Sequence

from pyarrow import Schema  # type: ignore
from pyarrow import RecordBatchReader

from .reader import (
    DEFAULT_FETCH_BUFFER_LIMIT_IN_BYTES,
    DEFAULT_FETCH_BUFFER_LIMIT_IN_ROWS,
    BatchReader,
    TextEncoding,
)
from .connection_raii import ConnectionRaii
from .pool import enable_odbc_connection_pooling
from .writer import BatchWriter


class Connection:
    """
    A strong reference to an ODBC connection.
    """

    def __init__(self, raii: ConnectionRaii) -> None:
        self.raii = raii

    @classmethod
    def enable_connection_pooling(cls) -> None:
        """
        Activates the connection pooling of the ODBC driver manager for the entire process. Best
        called before creating the ODBC environment, i.e. before you the first connection is opend
        with arrow-odbc. This is useful in scenarios there you frequently read or write rows and the
        overhead of creating a connection for each query is significant.

        Example:

        .. code-block:: python

            from arrow_odbc import Connection

            # Let the ODBC driver manager take care of connection pooling for us
            Connection.enable_connection_pooling()

            # Create the first connection after Connection pooling is enabled
            connection_string=
                "Driver={ODBC Driver 18 for SQL Server};" \
                "Server=localhost;" \
                "TrustServerCertificate=yes;"
            connection = connect(
                connection_string=connection_string,
                user="SA",
                password="My@Test@Password"
            )
        """
        enable_odbc_connection_pooling()

    def read_arrow_batches(
        self,
        query: str,
        batch_size: int = DEFAULT_FETCH_BUFFER_LIMIT_IN_ROWS,
        parameters: Sequence[str | None] | None = None,
        max_bytes_per_batch: int | None = DEFAULT_FETCH_BUFFER_LIMIT_IN_BYTES,
        max_text_size: int | None = None,
        max_binary_size: int | None = None,
        falliable_allocations: bool = False,
        schema: Schema | None = None,
        map_schema: Callable[[Schema], Schema] | None = None,
        fetch_concurrently: bool = True,
        query_timeout_sec: int | None = None,
        payload_text_encoding: TextEncoding = TextEncoding.AUTO,
    ) -> BatchReader:
        """
        Execute the query and read the result as an iterator over Arrow batches.

        Example:

        .. code-block:: python

            from arrow_odbc import connect

            # Connect to the data source
            connection_string=
                "Driver={ODBC Driver 18 for SQL Server};" \
                "Server=localhost;" \
                "TrustServerCertificate=yes;"

            connection = connect(
                connection_string=connection_string,
                user="SA",
                password="My@Test@Password"
            )

            # Execute query and create reader
            reader = connection.read_arrow_batches(
                query=f"SELECT * FROM MyTable WHERE a=?",
                batch_size=1000,
                parameters=["I'm a positional query parameter"],
            )

            # Process results
            for batch in reader:
                # Process arrow batches
                df = batch.to_pandas()
                # ...

        :param connection: An ODBC connection created with ``connect``.
        :param query: The SQL statement yielding the result set which is converted into arrow record
            batches.
        :param batch_size: The maximum number rows within each batch. The maximum number of rows can
            be less if the upper bound defined by ``max_bytes_per_batch`` is lower.
        :param parameters: ODBC allows you to use a question mark as placeholder marker (``?``) for
            positional parameters. This argument takes a list of parameters those number must match
            the number of placholders in the SQL statement. Using this instead of literals helps you
            avoid SQL injections or may otherwise simplify your code. Currently all parameters are
            passed as VARCHAR strings. You can use `None` to pass `NULL`.
        :param max_bytes_per_batch: An upper limit for the total size (all columns) of the buffer
            used to transit data from the ODBC driver to the application. Please note that memory
            consumption of this buffer is determined not by the actual values, but by the maximum
            possible length of an individual row times the number of rows it can hold. Both
            ``batch_size`` and this parameter define upper bounds for the same buffer. Which ever
            bound is lower is used to determine the buffer size.
        :param max_text_size: In order for fast bulk fetching to work, `arrow-odbc` needs to know
            the size of the largest possible field in each column. It will do so itself
            automatically by considering the schema information. However, trouble arises if the
            schema contains unbounded variadic fields like `VARCHAR(MAX)` which can hold really
            large values. These have a very high upper element size, if any. In order to work with
            such schemas we need a limit, of what the an upper bound of the actual values in the
            column is, as opposed to the what the largest value is the column could theoretically
            store. There is no need for this to be precise, but just knowing that a value would
            never exceed 4KiB rather than 2GiB is enough to allow for tremendous efficiency gains.
            The size of the text is specified in UTF-8 encoded bytes if using a narrow encoding
            (typically all non-windows systems) and in UTF-16 encoded pairs of bytes on systems
            using a wide encoding (typically windows). This means about the size in letters, yet if
            you are using a lot of emojis or other special characters this number might need to be
            larger.
        :param max_binary_size: An upper limit for the size of buffers bound to variadic binary
            columns of the data source. This limit does not (directly) apply to the size of the
            created arrow buffers, but rather applies to the buffers used for the data in transit.
            Use this option if you have e.g. VARBINARY(MAX) fields in your database schema. In such
            a case without an upper limit, the ODBC driver of your data source is asked for the
            maximum size of an element, and is likely to answer with either 0 or a value which is
            way larger than any actual entry in the column. If you can not adapt your database
            schema, this limit might be what you are looking for. This is the maximum size in bytes
            of the binary column.
        :param falliable_allocations: If ``True`` an recoverable error is raised in case there is
            not enough memory to allocate the buffers. This option may incurr a performance penalty
            which scales with the batch size parameter (but not with the amount of actual data in
            the source). In case you can test your query against the schema you can safely set this
            to ``False``. The required memory will not depend on the amount of data in the data
            source. Default is ``True`` though, safety first.
        :param schema: Allows you to overwrite the automatically detected schema with one supplied
            by the application. Reasons for doing so include domain knowledge you have about the
            data which is not reflected in the schema information. E.g. you happen to know a field
            of timestamps contains strictly dates. Another reason could be that for certain usecases
            another it can make sense to decide the type based on what you want to do with it,
            rather than its source. E.g. if you simply want to put everything into a CSV file it can
            make perfect sense to fetch everything as string independent of its source type.
        :param map_schema: Allows you to provide a custom schema based on the schema inferred from
            the metainformation of the query. This would allow you to e.g. map every column type to
            string or replace any float32 with a float64, or anything else you might want to
            customize, for various reasons while still staying generic over the input schema. If
            both ``map_schema`` and ``schema`` are specified ``map_schema`` takes priority.
        :param fetch_concurrently: Trade memory for speed. Allocates another transit buffer and use
            it to fetch row set groups (aka. batches) from the ODBC data source in a dedicated
            system thread, while the main thread converts the previous batch to arrow arrays and
            executes the application logic. The transit buffer may be the biggest part of the
            required memory so if ``True`` ``arrow-odbc`` consumes almost two times the memory as
            compared to false. On the flipsite the next batch can be fetched from the database
            immediatly without waiting for the application logic to return control.
        :param query_timeout_sec: Use this to limit the time the query is allowed to take, before
            responding with data to the application. The driver may replace the number of seconds
            you provide with a minimum or maximum value. You can specify ``0``, to deactivate the
            timeout, this is the default. For this to work the driver must support this feature.
            E.g. PostgreSQL, and Microsoft SQL Server do, but SQLite or MariaDB do not.
        :param payload_text_encoding: Controls the encoding used for transferring text data from the
                ODBC data source to the application. The resulting Arrow arrays will still be UTF-8
                encoded. You may want to use this if you get garbage characters or invalid UTF-8
                errors on non-windows systems to set the encoding to ``TextEncoding.Utf16``. On
                windows systems you may want to set this to ``TextEncoding::Utf8`` to gain
                performance benefits, after you have verified that your system locale is set to
                UTF-8.
        :return: A ``BatchReader`` is returned, which implements the iterator protocol and iterates
            over individual arrow batches.
        """
        return BatchReader._from_connection(
            connection=self.raii,
            query=query,
            batch_size=batch_size,
            parameters=parameters,
            max_bytes_per_batch=max_bytes_per_batch,
            max_text_size=max_text_size,
            max_binary_size=max_binary_size,
            falliable_allocations=falliable_allocations,
            schema=schema,
            map_schema=map_schema,
            fetch_concurrently=fetch_concurrently,
            query_timeout_sec=query_timeout_sec,
            payload_text_encoding=payload_text_encoding,
        )

    def insert_into_table(
        self,
        reader: Any,
        table: str,
        chunk_size: int,
    ):
        """
        Consume the batches in the reader and insert them into a table on the database.

        Example:

        .. code-block:: python

            from arrow_odbc import connect
            import pyarrow as pa
            import pandas


            def dataframe_to_table(df):
                table = pa.Table.from_pandas(df)
                reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
                connectiion = connect(
                    connection_string=connection_string,
                    user="SA",
                    password="My@Test@Password"
                )
                connection.insert_into_table(
                    table="MyTable",
                    reader=reader,
                    chunk_size=1000,
                )

        :param reader: Reader is used to iterate over record batches. It must expose a `schema`
            attribute, referencing an Arrow schema. Each field in the schema must correspond to a
            column in the table with identical name. The iterator must yield individual arrow tables
        :param table: Name of a database table to insert into. Used to generate the insert statement
            for the bulk writer.
        :param chunk_size: Number of records to insert in each roundtrip to the database.
            Independent of batch size (i.e. number of rows in an individual record batch).
        """
        writer = BatchWriter._from_connection(
            connection=self.raii,
            reader=reader,
            chunk_size=chunk_size,
            table=table,
        )

        # Write all batches in reader
        for batch in reader:
            writer.write_batch(batch)
        writer.flush()

    def from_table_to_db(
        self,
        source: Any,
        target: str,
        chunk_size: int = 1000,
    ):
        """
        Reads an arrow table and inserts its contents into a relational table on the database.

        This is a convinience wrapper around ``insert_into_table`` which converts an arrow table
        into a record batch reader for you.

        Example:

        .. code-block:: python

            from arrow_odbc import connect
            import pyarrow as pa
            import pandas


            def dataframe_to_table(df):
                table = pa.Table.from_pandas(df)
                connection = connect(
                    connection_string=connection_string,
                    user="SA",
                    password="My@Test@Password"
                )
                connection.from_table_to_db(
                    source=table,
                    target="MyTable",
                    chunk_size=1000
                )

        :param source: PyArrow table with content to be inserted into the target table on the
            database. Each column of the table must correspond to a column in the target table with
            identical name.
        :param target: Name of the database table to insert into.
        :param chunk_size: Number of records to insert in each roundtrip to the database. The number
            will be automatically reduced to the number of rows, if the table is small, in order to
            save memory.
        """
        # There is no need for chunk size to exceed the maximum amount of rows in the table
        chunk_size = min(chunk_size, source.num_rows)
        # We implemement this in terms of the functionality to insert a batches from a record batch
        # reader, so first we convert our table into a record batch reader.
        schema = source.schema
        batches = source.to_batches(chunk_size)
        reader = RecordBatchReader.from_batches(schema, batches)
        # Now we can insert from the reader
        self.insert_into_table(
            reader=reader,
            table=target,
            chunk_size=chunk_size,
        )


def connect(
    connection_string: str,
    user: str | None = None,
    password: str | None = None,
    login_timeout_sec: int | None = None,
    packet_size: int | None = None,
) -> Connection:
    """
    Opens a connection to an ODBC data source.

    In case you want to use connection pooling, call ``enable_odbc_connection_pooling()`` before
    calling this function.

    Example:

    .. code-block:: python

        from arrow_odbc import connect, enable_odbc_connection_pooling

        # Let the ODBC driver manager take care of connection pooling for us
        enable_odbc_connection_pooling()

        # Connect to the data source
        connection_string=
            "Driver={ODBC Driver 18 for SQL Server};" \
            "Server=localhost;" \
            "TrustServerCertificate=yes;"
        connection = connect(
            connection_string=connection_string,
            user="SA",
            password="My@Test@Password"
        )

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
    :return: A ``Connection`` is returned.
    """
    raii = ConnectionRaii.connect(
        connection_string=connection_string,
        user=user,
        password=password,
        login_timeout_sec=login_timeout_sec,
        packet_size=packet_size,
    )
    return Connection(raii=raii)
