from typing import Optional, Callable, Sequence

from pyarrow import Schema

from .connect import connect
from .reader import (
    DEFAULT_FETCH_BUFFER_LIMIT_IN_BYTES,
    DEFAULT_FETCH_BUFFER_LIMIT_IN_ROWS,
    BatchReader,
    TextEncoding,
)


def read_arrow_batches_from_odbc(
    query: str,
    connection_string: str,
    batch_size: int = DEFAULT_FETCH_BUFFER_LIMIT_IN_ROWS,
    user: Optional[str] = None,
    password: Optional[str] = None,
    parameters: Optional[Sequence[Optional[str]]] = None,
    max_bytes_per_batch: Optional[int] = DEFAULT_FETCH_BUFFER_LIMIT_IN_BYTES,
    max_text_size: Optional[int] = None,
    max_binary_size: Optional[int] = None,
    falliable_allocations: bool = False,
    login_timeout_sec: Optional[int] = None,
    packet_size: Optional[int] = None,
    schema: Optional[Schema] = None,
    map_schema: Optional[Callable[[Schema], Schema]] = None,
    fetch_concurrently=True,
    query_timeout_sec: Optional[int] = None,
    payload_text_encoding: TextEncoding = TextEncoding.AUTO,
) -> BatchReader:
    """
    Execute the query and read the result as an iterator over Arrow batches.

    Example:

    .. code-block:: python

        from arrow_odbc import read_arrow_batches_from_odbc

        connection_string=
            "Driver={ODBC Driver 18 for SQL Server};" \
            "Server=localhost;"
            "TrustServerCertificate=yes;"

        reader = read_arrow_batches_from_odbc(
            query=f"SELECT * FROM MyTable WHERE a=?",
            connection_string=connection_string,
            batch_size=1000,
            parameters=["I'm a positional query parameter"],
            user="SA",
            password="My@Test@Password",
        )

        for batch in reader:
            # Process arrow batches
            df = batch.to_pandas()
            # ...

    :param query: The SQL statement yielding the result set which is converted into arrow record
        batches.
    :param batch_size: The maximum number rows within each batch. The maximum number of rows can be
        less if the upper bound defined by ``max_bytes_per_batch`` is lower.
    :param connection_string: ODBC Connection string used to connect to the data source. To find a
        connection string for your data source try https://www.connectionstrings.com/.
    :param user: Allows for specifying the user seperatly from the connection string if it is not
        already part of it. The value will eventually be escaped and attached to the connection
        string as `UID`.
    :param password: Allows for specifying the password seperatly from the connection string if it
        is not already part of it. The value will eventually be escaped and attached to the
        connection string as `PWD`.
    :param parameters: ODBC allows you to use a question mark as placeholder marker (``?``) for
        positional parameters. This argument takes a list of parameters those number must match the
        number of placholders in the SQL statement. Using this instead of literals helps you avoid
        SQL injections or may otherwise simplify your code. Currently all parameters are passed as
        VARCHAR strings. You can use `None` to pass `NULL`.
    :param max_bytes_per_batch: An upper limit for the total size (all columns) of the buffer used
        to transit data from the ODBC driver to the application. Please note that memory consumption
        of this buffer is determined not by the actual values, but by the maximum possible length of
        an indiviual row times the number of rows it can hold. Both ``batch_size`` and this
        parameter define upper bounds for the same buffer. Which ever bound is lower is used to
        determine the buffer size.
    :param max_text_size: In order for fast bulk fetching to work, `arrow-odbc` needs to know the
        size of the largest possible field in each column. It will do so itself automatically by
        considering the schema information. However, trouble arises if the schema contains
        unbounded variadic fields like `VARCHAR(MAX)` which can hold really large values. These have
        a very high upper element size, if any. In order to work with such schemas we need a limit,
        of what the an upper bound of the actual values in the column is, as opposed to the what the
        largest value is the column could theoretically store. There is no need for this to be
        precise, but just knowing that a value would never exceed 4KiB rather than 2GiB is enough to
        allow for tremendous efficiency gains. The size of the text is specified in UTF-8 encoded
        bytes if using a narrow encoding (typically all non-windows systems) and in UTF-16 encoded
        pairs of bytes on systems using a wide encoding (typically windows). This means about the
        size in letters, yet if you are using a lot of emojis or other special characters this
        number might need to be larger.
    :param max_binary_size: An upper limit for the size of buffers bound to variadic binary columns
        of the data source. This limit does not (directly) apply to the size of the created arrow
        buffers, but rather applies to the buffers used for the data in transit. Use this option if
        you have e.g. VARBINARY(MAX) fields in your database schema. In such a case without an upper
        limit, the ODBC driver of your data source is asked for the maximum size of an element, and
        is likely to answer with either 0 or a value which is way larger than any actual entry in
        the column. If you can not adapt your database schema, this limit might be what you are
        looking for. This is the maximum size in bytes of the binary column.
    :param falliable_allocations: If ``True`` an recoverable error is raised in case there is not
        enough memory to allocate the buffers. This option may incurr a performance penalty which
        scales with the batch size parameter (but not with the amount of actual data in the source).
        In case you can test your query against the schema you can safely set this to ``False``. The
        required memory will not depend on the amount of data in the data source. Default is
        ``True`` though, safety first.
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
    :param schema: Allows you to overwrite the automatically detected schema with one supplied by
        the application. Reasons for doing so include domain knowledge you have about the data which
        is not reflected in the schema information. E.g. you happen to know a field of timestamps
        contains strictly dates. Another reason could be that for certain usecases another it can
        make sense to decide the type based on what you want to do with it, rather than its source.
        E.g. if you simply want to put everything into a CSV file it can make perfect sense to fetch
        everything as string independent of its source type.
    :param map_schema: Allows you to provide a custom schema based on the schema inferred from the
        metainformation of the query. This would allow you to e.g. map every column type to string
        or replace any float32 with a float64, or anything else you might want to customize, for
        various reasons while still staying generic over the input schema. If both ``map_schema``
        and ``schema`` are specified ``map_schema`` takes priority.
    :param fetch_concurrently: Trade memory for speed. Allocates another transit buffer and use it
        to fetch row set groups (aka. batches) from the ODBC data source in a dedicated system
        thread, while the main thread converts the previous batch to arrow arrays and executes the
        application logic. The transit buffer may be the biggest part of the required memory so if
        ``True`` ``arrow-odbc`` consumes almost two times the memory as compared to false. On the
        flipsite the next batch can be fetched from the database immediatly without waiting for the
        application logic to return control.
    :param query_timeout_sec: Use this to limit the time the query is allowed to take, before
        responding with data to the application. The driver may replace the number of seconds you
        provide with a minimum or maximum value. You can specify ``0``, to deactivate the timeout,
        this is the default. For this to work the driver must support this feature. E.g. PostgreSQL,
        and Microsoft SQL Server do, but SQLite or MariaDB do not.
    :param payload_text_encoding: Controls the encoding used for transferring text data from the
            ODBC data source to the application. The resulting Arrow arrays will still be UTF-8
            encoded. You may want to use this if you get garbage characters or invalid UTF-8 errors
            on non-windows systems to set the encoding to ``TextEncoding.Utf16``. On windows systems
            you may want to set this to ``TextEncoding::Utf8`` to gain performance benefits, after
            you have verified that your system locale is set to UTF-8.
    :return: A ``BatchReader`` is returned, which implements the iterator protocol and iterates over
        individual arrow batches.
    """
    connection = connect(
        connection_string=connection_string,
        user=user,
        password=password,
        login_timeout_sec=login_timeout_sec,
        packet_size=packet_size,
    )

    return connection.read_arrow_batches(
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
