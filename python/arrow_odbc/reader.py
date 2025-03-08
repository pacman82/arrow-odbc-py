from typing import List, Optional, Callable
from enum import Enum
from cffi.api import FFI  # type: ignore

import pyarrow

from pyarrow.cffi import ffi as arrow_ffi  # type: ignore
from pyarrow import RecordBatch, Schema, Array  # type: ignore

from arrow_odbc.connect import to_bytes_and_len, connect, ConnectionRaii  # type: ignore

from .arrow_odbc import ffi, lib  # type: ignore
from .error import raise_on_error

# Default maximum buffer size for transition buffer. Defaults to 512 MiB.
DEFAULT_FETCH_BUFFER_LIMIT_IN_BYTES = 2**29


class TextEncoding(Enum):
    """
    Text encoding used for the payload of text columns, to transfer data from the data source to the
    application.

    ``Auto`` evaluates to Utf16 on windows and Self::Utf8 on other systems. We do this, because most
    systems e.g. MacOs and Linux use UTF-8 as their default encoding, while windows may still use a
    Latin1 or some other extended ASCII as their narrow encoding. On the other hand many Posix
    drivers are lacking in their support for wide function calls and UTF-16. So using ``Utf16`` on
    windows and ``Utf8`` everythere else is a good starting point.

    ``Utf8`` use narrow characters (one byte) to encode text in payloads. ODBC lets the client
    choose the encoding which should be based on the system local. This is often not what is
    actually happening though. If we use narrow encoding, we assume the text to be UTF-8 and error
    if we find that not to be the case.

    ``Utf16`` use wide characters (two bytes) to encode text in payloads. ODBC defines the encoding
    to be always UTF-16.
    """

    AUTO = 0
    UTF8 = 1
    UTF16 = 2


def _schema_from_handle(handle) -> Schema:
    """
    Take a handle to an ArrowOdbcReader and return the associated pyarrow schema
    """
    # Expose schema as attribute
    # https://github.com/apache/arrow/blob/5ead37593472c42f61c76396dde7dcb8954bde70/python/pyarrow/tests/test_cffi.py
    with arrow_ffi.new("struct ArrowSchema *") as schema_out:
        error = lib.arrow_odbc_reader_schema(handle, schema_out)
        raise_on_error(error)
        ptr_schema = int(ffi.cast("uintptr_t", schema_out))
        return Schema._import_from_c(ptr_schema)


class _BatchReaderRaii:
    """
    Takes ownership of the reader in its various states and makes sure its resources are freed if
    the object is deleted.
    """

    def __init__(self):
        reader_out = ffi.new("ArrowOdbcReader **")
        lib.arrow_odbc_reader_make(reader_out)
        # We take ownership of the corresponding reader written in Rust and keep it alive until
        # `self` is deleted.
        self.handle = reader_out[0]

    def __del__(self):
        # Free the resources associated with this handle.
        lib.arrow_odbc_reader_free(self.handle)

    def schema(self) -> Schema:
        return _schema_from_handle(self.handle)

    def next_batch(self):
        array = arrow_ffi.new("struct ArrowArray *")
        schema = arrow_ffi.new("struct ArrowSchema *")

        has_next_out = ffi.new("int*")

        error = lib.arrow_odbc_reader_next(self.handle, array, schema, has_next_out)
        raise_on_error(error)

        if has_next_out[0] == 0:
            return None
        else:
            array_ptr = int(ffi.cast("uintptr_t", array))
            schema_ptr = int(ffi.cast("uintptr_t", schema))
            struct_array = Array._import_from_c(array_ptr, schema_ptr)
            return RecordBatch.from_struct_array(struct_array)

    def query(
        self,
        connection: ConnectionRaii,
        query: str,
        parameters: Optional[List[Optional[str]]],
        query_timeout_sec: Optional[int],
    ):
        query_bytes = query.encode("utf-8")

        if parameters is None:
            parameters_array = FFI.NULL
            parameters_len = 0
            encoded_parameters = []
        else:
            # Check precondition in order to save users some debugging, in case they directly pass a
            # non-string argument and do not use a type linter.
            if not all([p is None or hasattr(p, "encode") for p in parameters]):
                raise TypeError(
                    "read_arrow_batches_from_odbc only supports string arguments for SQL query "
                    "parameters"
                )

            parameters_array = ffi.new("ArrowOdbcParameter *[]", len(parameters))
            parameters_len = len(parameters)
            # Must be kept alive. Within Rust code we only allocate an additional indicator the string
            # payload is just referenced.
            encoded_parameters = [to_bytes_and_len(p) for p in parameters]

        for p_index in range(0, parameters_len):
            (p_bytes, p_len) = encoded_parameters[p_index]
            parameters_array[p_index] = lib.arrow_odbc_parameter_string_make(p_bytes, p_len)

        if query_timeout_sec is None:
            query_timeout_sec_pointer = ffi.NULL
        else:
            query_timeout_sec_pointer = ffi.new("uintptr_t *")
            query_timeout_sec_pointer[0] = query_timeout_sec

        error = lib.arrow_odbc_reader_query(
            self.handle,
            connection._arrow_odbc_connection(),
            query_bytes,
            len(query_bytes),
            parameters_array,
            parameters_len,
            query_timeout_sec_pointer,
        )

        # See if we managed to execute the query successfully and return an error if not
        raise_on_error(error)

    def bind_buffers(
        self,
        batch_size: int,
        max_bytes_per_batch: int,
        max_text_size: int,
        max_binary_size: int,
        falliable_allocations: bool,
        payload_text_encoding: TextEncoding,
        schema: Optional[Schema],
        map_schema: Optional[Callable[[Schema], Schema]],
        fetch_concurrently: bool,
    ):
        if map_schema is not None:
            schema = map_schema(self.schema())

        payload_text_encoding_int = payload_text_encoding.value

        ptr_schema = _export_schema_to_c(schema)

        error = lib.arrow_odbc_reader_bind_buffers(
            self.handle,
            batch_size,
            max_bytes_per_batch,
            max_text_size,
            max_binary_size,
            falliable_allocations,
            fetch_concurrently,
            payload_text_encoding_int,
            ptr_schema,
        )
        # See if we managed to execute the query successfully and return an error if not
        raise_on_error(error)

    def more_results(
        self,
    ) -> bool:
        with ffi.new("bool *") as has_more_results_c:
            error = lib.arrow_odbc_reader_more_results(
                self.handle,
                has_more_results_c,
            )
            # See if we managed to execute the query successfully and return an error if not
            raise_on_error(error)
            # Remember wether there is a new result set in a boolean
            has_more_results = has_more_results_c[0] != 0

        return has_more_results


class BatchReader:
    """
    Iterates over Arrow batches from an ODBC data source
    """

    def __init__(self, reader: _BatchReaderRaii):
        """
        Low level constructor, users should rather invoke `read_arrow_batches_from_odbc` in order to
        create instances of `BatchReader`.
        """
        # We take ownership of the corresponding reader written in Rust and keep it alive until
        # `self` is deleted. We also take care to keep this reader either in empty or reader state,
        # meaning we always have it ready to produce batches, or we consumed everything. We avoid
        # exposing the intermediate cursor state directly to users.
        self.reader = reader

        # This is the schema of the batches returned by reader. We take care to keep it in sync in
        # case the state of reader changes.
        self.schema = self.reader.schema()

    def __iter__(self):
        # Implement iterable protocol so reader can be used in for loops.
        return self

    def __next__(self) -> RecordBatch:
        # Implement iterator protocol
        batch = self.reader.next_batch()
        if batch is None:
            raise StopIteration()
        else:
            return batch

    def more_results(
        self,
        batch_size: int = 65535,
        max_bytes_per_batch: int = DEFAULT_FETCH_BUFFER_LIMIT_IN_BYTES,
        max_text_size: Optional[int] = None,
        max_binary_size: Optional[int] = None,
        falliable_allocations: bool = False,
        schema: Optional[Schema] = None,
        map_schema: Optional[Callable[[Schema], Schema]] = None,
        fetch_concurrently=True,
        payload_text_encoding: TextEncoding = TextEncoding.AUTO,
    ) -> bool:
        """
        Move the reader to the next result set returned by the data source.

        A datasource may return multiple results if multiple SQL statements are executed in a single
        query or a stored procedure is called. This method closes the current cursor and moves it to
        the next result set. You may move to the next result set without extracting the current one.

        Example:

        .. code-block:: python

            from arrow_odbc import read_arrow_batches_from_odbc

            connection_string="Driver={ODBC Driver 18 for SQL Server};Server=localhost;TrustServerCertificate=yes;"
            reader = read_arrow_batches_from_odbc(
                query=f"SELECT * FROM MyTable; SELECT * FROM OtherTable;",
                connection_string=connection_string,
                batch_size=1000,
                user="SA",
                password="My@Test@Password",
            )

            # Process first result
            for batch in reader:
                # Process arrow batches
                df = batch.to_pandas()
                # ...

            reader.more_results()

            # Process second result
            for batch in reader:
                # Process arrow batches
                df = batch.to_pandas()
                # ...

        :param batch_size: The maximum number rows within each batch. Batch size can be individually
            choosen for each result set. The maximum number of rows can be less if the upper bound
            defined by ``max_bytes_per_batch`` is lower.
        :param max_bytes_per_batch: An upper limit for the total size (all columns) of the buffer
            used to transit data from the ODBC driver to the application. Please note that memory
            consumption of this buffer is determined not by the actual values, but by the maximum
            possible length of an indiviual row times the number of rows it can hold. Both
            ``batch_size`` and this parameter define upper bounds for the same buffer. Which ever
            bound is lower is used to determine the buffer size.
        :param max_text_size: In order for fast bulk fetching to work, `arrow-odbc` needs to know the
            size of the largest possible field in each column. It will do so itself automatically by
            considering the schema information. However, trouble arises if the schema contains
            unbounded variadic fields like `VARCHAR(MAX)` which can hold really large values. These
            have a very high upper element size, if any. In order to work with such schemas we need
            a limit, of what the an upper bound of the actual values in the column is, as opposed to
            the what the largest value is the column could theoretically store. There is no need for
            this to be precise, but just knowing that a value would never exceed 4KiB rather than
            2GiB is enough to allow for tremendous efficiency gains. The size of the text is
            specified in UTF-8 encoded bytes if using a narrow encoding (typically all non-windows
            systems) and in UTF-16 encoded pairs of bytes on systems using a wide encoding
            (typically windows). This means about the size in letters, yet if you are using a lot of
            emojis or other special characters this number might need to be larger.
        :param max_binary_size: An upper limit for the size of buffers bound to variadic binary
            columns of the data source. This limit does not (directly) apply to the size of the
            created arrow buffers, but rather applies to the buffers used for the data in transit.
            Use this option if you have e.g. VARBINARY(MAX) fields in your next batch.
        :param falliable_allocations: If ``True`` an recoverable error is raised in case there is
            not enough memory to allocate the buffers. This option may incurr a performance penalty
            which scales with the batch size parameter (but not with the amount of actual data in
            the source). In case you can test your query against the schema you can safely set this
            to ``False``. The required memory will not depend on the amount of data in the data
            source. Default is ``True`` though, safety first.
        :param fetch_concurrently: Trade memory for speed. Allocates another transit buffer and use
            it to fetch row set groups (aka. batches) from the ODBC data source in a dedicated
            system thread, while the main thread converts the previous batch to arrow arrays and
            executes the application logic. The transit buffer may be the biggest part of the
            required memory so if ``True`` ``arrow-odbc`` consumes almost two times the memory as
            compared to false. On the flipsite the next batch can be fetched from the database
            immediatly without waiting for the application logic to return control.
        :param payload_text_encoding: Controls the encoding used for transferring text data from the
            ODBC data source to the application. The resulting Arrow arrays will still be UTF-8
            encoded. You may want to use this if you get garbage characters or invalid UTF-8 errors
            on non-windows systems to set the encoding to ``TextEncoding.Utf16``. On windows systems
            you may want to set this to ``TextEncoding::Utf8`` to gain performance benefits, after
            you have verified that your system locale is set to UTF-8.
        :return: ``True`` in case there is another result set. ``False`` in case that the last
            result set has been processed.
        """
        if max_text_size is None:
            max_text_size = 0
        if max_binary_size is None:
            max_binary_size = 0

        has_more_results = self.reader.more_results()

        self.reader.bind_buffers(
            batch_size=batch_size,
            max_bytes_per_batch=max_bytes_per_batch,
            max_text_size=max_text_size,
            max_binary_size=max_binary_size,
            falliable_allocations=falliable_allocations,
            payload_text_encoding=payload_text_encoding,
            schema=schema,
            map_schema=map_schema,
            fetch_concurrently=fetch_concurrently,
        )

        # Every result set can have its own schema, so we must update our member
        self.schema = self.reader.schema()

        return has_more_results

    def into_pyarrow_record_batch_reader(self):
        """
        Converts the ``arrow-odbc`` ``BatchReader`` into a ``pyarrow`` ``RecordBatchReader``. This
        method fully passes ownership to the new reader and leaves ``self`` empty.

        ``arrow-odbc``s BatchReader interface offers some functionality specific to ODBC
        datasources. E.g. the ability to move to the next result set of a stored procedure. You may
        not need this extra functionality and would rather like to integrate the ``BatchReader``
        with other libraries like e.g. DuckDB. In order to do this you can use this method to
        convert the ``arrow-odbc`` BatchReader into a ``pyarrow`` ``RecordBatchReader``.
        """
        # New empty tmp reader
        reader = _BatchReaderRaii()
        tmp = BatchReader(reader)
        # Swap self and tmp
        tmp.reader, self.reader = self.reader, tmp.reader
        tmp.schema, self.schema = self.schema, tmp.schema
        return pyarrow.RecordBatchReader.from_batches(tmp.schema, tmp)


def read_arrow_batches_from_odbc(
    query: str,
    connection_string: str,
    batch_size: int = 65535,
    user: Optional[str] = None,
    password: Optional[str] = None,
    parameters: Optional[List[Optional[str]]] = None,
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

        connection_string="Driver={ODBC Driver 18 for SQL Server};Server=localhost;TrustServerCertificate=yes;"

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
    reader = _BatchReaderRaii()

    connection = connect(
        connection_string=connection_string,
        user=user,
        password=password,
        login_timeout_sec=login_timeout_sec,
        packet_size=packet_size,
    )

    reader.query(
        connection=connection,
        query=query,
        parameters=parameters,
        query_timeout_sec=query_timeout_sec,
    )

    if max_text_size is None:
        max_text_size = 0
    if max_binary_size is None:
        max_binary_size = 0
    if max_bytes_per_batch is None:
        max_bytes_per_batch = 0

    # Let us transition to reader state
    reader.bind_buffers(
        batch_size=batch_size,
        max_bytes_per_batch=max_bytes_per_batch,
        max_text_size=max_text_size,
        max_binary_size=max_binary_size,
        falliable_allocations=falliable_allocations,
        payload_text_encoding=payload_text_encoding,
        schema=schema,
        map_schema=map_schema,
        fetch_concurrently=fetch_concurrently,
    )

    return BatchReader(reader)


def _export_schema_to_c(schema):
    if schema is None:
        ptr_schema = ffi.NULL
    else:
        ptr_schema = arrow_ffi.new("struct ArrowSchema *")
        int_schema = int(ffi.cast("uintptr_t", ptr_schema))
        schema._export_to_c(int_schema)
    return ptr_schema
