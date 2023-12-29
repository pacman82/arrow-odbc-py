import pyarrow as pa

from typing import List, Optional, Tuple
from cffi.api import FFI  # type: ignore

from pyarrow.cffi import ffi as arrow_ffi  # type: ignore
from pyarrow import RecordBatch, Schema, Array

from arrow_odbc.connect import to_bytes_and_len, connect_to_database  # type: ignore

from .arrow_odbc import ffi, lib  # type: ignore
from .error import raise_on_error


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


class BatchReader:
    """
    Iterates over Arrow batches from an ODBC data source
    """

    def __init__(self, handle):
        """
        Low level constructor, users should rather invoke
        `read_arrow_batches_from_odbc` in order to create instances of
        `BatchReader`.
        """

        # We take owners of the corresponding reader written in Rust and keep it alive until `self`
        # is deleted.
        #
        # The introduction of `more_results` made it necessary to also be able to represent already
        # consumed or empty cursors. In Python the user can always keep a reference to a
        # BatchReader alive and we have no way to force him/her to release it in case we move past
        # the last result set. Therfore we mutate this instance and represent these states with
        # handle == NULL and an empty schema.
        self.handle = handle

        # If this raises `__del__` will be invoked and free the handle, so we do not leak
        # resources here.
        self.schema = _schema_from_handle(self.handle)

    def __del__(self):
        # Free the resources associated with this handle.
        lib.arrow_odbc_reader_free(self.handle)

    def __iter__(self):
        # Implement iterable protocol so reader can be used in for loops.
        return self

    def __next__(self) -> RecordBatch:
        # Implment iterator protocol

        # In case this represents a non-cursor behave as iterating over an empty set of batches
        if self.handle == ffi.NULL:
            raise StopIteration()

        array = arrow_ffi.new("struct ArrowArray *")
        schema = arrow_ffi.new("struct ArrowSchema *")

        has_next_out = ffi.new("int*")

        error = lib.arrow_odbc_reader_next(self.handle, array, schema, has_next_out)
        raise_on_error(error)

        if has_next_out[0] == 0:
            raise StopIteration()
        else:
            array_ptr = int(ffi.cast("uintptr_t", array))
            schema_ptr = int(ffi.cast("uintptr_t", schema))
            struct_array = Array._import_from_c(array_ptr, schema_ptr)
            return RecordBatch.from_struct_array(struct_array)

    def more_results(
        self,
        batch_size: int = 65535,
        max_bytes_per_batch: Optional[int] = 2**21,
        max_text_size: Optional[int] = None,
        max_binary_size: Optional[int] = None,
        falliable_allocations: bool = False,
        schema: Optional[Schema] = None
    ) -> bool:
        """
        Move the reader to the next result set returned by the data source.

        A datasource may return multiple results if multiple SQL statements are executed in a single
        query or a stored procedure is called. This method closes the current cursor and moves it to
        the next result set. You may move to the next result set without extracting the current one.

        Example:

        .. code-block:: python

            from arrow_odbc import read_arrow_batches_from_odbc

            connection_string="Driver={ODBC Driver 17 for SQL Server};Server=localhost;"
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
        :param max_text_size: An upper limit for the size of buffers bound to variadic text columns
            of the data source. This limit does not (directly) apply to the size of the created
            arrow buffers, but rather applies to the buffers used for the data in transit. Use this
            option if you have e.g. VARCHAR(MAX) fields the next batch.
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
        :return: ``True`` in case there is another result set. ``False`` in case that the last
            result set has been processed.
        """
        if max_text_size is None:
            max_text_size = 0
        if max_binary_size is None:
            max_binary_size = 0

        ptr_schema = _export_schema_to_c(schema)

        with ffi.new("bool *") as has_more_results_c:
            error = lib.arrow_odbc_reader_more_results(
                self.handle,
                has_more_results_c,
                batch_size,
                max_bytes_per_batch,
                max_text_size,
                max_binary_size,
                falliable_allocations,
                ptr_schema,
            )
            # See if we managed to execute the query successfully and return an
            # error if not
            raise_on_error(error)

            has_more_results = has_more_results_c[0] != 0

        # Every result set can have its own schema, so we must update our member
        self.schema = _schema_from_handle(self.handle)

        return has_more_results

    def fetch_concurrently(self):
        """
        Allocate another transit buffer and use it to fetch row set groups (aka. batches) from the
        ODBC data source in a dedicated system thread, while the main thread converts the previous
        batch to arrow arrays and executes the application logic.

        If you extract more than one result set from the cursor, you need to call these method for
        each result set you want to extract concurrently. This has been done so it is possible to
        skip result sets without worrying about the fetching thread to start fetching row groups
        from a result set you intended to skip.

        Calling this method on an already concurrent reader has no effect.

        Example:

        .. code-block:: python

            from arrow_odbc import read_arrow_batches_from_odbc

            connection_string="Driver={ODBC Driver 17 for SQL Server};Server=localhost;"

            reader = read_arrow_batches_from_odbc(
                query=f"SELECT * FROM MyTable,
                connection_string=connection_string,
                batch_size=1000,
                user="SA",
                password="My@Test@Password",
            )
            # Trade memory for speed. For the price of an additional transit buffer and a native
            # system thread we fetch batches now concurrent to our application logic.
            reader.fetch_concurrently()

            for batch in reader:
                # Process arrow batches
                df = batch.to_pandas()
                # ...
        """
        error = lib.arrow_odbc_reader_into_concurrent(self.handle)

        # Making a reader concurrent will not change its schema, yet if there is an error the reader
        # is destroyed and its schema is empty. The if is a slight optimization.
        # self.schema == _schema_from_handle(self.handle)
        # should always be true and so asigning it never would make the code incorrect. Yet we only
        # need to do so if it actually changes.
        if error != ffi.NULL:
            self.schema = _schema_from_handle(self.handle)

        raise_on_error(error)


def read_arrow_batches_from_odbc(
    query: str,
    connection_string: str,
    batch_size: int = 65535,
    user: Optional[str] = None,
    password: Optional[str] = None,
    parameters: Optional[List[Optional[str]]] = None,
    max_bytes_per_batch: Optional[int] = 2**29,
    max_text_size: Optional[int] = None,
    max_binary_size: Optional[int] = None,
    falliable_allocations: bool = False,
    login_timeout_sec: Optional[int] = None,
    schema: Optional[Schema] = None,
) -> Optional[BatchReader]:
    """
    Execute the query and read the result as an iterator over Arrow batches.

    Example:

    .. code-block:: python

        from arrow_odbc import read_arrow_batches_from_odbc

        connection_string="Driver={ODBC Driver 17 for SQL Server};Server=localhost;"

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
    :param max_text_size: An upper limit for the size of buffers bound to variadic text columns of
        the data source. This limit does not (directly) apply to the size of the created arrow
        buffers, but rather applies to the buffers used for the data in transit. Use this option if
        you have e.g. VARCHAR(MAX) fields in your database schema. In such a case without an upper
        limit, the ODBC driver of your data source is asked for the maximum size of an element, and
        is likely to answer with either 0 or a value which is way larger than any actual entry in
        the column If you can not adapt your database schema, this limit might be what you are
        looking for. On windows systems the size is double words (16Bit), as windows utilizes an
        UTF-16 encoding. So this translates to roughly the size in letters. On non windows systems
        this is the size in bytes and the datasource is assumed to utilize an UTF-8 encoding.
        ``None`` means no upper limit is set and the maximum element size, reported by ODBC is used
        to determine buffer sizes.
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
    :param schema: Allows you to overwrite the automatically detected schema with one supplied by
        the application. Reasons for doing so include domain knowledge you have about the data which
        is not reflected in the schema information. E.g. you happen to know a field of timestamps
        contains strictly dates. Another reason could be that for certain usecases another it can
        make sense to decide the type based on what you want to do with it, rather than its source.
        E.g. if you simply want to put everything into a CSV file it can make perfect sense to fetch
        everything as string independent of its source type.
    :return: In case the query does not produce a result set (e.g. in case of an INSERT statement),
        ``None`` is returned. Should the statement return a result set a ``BatchReader`` is
        returned, which implements the iterator protocol and iterates over individual arrow batches.
    """
    query_bytes = query.encode("utf-8")

    connection = connect_to_database(
        connection_string, user, password, login_timeout_sec
    )

    # Connecting to the database has been successful. Note that connection does not truly take
    # ownership of the connection. If it runs out of scope (e.g. due to a raised exception) the
    # connection would not be closed and its associated resources would not be freed.
    # However, this is fine since everything from here on out until we call arrow_odbc_reader_make
    # is infalliable. arrow_odbc_reader_make will truly take ownership of the connection. Even if it
    # should fail, it will be closed correctly.

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
        # Must be kept alive. Within Rust code we only allocate an additional
        # indicator the string payload is just referenced.
        encoded_parameters = [to_bytes_and_len(p) for p in parameters]

    if max_text_size is None:
        max_text_size = 0

    if max_binary_size is None:
        max_binary_size = 0

    if max_bytes_per_batch is None:
        max_bytes_per_batch = 0

    ptr_schema = _export_schema_to_c(schema)

    for p_index in range(0, parameters_len):
        (p_bytes, p_len) = encoded_parameters[p_index]
        parameters_array[p_index] = lib.arrow_odbc_parameter_string_make(p_bytes, p_len)

    reader_out = ffi.new("ArrowOdbcReader **")

    error = lib.arrow_odbc_reader_make(
        connection,
        query_bytes,
        len(query_bytes),
        batch_size,
        max_bytes_per_batch,
        parameters_array,
        parameters_len,
        max_text_size,
        max_binary_size,
        falliable_allocations,
        ptr_schema,
        reader_out,
    )

    # See if we managed to execute the query successfully and return an
    # error if not
    raise_on_error(error)

    reader = reader_out[0]
    return BatchReader(reader)


def _export_schema_to_c(schema):
    if schema is None:
        ptr_schema = ffi.NULL
    else:
        ptr_schema = arrow_ffi.new("struct ArrowSchema *")
        int_schema = int(ffi.cast("uintptr_t", ptr_schema))
        schema._export_to_c(int_schema)
    return ptr_schema
