from typing import Optional, Any

from pyarrow import RecordBatchReader
from pyarrow.cffi import ffi as arrow_ffi
from arrow_odbc.connect import connect_to_database

from .arrow_odbc import ffi, lib  # type: ignore
from .error import raise_on_error


class BatchWriter:
    """
    Writes arrow batches to a database table.
    """

    def __init__(
        self,
        handle,
    ):
        # We take ownership of the corresponding writer written in Rust and keep it alive until
        # `self` is deleted
        self.handle = handle

    def __del__(self):
        # Free the resources associated with this handle.
        lib.arrow_odbc_writer_free(self.handle)

    def write_batch(self, batch):
        """
        Fills the internal buffers of the writer with data from the batch. Every
        time they are full, the data is send to the database. To make sure all
        the data is is send ``flush`` must be called.
        """
        with arrow_ffi.new("struct ArrowArray*") as c_array, arrow_ffi.new(
            "struct ArrowSchema*"
        ) as c_schema:
            # Get the references to the C Data structures
            c_array_ptr = int(arrow_ffi.cast("uintptr_t", c_array))
            c_schema_ptr = int(arrow_ffi.cast("uintptr_t", c_schema))

            # Export the Array to the C Data structures.
            batch._export_to_c(c_array_ptr)
            batch.schema._export_to_c(c_schema_ptr)

            error = lib.arrow_odbc_writer_write_batch(self.handle, c_array, c_schema)
            raise_on_error(error)

    def flush(self):
        """
        Inserts the remaining rows of the last chunk to the database.
        """
        error = lib.arrow_odbc_writer_flush(self.handle)
        raise_on_error(error)


def insert_into_table(
    reader: Any,
    chunk_size: int,
    table: str,
    connection_string: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    login_timeout_sec: Optional[int] = None,
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
        column in the table with identical name. The iterator must yield individual arrow tables
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
    """
    table_bytes = table.encode("utf-8")

    # Allocate structures where we will export the Array data and the Array schema. They will be
    # released when we exit the with block.
    with arrow_ffi.new("struct ArrowSchema*") as c_schema:
        # Get the references to the C Data structures.
        c_schema_ptr = int(arrow_ffi.cast("uintptr_t", c_schema))

        # Export the schema to the C Data structures.
        reader.schema._export_to_c(c_schema_ptr)

        connection = connect_to_database(
            connection_string, user, password, login_timeout_sec
        )

        # Connecting to the database has been successful. Note that connection does not truly take
        # ownership of the connection. If it runs out of scope (e.g. due to a raised exception) the
        # connection would not be closed and its associated resources would not be freed. However
        # `arrow_odbc_writer_make` will take ownership of connection. Even if it should fail the
        # connection will be closed.

        writer_out = ffi.new("ArrowOdbcWriter **")
        error = lib.arrow_odbc_writer_make(
            connection,
            table_bytes,
            len(table_bytes),
            chunk_size,
            c_schema,
            writer_out,
        )
        raise_on_error(error)

    writer = BatchWriter(handle=writer_out[0])

    # Write all batches in reader
    for batch in reader:
        writer.write_batch(batch)
    writer.flush()


def from_table_to_db(
    source: Any,
    target: str,
    connection_string: str,
    chunk_size: int = 1000,
    user: Optional[str] = None,
    password: Optional[str] = None,
    login_timeout_sec: Optional[int] = None,
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
    # There is no need for chunk size to exceed the maximum amount of rows in the table
    chunk_size = min(chunk_size, source.num_rows)
    # We implemement this in terms of the functionality to insert a batches from a record batch
    # reader, so first we convert our table into a record batch reader.
    schema = source.schema
    batches = source.to_batches(chunk_size)
    reader = RecordBatchReader.from_batches(schema, batches)
    # Now we can insert from the reader
    insert_into_table(
        reader,
        chunk_size=chunk_size,
        table=target,
        connection_string=connection_string,
        user=user,
        password=password,
        login_timeout_sec=login_timeout_sec,
    )
