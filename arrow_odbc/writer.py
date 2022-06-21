from typing import Optional, Any

from pyarrow.cffi import ffi as arrow_ffi
from arrow_odbc.connect import connect_to_database

from ._native import ffi, lib  # type: ignore


def insert_into_table(
    reader: Any,
    chunk_size: int,
    table: str,
    connection_string: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
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
    table_bytes = table.encode("utf-8")

    # See: <https://arrow.apache.org/docs/python/integration/python_r.html>

    # Allocate structures where we will export the Array data and the Array schema. They will be
    # released when we exit the with block.
    with arrow_ffi.new("struct ArrowArray*") as c_array, \
        arrow_ffi.new("struct ArrowSchema*") as c_schema:
        # Get the references to the C Data structures.
        c_array_ptr = int(arrow_ffi.cast("uintptr_t", c_array))
        c_schema_ptr = int(arrow_ffi.cast("uintptr_t", c_schema))

        # Export the Array and its schema to the C Data structures.
        # array._export_to_c(c_array_ptr)
        # array.type._export_to_c(c_schema_ptr)
        reader.schema._export_to_c(c_schema_ptr)

        connection = connect_to_database(connection_string, user, password)

        # Connecting to the database has been successful. Note that connection_out does not truly take
        # ownership of the connection. If it runs out of scope (e.g. due to a raised exception) the
        # connection would not be closed and its associated resources would not be freed.

        writer_out = ffi.new("ArrowOdbcWriter **")
        lib.arrow_odbc_writer_make(
            connection, table_bytes, len(table_bytes), chunk_size, c_schema_ptr, writer_out
        )
        writer = writer_out[0]
    lib.arrow_odbc_writer_free(writer)
