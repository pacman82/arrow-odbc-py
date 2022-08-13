from typing import Optional, Any

from pyarrow.cffi import ffi as arrow_ffi
from arrow_odbc.connect import connect_to_database

from ._native import ffi, lib  # type: ignore

class BatchWriter:
    """
    Writes arrow batches to a database table.
    """

    def __init__(self, handle):
        """
        Low level constructor, users should rather invoke ``insert_into_table``
        in order to create instances of ``BatchWriter``.
        """

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
        with arrow_ffi.new("struct ArrowArray*") as c_array, \
            arrow_ffi.new("struct ArrowSchema*") as c_schema:
            
            # Get the references to the C Data structures
            c_array_ptr = int(arrow_ffi.cast("uintptr_t", c_array))
            c_schema_ptr = int(arrow_ffi.cast("uintptr_t", c_schema))

            # Export the Array to the C Data structures.
            batch._export_to_c(c_array_ptr)
            batch.schema._export_to_c(c_schema_ptr)

            lib.arrow_odbc_writer_write_batch(self.handle, c_array, c_schema)

    def flush(self):
        """
        Inserts the remaining rows of the last chunk to the database.
        """
        lib.arrow_odbc_writer_flush(self.handle)

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

    :param reader: Reader is used to iterate over record batches. It must expose a `schema`
        attribute, referencing an Arrow schema. Each field in the schema must correspond to a
        column in the table with identical name.
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
    """
    table_bytes = table.encode("utf-8")

    # Allocate structures where we will export the Array data and the Array schema. They will be
    # released when we exit the with block.
    with arrow_ffi.new("struct ArrowSchema*") as c_schema:
        # Get the references to the C Data structures.
        c_schema_ptr = int(arrow_ffi.cast("uintptr_t", c_schema))

        # Export the schema to the C Data structures.
        reader.schema._export_to_c(c_schema_ptr)

        connection = connect_to_database(connection_string, user, password)

        # Connecting to the database has been successful. Note that connection does not truly take
        # ownership of the connection. If it runs out of scope (e.g. due to a raised exception) the
        # connection would not be closed and its associated resources would not be freed. However
        # `arrow_odbc_writer_make` will take ownership of connection. Even if it should fail the
        # connection will be closed.

        writer_out = ffi.new("ArrowOdbcWriter **")
        lib.arrow_odbc_writer_make(
            connection, table_bytes, len(table_bytes), chunk_size, c_schema, writer_out
        )
        writer = BatchWriter(writer_out[0])

    # Write all batches in reader
    for batch in reader:
        writer.write_batch(batch)
    writer.flush()