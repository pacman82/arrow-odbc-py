from typing import Any

from pyarrow.cffi import ffi as arrow_ffi

from .arrow_odbc import ffi, lib  # type: ignore
from .connection_raii import ConnectionRaii
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

    @classmethod
    def _from_connection(
        cls,
        connection: ConnectionRaii,
        reader: Any,
        chunk_size: int,
        table: str,
    ):
        table_bytes = table.encode("utf-8")

        # Allocate structures where we will export the Array data and the Array schema. They will be
        # released when we exit the with block.
        with arrow_ffi.new("struct ArrowSchema*") as c_schema:
            # Get the references to the C Data structures.
            c_schema_ptr = int(arrow_ffi.cast("uintptr_t", c_schema))

            # Export the schema to the C Data structures.
            reader.schema._export_to_c(c_schema_ptr)

            writer_out = ffi.new("ArrowOdbcWriter **")
            error = lib.arrow_odbc_writer_make(
                connection.arrow_odbc_connection(),
                table_bytes,
                len(table_bytes),
                chunk_size,
                c_schema,
                writer_out,
            )
            raise_on_error(error)

        return BatchWriter(handle=writer_out[0])

    def write_batch(self, batch):
        """
        Fills the internal buffers of the writer with data from the batch. Every
        time they are full, the data is send to the database. To make sure all
        the data is is send ``flush`` must be called.
        """
        with (
            arrow_ffi.new("struct ArrowArray*") as c_array,
            arrow_ffi.new("struct ArrowSchema*") as c_schema,
        ):
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
