from ._arrow_odbc_c import lib # type: ignore

class BatchReader:
    """
    Iterates over Arrow batches from an ODBC data source
    """
    def __init__(self, handle, connection):
        # Must keep connection alive, for the lifetime of the reader
        self.connection = connection
        self.handle = handle

    def __del__(self):
        lib.arrow_odbc_reader_free(self.handle)

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration()