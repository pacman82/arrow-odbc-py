class BatchReader:
    """
    Iterates over Arrow batches from an ODBC data source
    """

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration()