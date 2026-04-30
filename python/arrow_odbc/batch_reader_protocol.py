from collections.abc import Iterator
from typing import Protocol

from pyarrow import RecordBatch, Schema


class BatchReaderProtocol(Protocol):
    """
    Anything that exposes an Arrow schema and iterates over record batches.

    Both ``pyarrow.RecordBatchReader`` and ``arrow_odbc.BatchReader`` satisfy this
    structurally — no inheritance required.
    """

    @property
    def schema(self) -> Schema: ...

    def __iter__(self) -> Iterator[RecordBatch]: ...
