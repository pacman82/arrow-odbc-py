"""Type stubs for the maturin-generated cffi sub-package `arrow_odbc.arrow_odbc`.

Mirrors the FFI surface in arrow-odbc.h (cbindgen output of src/*.rs).
Hand-maintained — re-sync when `extern "C"` signatures change in Rust.
"""

from cffi import FFI

ffi: FFI

class _Lib:
    # --- connection.rs ---
    def arrow_odbc_connection_free(self, connection: FFI.CData) -> None: ...
    def arrow_odbc_connection_make(
        self,
        connection_string_buf: bytes,
        connection_string_len: int,
        user: bytes | FFI.CData,
        user_len: int,
        password: bytes | FFI.CData,
        password_len: int,
        login_timeout_sec_ptr: FFI.CData,
        packet_size_ptr: FFI.CData,
        connection_out: FFI.CData,
    ) -> FFI.CData: ...
    def arrow_odbc_connection_set_autocommit(
        self, connection: FFI.CData, enabled: bool
    ) -> FFI.CData: ...
    def arrow_odbc_connection_commit(self, connection: FFI.CData) -> FFI.CData: ...
    def arrow_odbc_connection_rollback(self, connection: FFI.CData) -> FFI.CData: ...

    # --- error.rs ---
    def arrow_odbc_error_free(self, error: FFI.CData) -> None: ...
    def arrow_odbc_error_message(self, error: FFI.CData) -> FFI.CData: ...

    # --- logging.rs ---
    def arrow_odbc_log_to_stderr(self, level: int) -> FFI.CData: ...

    # --- parameter.rs ---
    def arrow_odbc_parameter_string_make(
        self, char_buf: bytes | FFI.CData, char_len: int, text_encoding: int
    ) -> FFI.CData: ...

    # --- pool.rs ---
    def arrow_odbc_enable_connection_pooling(self) -> FFI.CData: ...

    # --- reader.rs ---
    def arrow_odbc_reader_make(self, reader_out: FFI.CData) -> None: ...
    def arrow_odbc_reader_free(self, reader: FFI.CData) -> None: ...
    def arrow_odbc_reader_query(
        self,
        reader: FFI.CData,
        connection: FFI.CData,
        query_buf: bytes,
        query_len: int,
        parameters: FFI.CData,
        parameters_len: int,
        query_timeout_sec: FFI.CData,
    ) -> FFI.CData: ...
    def arrow_odbc_reader_next(
        self, reader: FFI.CData, array: FFI.CData, schema: FFI.CData, has_next_out: FFI.CData
    ) -> FFI.CData: ...
    def arrow_odbc_reader_more_results(
        self, reader: FFI.CData, has_more_results: FFI.CData
    ) -> FFI.CData: ...
    def arrow_odbc_reader_bind_buffers(
        self,
        reader: FFI.CData,
        max_num_rows_per_batch: int,
        max_bytes_per_batch: int,
        max_text_size: int,
        max_binary_size: int,
        fallibale_allocations: bool,
        fetch_concurrently: bool,
        payload_text_encoding: int,
        schema: FFI.CData,
    ) -> FFI.CData: ...
    def arrow_odbc_reader_schema(self, reader: FFI.CData, out_schema: FFI.CData) -> FFI.CData: ...
    def arrow_odbc_reader_into_concurrent(self, reader: FFI.CData) -> FFI.CData: ...

    # --- writer.rs ---
    def arrow_odbc_writer_free(self, writer: FFI.CData) -> None: ...
    def arrow_odbc_writer_make(
        self,
        connection: FFI.CData,
        table_buf: bytes,
        table_len: int,
        chunk_size: int,
        schema: FFI.CData,
        writer_out: FFI.CData,
    ) -> FFI.CData: ...
    def arrow_odbc_writer_write_batch(
        self, writer: FFI.CData, array_ptr: FFI.CData, schema_ptr: FFI.CData
    ) -> FFI.CData: ...
    def arrow_odbc_writer_flush(self, writer: FFI.CData) -> FFI.CData: ...

lib: _Lib
