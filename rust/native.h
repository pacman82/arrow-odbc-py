#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Handle to an error emmitted by arrow odbc
 */
typedef struct ArrowOdbcError ArrowOdbcError;

/**
 * Opaque type holding a parameter intended to be bound to a placeholder (`?`) in an SQL query.
 */
typedef struct ArrowOdbcParameter ArrowOdbcParameter;

/**
 * Opaque type holding all the state associated with an ODBC reader implementation in Rust. This
 * type also has ownership of the ODBC Connection handle.
 */
typedef struct ArrowOdbcReader ArrowOdbcReader;

/**
 * Opaque type holding all the state associated with an ODBC writer implementation in Rust. This
 * type also has ownership of the ODBC Connection handle.
 */
typedef struct ArrowOdbcWriter ArrowOdbcWriter;

/**
 * Opaque type to transport connection to an ODBC Datasource over language boundry
 */
typedef struct OdbcConnection OdbcConnection;

/**
 * Allocate and open an ODBC connection using the specified connection string. In case of an error
 * this function returns a NULL pointer.
 *
 * # Safety
 *
 * `connection_string_buf` must point to a valid utf-8 encoded string. `connection_string_len` must
 * hold the length of text in `connection_string_buf`.
 * `user` and or `password` are optional and are allowed to be `NULL`.
 */
struct ArrowOdbcError *arrow_odbc_connect_with_connection_string(const uint8_t *connection_string_buf,
                                                                 uintptr_t connection_string_len,
                                                                 const uint8_t *user,
                                                                 uintptr_t user_len,
                                                                 const uint8_t *password,
                                                                 uintptr_t password_len,
                                                                 struct OdbcConnection **connection_out);

/**
 * Deallocates the resources associated with an error.
 *
 * # Safety
 *
 * Error must be a valid non null pointer to an Error.
 */
void arrow_odbc_error_free(struct ArrowOdbcError *error);

/**
 * A zero terminated string describing the error
 *
 * # Safety
 *
 * Error must be a valid non null pointer to an Error.
 */
const char *arrow_odbc_error_message(const struct ArrowOdbcError *error);

/**
 * # Safety
 *
 * `char_buf` may be `NULL`, but if it is not, it must contain a valid utf-8 sequence not shorter
 * than `char_len`. This function does not take ownership of the parameter. The parameter must at
 * least be valid until the call make reader is finished.
 */
struct ArrowOdbcParameter *arrow_odbc_parameter_string_make(const uint8_t *char_buf,
                                                            uintptr_t char_len);

/**
 * Creates an Arrow ODBC reader instance.
 *
 * Takes ownership of connection even in case of an error. `reader_out` is assigned a NULL pointer
 * in case the query does not return a result set.
 *
 * # Safety
 *
 * * `connection` must point to a valid OdbcConnection. This function takes ownership of the
 *   connection, even in case of an error. So The connection must not be freed explicitly
 *   afterwards.
 * * `query_buf` must point to a valid utf-8 string
 * * `query_len` describes the len of `query_buf` in bytes.
 * * `parameters` must contain only valid pointers. This function takes ownership of all of them
 *   independent if the function succeeds or not. Yet it does not take ownership of the array
 *   itself.
 * * `parameters_len` number of elements in parameters.
 * * `max_text_size` optional upper bound for the size of text columns. Use `0` to indicate that no
 *   uppper bound applies.
 * * `max_binary_size` optional upper bound for the size of binary columns. Use `0` to indicate
 *   that no uppper bound applies.
 * * `fallibale_allocations`: `TRUE` if allocations should return an error, `FALSE` if it is fine
 *   to abort the process. Enabling might have a performance overhead, so it might be desirable to
 *   disable it, if you know there is enough memory available.
 * * `reader_out` in case of success this will point to an instance of `ArrowOdbcReader`.
 *   Ownership is transferred to the caller.
 */
struct ArrowOdbcError *arrow_odbc_reader_make(struct OdbcConnection *connection,
                                              const uint8_t *query_buf,
                                              uintptr_t query_len,
                                              uintptr_t batch_size,
                                              struct ArrowOdbcParameter *const *parameters,
                                              uintptr_t parameters_len,
                                              uintptr_t max_text_size,
                                              uintptr_t max_binary_size,
                                              bool fallibale_allocations,
                                              struct ArrowOdbcReader **reader_out);

/**
 * Frees the resources associated with an ArrowOdbcReader
 *
 * # Safety
 *
 * `reader` must point to a valid ArrowOdbcReader.
 */
void arrow_odbc_reader_free(struct ArrowOdbcReader *reader);

/**
 * # Safety
 *
 * * `reader` must be valid non-null reader, allocated by [`arrow_odbc_reader_make`].
 * * `array_out` and `schema_out` must both point to valid pointers, which themselves may be null.
 */
struct ArrowOdbcError *arrow_odbc_reader_next(struct ArrowOdbcReader *reader,
                                              void *array,
                                              void *schema,
                                              int *has_next_out);

/**
 * Retrieve the associated schema from a reader.
 */
struct ArrowOdbcError *arrow_odbc_reader_schema(struct ArrowOdbcReader *reader, void *out_schema);

/**
 * Frees the resources associated with an ArrowOdbcWriter
 *
 * # Safety
 *
 * `writer` must point to a valid ArrowOdbcReader.
 */
void arrow_odbc_writer_free(struct ArrowOdbcWriter *writer);

/**
 * Creates an Arrow ODBC writer instance.
 *
 * Takes ownership of connection even in case of an error.
 *
 * # Safety
 *
 * * `connection` must point to a valid OdbcConnection. This function takes ownership of the
 *   connection, even in case of an error. So The connection must not be freed explicitly
 *   afterwards.
 * * `table_buf` must point to a valid utf-8 string
 * * `table_len` describes the len of `table_buf` in bytes.
 * * `schema` pointer to an arrow schema.
 * * `writer_out` in case of success this will point to an instance of `ArrowOdbcWriter`. Ownership
 *   is transferred to the caller.
 */
struct ArrowOdbcError *arrow_odbc_writer_make(struct OdbcConnection *connection,
                                              const uint8_t *table_buf,
                                              uintptr_t table_len,
                                              uintptr_t chunk_size,
                                              const void *schema,
                                              struct ArrowOdbcWriter **writer_out);

/**
 * # Safety
 *
 * * `writer` must be valid non-null writer, allocated by [`arrow_odbc_writer_make`].
 * * `batch` must be a valid pointer to an arrow batch
 */
struct ArrowOdbcError *arrow_odbc_writer_write_batch(struct ArrowOdbcWriter *writer,
                                                     void *array_ptr,
                                                     void *schema_ptr);

/**
 * # Safety
 *
 * * `writer` must be valid non-null writer, allocated by [`arrow_odbc_writer_make`].
 */
struct ArrowOdbcError *arrow_odbc_writer_flush(struct ArrowOdbcWriter *writer);
