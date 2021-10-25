#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

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
 */
struct OdbcConnection *arrow_odbc_connect_with_connection_string(const uint8_t *connection_string_buf,
                                                                 uintptr_t connection_string_len);
