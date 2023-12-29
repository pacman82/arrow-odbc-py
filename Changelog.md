# Changelog

## 2.1.1

- Change default `max_bytes_per_batch`` from 2MiB to 256MiB.

## 2.1.0

- Allow overwriting automatically deduced arrow schemas through optional schema arguments.

## 2.0.5

- Updated Rust dependencies. This includes an update to `arrow-odbc` (the Rust crate) which fixes a panic occuring if the database returns column names in non UTF-8 encodings on non-windows platforms. This version will raise an exception instead.

## 2.0.4

- Updated Rust dependencies.
- In order to work with mandatory columns workaround for IBM DB2 returning memory garbage now no longer maps empty strings to zero.

## 2.0.3

- Updated Rust dependencies. Use terminating zeros instead of indicators to determine string length, if the database management system name is reported to be 'DB2/LINUX'. This is to work around a bug in the IBM ODBC driver which returns garbage memory as indicators.

## 2.0.2

- Updated Rust dependencies. Including an update to `arrow-odbc` (the Rust crate) which fixes an issue which would cause a panic if extracting JSON from a MySQL database.

## 2.0.1

- Updated Rust dependencies. Including an update to `arrow-odbc` (the Rust crate) which fixes an issue which would cause a division by zero if iterating using `more_results` over several SQL statements, there one of them would not be a "true" cursor, i.e. not emitting a result set, or having an empty schema.

## 2.0.0

- Transit buffers are now limited to 2GiB by default. You can use the `max_bytes_per_batch` parameter to adjust or deactivate that limit.
- `read_arrow_batches_from_odbc` has the order of its arguments changed, so `batch_size` can have a default argument.
- `fallibale_allocations` now defaults to `False`, as due to the size limits, it would rarerly trigger and it can have huge potential performance implications.

## 1.3.2

- Updated rust dependencies
- Error message indicating that an update of unixODBC could help if failing to create an environment.

## 1.3.1

- Then failing to create an environment `arrow-odbc` is no longer going to panic and emit an exception instead.

## 1.3.0

- Add method `fetch_concurrently` to `BatchReader` allowing for fetching batches concurrently from the ODBC data source at the price of an additional transit buffer.

## 1.2.8

- Debug logging around the inspection of relational types using the ODBC driver.

## 1.2.7

- Update Rust dependencies. Including an update to `odbc-api 2.0.0`, which provides more details on truncation errors.

## 1.2.6

- Update Rust dependencies. Including an update to `odbc-api 1.0.1`, which contains a fix preventing false positive truncation errors in the presence of `NULL` values and ODBC diagnostics. This bugfix fixes the error in variadic text fields theras the previous one only fixed the issue for variadic binary fields.

## 1.2.5

- Update Rust dependencies. Including an update to `odbc-api 1.0.1`, which contains a fix preventing false positive truncation errors in the presence of `NULL` values and ODBC diagnostics.

## 1.2.4

- Update Rust dependencies
- Better error message if passing a non-string argument using `parameters` into `read_arrow_batches_from_odbc`.

## 1.2.3

- Update Rust dependencies. Includes update to arrow-odbc 0.28.12, which does not add a semicolon at the end of INSERT statements any more in an effort to increase compatbility with IBM db2.

## 1.2.2

- Update Rust dependencies. Includes update to arrow-odbc 0.28.11 which raises an error in case timestamp with nano seconds precision are outside of the valid range.

## 1.2.1

- Update Rust dependencies. Includes update to arrow-odbc 0.28.9 which forwards the original error message from the ODBC driver in situtations there it has been previously hidden by error translation.

## 1.2.0

- Introduce `enable_odbc_connection_pooling` to allow for reducing overhead then creating "new" connections.

## 1.1.3

- Update Rust dependencies

## 1.1.2

- Declare minimal version in pyproject.toml

## 1.1.1

- Fix version number in documentation.

## 1.1.0

- Update Rust dependencies
- Adds `log_to_stderr` for emitting diagnostics directly to standard output.

## 1.0.1

- More code examples in Docstrings

## 1.0.0

- Breaking change: `read_arrow_batches_from_odbc` now also returns a batch reader instead of `None`, even if the SQL statement did not produce a result set. The resulting reader will be empty, i.e. iterating over batches stops immediatly. The assaciated schema attribute will contain no columns.
- Support for querying stored procedures returning multiple result sets is added. Call `more_results` on the reader to advance to the next result set.

## 0.3.14

- Update Rust dependencies
- Add `from_table_to_db` to better support insertion directly from an arrow header rather than a record batch reader.

## 0.3.13

- Support for inserting large binary strings

## 0.3.12

- Fix: If an error occurrs during inintalizing the writer it is now correctly translated into a python exception interrupting the control flow befor accessing an invalid writer object. Before this fix an error would have caused an invalid memory access violation.

## 0.3.11

- Fix manylinux wheel build

## 0.3.10

- Support for explicit login timeout in seconds via the `login_timeout_sec` parameter in both `read_arrow_batches_from_odbc` and `insert_into_table`.
- Updated Rust dependencies

## 0.3.9

- Updated Rust dependencies
  - This includes an update to `odbc-api 0.54.0`. This avoids escalating into an error if a query emits at least 32767 warnings.

## 0.3.8

- Fix: Be resilient against ineterior `Nul`s in error messages.

## 0.3.7

- Updated Rust dependencies
- Better error message in case the ODBC driver emits more than 32767 diagnostic records then fetching data.

## 0.3.6

- Rerelease due to failed build of windows wheel.
- Update Rust dependencies

## 0.3.5

- Update Rust dependencies

## 0.3.4

- Update Rust dependencies

## 0.3.3

- Update Rust dependencies

## 0.3.2

- Disabled arrow default features again for leaner build. They had been enabled to workaround an upstream bug, there the `ffi` feature has not been self reliant. This update uses a smaller workaround and only enables the `ipc` feature in addition to `ffi`.

## 0.3.1

- Update Rust dependencies

## 0.3.0

- Use narrow strings on non-windows platforms. Assumes Sytem locale with UTF-8.

## 0.2.6

- Docs are now available on <http://arrow-odbc.readthedocs.org>

## 0.2.5

- `BatchWriter` is now "public" in top level scope.
- Test release pipeline using `maturin` instead of `setuptools` with `milksnake`.
- Fix: Non-linux wheels are no longer tagged to support Python 2

## 0.2.4

- Update Rust dependencies

## 0.2.3

- Update Rust dependencies

## 0.2.2

- Support for inserting `Decimal256`

## 0.2.1

- Updated Rust dependencies

## 0.2.0

- Support for inserting record batches into a database table
- `BatchReader` now exposes `schema` as an attribute rather than a function.

## 0.1.23

- Updated Rust dependencies

## 0.1.22

- Updated Rust dependencies

## 0.1.21

- Add paramater `falliable_allocations` to give users the option to opt out of falliable allocations, and potential performance overhead.

## 0.1.20

- Support specifying an upper limit for text and binary columns. This allows to circumvent allocation and or zero sized column errors.

## 0.1.19

- Raise exception in case buffer allocation fails instead of panicing.

## 0.1.18

- Updated Rust dependencies

## 0.1.17

- Updated Rust dependencies
  
## 0.1.16

- Updated Rust dependencies

## 0.1.15

- Updated Rust dependencies

## 0.1.14

- Support for positional query parameters
- Updated dependencies

## 0.1.13

- Improved error messages.

## 0.1.12

Fix: There had been an issue, there the correct binary size for the longest possible text has been underestimated on Linux then querying a column with UTF-8 encoded characters.

## 0.1.11

- Fix: There had been an issue, there the correct binary size for the longest possible text has been underestimated on Linux then querying a column with UTF-16 encoded characters.
- Fix: There had been an issue, there the correct binary size for the longest possible text has been underestimated on Windown then querying a column with UTF-8 encoded characters.

## 0.1.10

- Allow specifying user and password seperatly from the connection string.

## 0.1.9

- Fix: Manylinux wheel are now build against a recent version of Unix ODBC and link against the `libodbc.so` provided by the system.

## 0.1.8

Fix: An upstream issue causing overflows for `timestamp['us']` has been fixed.

## 0.1.7

- Fix: `BatchReader.__next__()` now returns `pyarrow.RecordBatch`. Previous version returned a `StructArray`.
- Fix: `BatchReader.schema()` now return `pyarrow.Schema` instead of `pyarrow.Datatype`.

## 0.1.6

Replace maturin with milksnake

## 0.1.5

- Fix windows wheel

## 0.1.4

- Fixed a memory leak, than iterating over batches.

## 0.1.1-3

Test Release process for Wheels

## 0.1.0

Initial release
