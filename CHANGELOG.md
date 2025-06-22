# Changelog

## 9.1.0

- Update of upstream `arrow-odbc` Rust crate. Includes support for inserting timestamps with time zones.

## 9.0.0

- Map SQL type `TIME` to arrow `time32` or `time64` depending on the precision.

## 8.3.9

- fix: Pyproject.toml now specifies Python 3.9 as the oldest supported Python version. With introduction of UV this had been wrongly set to 3.10.

## 8.3.2-8

Same as 8.3.1. Release triggered again, due to failure in publishing wheels.

## 8.3.1

- Updated Rust dependencies. This includes an update of `odbc-api`. With this arrow-odbc can now insert text larger than 4000 characters into an Microsoft SQL Server, if on Windows.

## 8.3.0

- Add support for selecting wide or narrow encoding for transporting text data to the application explicitly. For this the `TextEncoding` enumration can be used and its variants passed to `payload_text_encoding` parameter of `read_arrow_batches_from_odbc`.

## 8.2.0

- Add support for query timeouts, introducing a new `query_timeout_sec` parameter to `read_arrow_batches_from_odbc`.

## 8.1.1

- Fix: Upload for manylinux arm wheel to pypi

## 8.1.0

- Publish Manylinux wheel for ARM architecture (aarch64) to pypi

## 8.0.9

- Fix: Licenese metadata in in `pyproject.toml` now states `licenese="MIT"` instead of specifying a file path.

## 8.0.8

- Fix: SHA256 has for WHEEL file in the many-linux wheel dist info had been wrong, due to manually tinkering with the wheel metadata in the build process. The step is no migrated to `auditwheel`. Thanks to @rupurt for spotting the issue.

## 8.0.7

- Fix: build wheel for Mac OS on x86-64 architecture

## 8.0.6

- Due to a typo `8.0.3` had been released as `8.0.5`. `8.0.6` releases without any changes, and realigns version number of Changelog, wheel and git tag.

## 8.0.3 & 5

- Fix: Update to latest `arrow-odbc 14.0.1`. Inserting multiple small batches now works, even if the second batch triggers rebinding the buffer due to element size. Previously in this scenario not all values already inserted were correctly copied into the new buffer. This caused strings to be replaced with `null` bytes.

## 8.0.2

- Fix: Due to fixes in the upstream `arrow-odbc` Rust crate it is now possible to fetch timestamps before unix-epoch without a panic

## 8.0.1

- Fix: `typing_extension` has been pinned to strictly, preventing users from updating to the newest version.

## 8.0.0

- Parameter `fetch_concurrently` now defaults to `True`. This causes `arrow-odbc-py` to use more memory by default, but enables fetching concurrently with the rest of the application logic. You can set this parameter explicitly to `False` to get the old behaviour.
- Removed deprecated method `fetch_concurrently`. Use the paremeter instead.

## 7.1.0

- Deprecate method `fetch_concurrently` in favour of dedicated parameters in `read_arrow_batches_from_odbc` and `more_results`. This is intended to increase discoverability of the feature.

## 7.0.7-8

- Fix build for Mac ARM wheel. It seems like fly-ci runners do no longer pick up the job, switching to GitHub runners.

## 7.0.6

- Fix: Typo in connect function, lead to connections not properly established and failing opertions.

## 7.0.5

- Updated Rust dependencies

## 7.0.1-4

- Build wheel for MacOS 13 x86

## 7.0.0

- unsigned TinyInt is now mapped to `UInt8`.

## 6.0.0

- Support for passing desired packet size to the ODBC driver. This may help with packet loss on fragile connections if the ODBC driver supports it.
- `insert_into_table` will now wrap column names in double quotes then creating the statement, if the column name is not a valid identifier in transact SQL.

## 5.0.0

- Fix: Database connection have not been cleaned up in case the parameters caused a type error.
- Changend `BatchReader.to_pyarrow_record_batch_reader` into `BatchReader.into_pyarrow_record_batch_reader`. The new method fully passes ownership and leaves self empty.

## 4.2.0

- Adds `BatchReader.to_pyarrow_record_batch_reader` in order to avoid for conviniently create a PyArrow `RecordBatchReader` which can be consumed by other libraries like DuckDB.

## 4.1.0

- Release wheel also for MacOS ARM 64 architectures to PyPi. Thanks to [FlyCI](https://www.flyci.net/) for their free tier!

## 4.0.0

- Removed parameter `driver_returns_memory_garbage_for_indicators` from `read_arrow_batches_from_odbc` as it was intended as a workaround for IBM/DB2 drivers. Turns out IBM offers drivers which work correctly with 64Bit driver managers. Look for file names ending in 'o'.
- Add support for mapping inferred schemas via the `map_schema` parameter. It accepts a callable taking and returning an Arrow Schema. This allows you to avoid data types which are not supported in downstream operations and map them e.g. to string. It also enables you to work around quirks of your ODBC driver and map float 32 to float 64 if precisions inferred by the driver are too small.

## 3.0.1

- Updated Rust dependencies. This includes an update to `odbc-api` which fixes a bug in decimal parsing. Decimal values with a magnitude smaller 1 and negative sign would have been interpreted as positive without this fix.

## 3.0.0

- `read_arrow_batches_from_odbc` now requires the paramteter `driver_returns_memory_garbage_for_indicators` explicitly set to `True` in order to compensate for weaknesses in the IBM/DB2 driver and no longer tries to autodetect this. This change in interface has been made in order to not accidentially apply the workaround to drivers which in actuallity report indices just fine.

## 2.1.6

- `BatchReader.more_results` now shares the same default value for fetch buffer size as `read_arrow_batches_from_odbc`.

## 2.1.5

- Updated Rust dependencies. This includes an update to `odbc-api` which activates db2 specific workaround for any platform

## 2.1.4

- Updated Rust dependencies. This includes an update to `arrow-odbc` (the Rust crate) which features a more robust parsing of decimals. Decimals now work even if they do not have all trailing zeroes in their text representation.

## 2.1.3

- Updated Rust dependencies. This includes an update to `arrow-odbc` (the Rust crate) which features a more robust parsing of decimals. Decimals now work even if they are rendered with a comma `,` as a radix character instead of a decimal point (`.`).

## 2.1.2

- Better comments for `max_text_size`

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
