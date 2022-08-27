# Changelog

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
