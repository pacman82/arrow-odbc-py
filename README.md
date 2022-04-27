# arrow-odbc-py

[![Licence](https://img.shields.io/crates/l/arrow-odbc)](https://github.com/pacman82/arrow-odbc-py/blob/master/License)
[![PyPI version](https://badge.fury.io/py/arrow-odbc.svg)](https://pypi.org/project/arrow-odbc/)

Fill Apache Arrow arrays from ODBC data sources. This crate is build on top of the [`pyarrow`](https://pypi.org/project/arrow/) Python package and [`arrow-odbc`](https://crates.io/crates/arrow-odbc) Rust crate and enables you to read the data of an ODBC data source as sequence of Apache Arrow record batches.

This package has been designed to be easily deployable, so it provides a prebuild many linux wheel which is independent of the specific version of your Python interpreter and the specific Arrow Version you want to use. It will dynamically link against the ODBC driver manager provided by your system.

Users looking for more features than just bulk fetching data from ODBC data sources in Python should also take a look at [`turbodbc`](https://github.com/blue-yonder/turbodbc) which has a helpful community and seen a lot of battle testing. This Python package is more narrow in Scope (which is a fancy way of saying it has less features), as it is only concerned with bulk fetching Arrow Arrays. `turbodbc` may be harder to install using `pip` though, due to it's reliance on C++ API and external dependencies like `boost`.

## About Arrow

> [Apache Arrow](https://arrow.apache.org/) defines a language-independent columnar memory format for flat and hierarchical data, organized for efficient analytic operations on modern hardware like CPUs and GPUs. The Arrow memory format also supports zero-copy reads for lightning-fast data access without serialization overhead.

## About ODBC

[ODBC](https://docs.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc) (Open DataBase Connectivity) is a standard which enables you to access data from a wide variaty of data sources using SQL.

## Usage

```python
from arrow_odbc import read_arrow_batches_from_odbc

connection_string="Driver={ODBC Driver 17 for SQL Server};Server=localhost;"

reader = read_arrow_batches_from_odbc(
    query=f"SELECT * FROM MyTable WHERE a=?",
    connection_string=connection_string,
    batch_size=1000,
    parameters=["I'm a positional query parameter"],
    user="SA",
    password="My@Test@Password",
)

for batch in reader:
    # Process arrow batches
    df = batch.to_pandas()
    # ...
```

## Installation

### Installing ODBC driver manager

The provided wheels dynamically link against the driver manager, which must be provided by the system.

#### Windows

Nothing to do. ODBC driver manager is preinstalled.

#### Ubuntu

```shell
sudo apt-get install unixodbc-dev
```

#### OS-X

You can use homebrew to install UnixODBC

```shell
brew install unixodbc
```

### Installing Rust toolchain

Note: **Only required if building from source**

To build from source you need to install the Rust toolchain. Installation instruction can be found here: <https://www.rust-lang.org/tools/install>

### Installing the wheel

Wheels have been uploaded to [PyPi](https://pypi.org) and can be installed using pip. The wheel (including the manylinux wheel) will link against the your system ODBC driver manager at runtime. If there are no prebuild wheels for your platform, you can build the wheel from source. For this the rust toolchain must be installed.

```shell
pip install arrow-odbc
```

`arrow-odbc` utilizes `cffi` and the Arrow C-Interface to glue Rust and Python code together. Therefore the wheel does not need to be build against the precise version either of Python or Arrow.

## Matching of ODBC to Arrow types

| ODBC               | Arrow                |
| ------------------ | -------------------- |
| Numeric(p <= 38)   | Decimal              |
| Decimal(p <= 38)   | Decimal              |
| Integer            | Int32                |
| SmallInt           | Int16                |
| Real               | Float32              |
| Float(p <=24)      | Float32              |
| Double             | Float64              |
| Float(p > 24)      | Float64              |
| Date               | Date32               |
| LongVarbinary      | Binary               |
| Timestamp(p = 0)   | TimestampSecond      |
| Timestamp(p: 1..3) | TimestampMilliSecond |
| Timestamp(p: 4..6) | TimestampMicroSecond |
| Timestamp(p >= 7 ) | TimestampNanoSecond  |
| BigInt             | Int64                |
| TinyInt            | Int8                 |
| Bit                | Boolean              |
| Varbinary          | Binary               |
| Binary             | FixedSizedBinary     |
| All others         | Utf8                 |
