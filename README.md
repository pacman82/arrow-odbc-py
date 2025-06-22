# arrow-odbc-py

[![Licence](https://img.shields.io/crates/l/arrow-odbc)](https://github.com/pacman82/arrow-odbc-py/blob/master/LICENSE)
[![PyPI version](https://badge.fury.io/py/arrow-odbc.svg)](https://pypi.org/project/arrow-odbc/)
[![Documentation Status](https://readthedocs.org/projects/arrow-odbc/badge/?version=latest)](https://arrow-odbc.readthedocs.io/en/latest/?badge=latest)

Fill Apache Arrow arrays from ODBC data sources. This package is build on top of the [`pyarrow`](https://pypi.org/project/arrow/) Python package and [`arrow-odbc`](https://crates.io/crates/arrow-odbc) Rust crate and enables you to read the data of an ODBC data source as sequence of Apache Arrow record batches.

* **Fast**. Makes efficient use of ODBC bulk reads and writes, to lower IO overhead.
* **Flexible**. Query any ODBC data source you have a driver for. MySQL, MS SQL, Excel, ...
* **Portable**. Easy to install and update dependencies. No binary dependency to specific implemenations of Python interpreter, Arrow or ODBC driver manager.

## About Arrow

> [Apache Arrow](https://arrow.apache.org/) defines a language-independent columnar memory format for flat and hierarchical data, organized for efficient analytic operations on modern hardware like CPUs and GPUs. The Arrow memory format also supports zero-copy reads for lightning-fast data access without serialization overhead.

## About ODBC

[ODBC](https://docs.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc) (Open DataBase Connectivity) is a standard which enables you to access data from a wide variaty of data sources using SQL.

## Usage

### Query

```python
from arrow_odbc import read_arrow_batches_from_odbc

connection_string="Driver={ODBC Driver 18 for SQL Server};Server=localhost;TrustServerCertificate=yes;"

reader = read_arrow_batches_from_odbc(
    query=f"SELECT * FROM MyTable WHERE a=?",
    connection_string=connection_string,
    parameters=["I'm a positional query parameter"],
    user="SA",
    password="My@Test@Password",
)

for batch in reader:
    # Process arrow batches
    df = batch.to_pandas()
    # ...
```

### Insert

```python
from arrow_odbc import insert_into_table
import pyarrow as pa
import pandas


def dataframe_to_table(df):
    table = pa.Table.from_pandas(df)
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    insert_into_table(
        connection_string=connection_string,
        user="SA",
        password="My@Test@Password",
        chunk_size=1000,
        table="MyTable",
        reader=reader,
    )
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

### Installing the wheel

This package has been designed to be easily deployable, so it provides a prebuild many linux wheel which is independent of the specific version of your Python interpreter and the specific Arrow Version you want to use. It will dynamically link against the ODBC driver manager provided by your system.

Wheels have been uploaded to [`PyPi`](https://pypi.org/project/arrow-odbc/) and can be installed using pip. The wheel (including the manylinux wheel) will link against the your system ODBC driver manager at runtime. If there are no prebuild wheels for your platform, you can build the wheel from source. For this the rust toolchain must be installed.

```shell
pip install arrow-odbc
```

`arrow-odbc` utilizes `cffi` and the Arrow C-Interface to glue Rust and Python code together. Therefore the wheel does not need to be build against the precise version either of Python or Arrow.

### Installing with conda

```shell
conda install -c conda-forge arrow-odbc
```

**Warning:** The conan recipie is currently unmaintained. So to install the newest version you need to either install from source or use a wheel deployed via pip.

### Building wheel from source

There is no ready made wheel for the platform you want to target? Do not worry, you can probably build it from source.

* To build from source you need to install the Rust toolchain. Installation instruction can be found here: <https://www.rust-lang.org/tools/install>
* Install ODBC driver manager. See above.
* Build wheel

  ```shell
  python -m pip install build
  python -m build
  ```

## Matching of ODBC to Arrow types then querying

| ODBC                     | Arrow                |
| ------------------------ | -------------------- |
| Numeric(p <= 38)         | Decimal128           |
| Decimal(p <= 38, s >= 0) | Decimal128           |
| Integer                  | Int32                |
| SmallInt                 | Int16                |
| Real                     | Float32              |
| Float(p <=24)            | Float32              |
| Double                   | Float64              |
| Float(p > 24)            | Float64              |
| Date                     | Date32               |
| LongVarbinary            | Binary               |
| Time(p = 0)              | Time32Second         |
| Time(p = 1..3)           | Time32Millisecond    |
| Time(p = 4..6)           | Time64Microsecond    |
| Time(p = 7..9)           | Time64Nanosecond     |
| Timestamp(p = 0)         | TimestampSecond      |
| Timestamp(p: 1..3)       | TimestampMilliSecond |
| Timestamp(p: 4..6)       | TimestampMicroSecond |
| Timestamp(p >= 7 )       | TimestampNanoSecond  |
| BigInt                   | Int64                |
| TinyInt Signed           | Int8                 |
| TinyInt Unsigned         | UInt8                |
| Bit                      | Boolean              |
| Varbinary                | Binary               |
| Binary                   | FixedSizedBinary     |
| All others               | Utf8                 |

## Matching of Arrow to ODBC types then inserting

| Arrow                 | ODBC               |
| --------------------- | ------------------ |
| Utf8                  | VarChar            |
| Decimal128(p, s = 0)  | VarChar(p + 1)     |
| Decimal128(p, s != 0) | VarChar(p + 2)     |
| Decimal128(p, s < 0)  | VarChar(p - s + 1) |
| Decimal256(p, s = 0)  | VarChar(p + 1)     |
| Decimal256(p, s != 0) | VarChar(p + 2)     |
| Decimal256(p, s < 0)  | VarChar(p - s + 1) |
| Int8                  | TinyInt            |
| Int16                 | SmallInt           |
| Int32                 | Integer            |
| Int64                 | BigInt             |
| Float16               | Real               |
| Float32               | Real               |
| Float64               | Double             |
| Timestamp s           | Timestamp(7)       |
| Timestamp ms          | Timestamp(7)       |
| Timestamp us          | Timestamp(7)       |
| Timestamp ns          | Timestamp(7)       |
| Timestamp with Tz s   | VarChar(25)        |
| Timestamp with Tz ms  | VarChar(29)        |
| Timestamp with Tz us  | VarChar(32)        |
| Timestamp with Tz ns  | VarChar(35)        |
| Date32                | Date               |
| Date64                | Date               |
| Time32 s              | Time               |
| Time32 ms             | VarChar(12)        |
| Time64 us             | VarChar(15)        |
| Time64 ns             | VarChar(16)        |
| Binary                | Varbinary          |
| FixedBinary(l)        | Varbinary(l)       |
| All others            | Unsupported        |

## Comparision to other Python ODBC bindings

* [`pyodbc`](https://github.com/mkleehammer/pyodbc) - General purpose ODBC python bindings. In contrast `arrow-odbc` is specifically concerned with bulk reads and writes to arrow arrays.
* [`turbodbc`](https://github.com/blue-yonder/turbodbc) - Complies with the Python Database API Specification 2.0 (PEP 249) which `arrow-odbc` does not aim to do. Like `arrow-odbc` bulk read and writes is the strong point of `turbodbc`. `turbodbc` has more system dependencies, which can make it cumbersome to install if not using conda. `turbodbc` is build against the C++ implementation of Arrow, which implies it is only compatible with matching version of `pyarrow`.
