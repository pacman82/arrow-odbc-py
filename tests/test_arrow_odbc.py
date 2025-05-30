import datetime
import os

import pyarrow as pa
import pyarrow.parquet as pq

import duckdb
import pytest
import pyodbc

from typing import List, Any
from subprocess import check_output

from pytest import raises

from arrow_odbc import (
    insert_into_table,
    from_table_to_db,
    read_arrow_batches_from_odbc,
    log_to_stderr,
    enable_odbc_connection_pooling,
    Error,
    TextEncoding,
)

MSSQL = "Driver={ODBC Driver 18 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;\
    TrustServerCertificate=yes;"

log_to_stderr()
enable_odbc_connection_pooling()


def setup_table(table: str, column_type: str, values: List[Any]):
    connection = pyodbc.connect(MSSQL)
    connection.execute(f"DROP TABLE IF EXISTS {table};")
    connection.execute(f"CREATE TABLE {table} (a {column_type});")
    for value in values:
        connection.execute(f"INSERT INTO {table} (a) VALUES (?);", value)
    connection.commit()
    connection.close()


def empty_table(table, column_type):
    """
    Create an empty table as a precondition for a test. The table will have an identy column (id)
    and an additional column of the custom type with typename a
    """
    connection = pyodbc.connect(MSSQL)
    connection.execute(f"DROP TABLE IF EXISTS {table};")
    connection.execute(f"CREATE TABLE {table} (id int IDENTITY(1,1), a {column_type});")
    connection.commit()
    connection.close()


def test_connection_options():
    """
    Just a smoke test, that we did not mess up passing the arguments for the connections over the
    c-interface.
    """
    read_arrow_batches_from_odbc(
        query="SELECT 1 AS a", connection_string=MSSQL, login_timeout_sec=2, packet_size=4096
    )


def test_should_report_error_on_invalid_connection_string_reading():
    """
    We want to forward the original ODBC errors to the end user. Of course foo
    is not a valid connection string. Therefore we want to see the creation of
    this connection fail, but with a nice error.
    """
    # Error on windows: Data source name not found and no default driver specified
    # Erron on linux: Data source name not found, and no default driver specified
    # We assert on less, so we don't care about the comma (,)
    with raises(Error, match="Data source name not found"):
        read_arrow_batches_from_odbc(
            query="SELECT * FROM Table", batch_size=100, connection_string="foo"
        )


def test_should_report_error_on_invalid_query():
    """
    We want the user to know why a query failed.
    """

    # 'Foo' does not exist in the datasource
    query = "SELECT * FROM Foo"

    with raises(Error, match="Invalid object name 'Foo'"):
        read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)


def test_no_result_set():
    """
    BatchReader should be be empty if no result set can be produced
    """
    table = "EmptyResult"
    setup_table(table=table, column_type="int", values=[])

    # This statement does not produce a result set
    query = f"INSERT INTO {table} (a) VALUES (42);"
    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)

    assert reader.schema == pa.schema([])
    with raises(StopIteration):
        next(iter(reader))


def test_skip_to_second_result_set():
    """
    Calling `more_results` should allow to consume the next result set
    """
    # This statement produces two result sets
    query = "SELECT 1 AS a; SELECT 2 AS b;"

    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)

    # Skip to second result set
    reader.more_results(batch_size=100)

    # Process second result
    schema = pa.schema([pa.field("b", pa.int32(), nullable=False)])
    assert reader.schema == schema
    expected = pa.RecordBatch.from_pydict({"b": [2]}, schema)
    assert expected == next(iter(reader))
    with raises(StopIteration):
        next(iter(reader))


def test_more_results_return_should_indicate_if_there_is_a_result_set():
    """
    Calling `more_results` should return a boolean indicating wether there is another result set or
    not to be extracted
    """
    # This statement produces two result sets
    query = "SELECT 1 AS a; SELECT 2 AS b;"

    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)

    assert reader.more_results(batch_size=100)
    assert not reader.more_results(batch_size=100)


def test_custom_schema_for_second_result_set():
    """
    Generate two result sets. Fetch the second of the two as text using a custom schema.
    """
    # This statement produces two result sets
    query = "SELECT 1 AS a; SELECT 2 AS a;"

    reader = read_arrow_batches_from_odbc(query=query, batch_size=1, connection_string=MSSQL)
    # Ignore first result and use second straight away
    schema = pa.schema([pa.field("a", pa.string())])
    reader.more_results(batch_size=1, schema=schema)
    batch = next(iter(reader))

    expected = pa.RecordBatch.from_pydict({"a": ["2"]}, schema)
    assert batch == expected


def test_advancing_past_last_result_set_leaves_empty_reader():
    """
    Moving past the last result set, leaves a reader returning a schema with no columns and no
    batches.
    """
    # This statement produces one result
    query = "SELECT 1 AS a;"

    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)
    # Move to a second result set, which does not exist
    reader.more_results(batch_size=100)

    # Assert schema and batches are empty
    assert reader.schema == pa.schema([])
    with raises(StopIteration):
        next(iter(reader))


def test_making_an_empty_reader_concurrent_is_no_error():
    """
    Making an empty reader, which has been moved past the last result set, concurrent has no effect.
    """
    # This statement produces one result
    query = "SELECT 1 AS a;"

    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)
    # Move to a second result set, which does not exist
    reader.more_results(batch_size=100, fetch_concurrently=True)

    # Assert schema and batches are empty
    assert reader.schema == pa.schema([])
    with raises(StopIteration):
        next(iter(reader))


def test_empty_table():
    """
    Should return an empty iterator querying an empty table.
    """
    table = "Empty"
    setup_table(table=table, column_type="int", values=[])

    query = f"SELECT * FROM {table}"

    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)

    with raises(StopIteration):
        next(iter(reader))


def test_one_row():
    """
    Query a table with one row. Should return one batch
    """
    table = "OneRow"
    setup_table(table=table, column_type="int", values=["42"])

    query = f"SELECT * FROM {table}"

    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)
    it = iter(reader)

    actual = next(it)

    schema = pa.schema([("a", pa.int32())])
    expected = pa.RecordBatch.from_pydict({"a": [42]}, schema)
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_fetch_concurrently():
    """
    Use a concurrent batch reader to fetch one row
    """
    table = "FetchConcurrently"
    setup_table(table=table, column_type="int", values=["42"])

    query = f"SELECT * FROM {table}"

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL, fetch_concurrently=True
    )
    it = iter(reader)

    actual = next(it)

    schema = pa.schema([("a", pa.int32())])
    expected = pa.RecordBatch.from_pydict({"a": [42]}, schema)
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_fetch_sequential():
    """
    Use a sequential batch reader to fetch one row
    """
    table = "FetchConcurrently"
    setup_table(table=table, column_type="int", values=["42"])

    query = f"SELECT * FROM {table}"

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL, fetch_concurrently=False
    )
    it = iter(reader)

    actual = next(it)

    schema = pa.schema([("a", pa.int32())])
    expected = pa.RecordBatch.from_pydict({"a": [42]}, schema)
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_schema():
    """
    Query a table. Reader should have a member indicating the correct schema
    """
    table = "TestSchema"
    connection = pyodbc.connect(MSSQL)
    connection.execute(f"DROP TABLE IF EXISTS {table};")
    connection.execute(f"CREATE TABLE {table} (a INT, b VARCHAR);")
    connection.commit()
    connection.close()

    query = f"SELECT * FROM {table}"
    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)

    expected = pa.schema([("a", pa.int32()), ("b", pa.string())])
    assert expected == reader.schema


def test_schema_from_concurrent_reader():
    """
    Query a table concurrently. Reader should have a member indicating the correct schema
    """
    table = "TestSchemaFromConcurrentReader"
    connection = pyodbc.connect(MSSQL)
    connection.execute(f"DROP TABLE IF EXISTS {table};")
    connection.execute(f"CREATE TABLE {table} (a INT, b VARCHAR);")
    connection.commit()
    connection.close()

    query = f"SELECT * FROM {table}"
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL, fetch_concurrently=True
    )

    expected = pa.schema([("a", pa.int32()), ("b", pa.string())])
    assert expected == reader.schema


def test_timestamp_us():
    """
    Query a table with one row. Should return one batch
    """
    table = "TimestampUs"
    setup_table(table=table, column_type="DATETIME2(6)", values=["2014-04-14 21:25:42.074841"])

    query = f"SELECT * FROM {table}"
    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)
    it = iter(reader)
    actual = next(it)

    schema = pa.schema([("a", pa.timestamp("us"))])
    expected = pa.RecordBatch.from_pydict({"a": [1397510742074841]}, schema)
    print(expected[0])
    print(actual[0])
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_timestamp_ns():
    """
    Query a table with one row. Should return one batch
    """
    table = "TimestampNs"
    setup_table(table=table, column_type="DATETIME2(7)", values=["2014-04-14 21:25:42.0748412"])

    query = f"SELECT * FROM {table}"
    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)
    it = iter(reader)
    actual = next(it)

    schema = pa.schema([("a", pa.timestamp("ns"))])
    expected = pa.RecordBatch.from_pydict({"a": [1397510742074841200]}, schema)
    print(expected[0])
    print(actual[0])
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_out_of_range_timestamp_ns():
    """
    Query a table with one row. Should return one batch
    """
    table = "OutOfRangeTimestampNs"
    setup_table(table=table, column_type="DATETIME2(7)", values=["2300-04-14 21:25:42.0748412"])

    query = f"SELECT * FROM {table}"

    with raises(
        Error,
        match="Timestamp is not representable in arrow: 2300-04-14 21:25:42.074841200",
    ):
        reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)
        it = iter(reader)
        it.__next__()


def test_time():
    """
    Query a table with with a TIME column on MS. Type detection requires special treatment for
    Microsoft SQL Server, since it does report a custom type (-154) for TIME columns.
    """
    table = "test_time"
    setup_table(table=table, column_type="TIME", values=["12:34:56.1234567"])

    query = f"SELECT * FROM {table}"
    reader = read_arrow_batches_from_odbc(query=query, batch_size=1, connection_string=MSSQL)
    it = iter(reader)
    actual = next(it)

    schema = pa.schema([("a", pa.time64("ns"))])
    expected = pa.RecordBatch.from_pydict({"a": [45296123456700]}, schema)
    print(expected[0])
    print(actual[0])
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_should_map_column_with_accounting_for_dbms():
    """
    Verifies that database specific mapping is accounted for, even than mapping types manually.
    """
    table = "test_should_map_column_with_accounting_for_dbms"
    setup_table(table=table, column_type="TIME", values=["12:34:56.1234567"])

    query = f"SELECT * FROM {table}"
    # We map the columns 'manually' with an identity function, so we can verify what the input
    # schema for the mapping would look like.
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=1, connection_string=MSSQL, map_schema=lambda x: x
    )
    it = iter(reader)
    actual = next(it)

    schema = pa.schema([("a", pa.time64("ns"))])
    expected = pa.RecordBatch.from_pydict({"a": [45296123456700]}, schema)
    print(expected[0])
    print(actual[0])
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_specify_user_and_password_separatly():
    """
    Query a table with one row. Should return one batch
    """

    query = "SELECT 42 as a;"

    # Connection string without credentials
    connection_string = "Driver={ODBC Driver 18 for SQL Server};\
        Server=localhost;TrustServerCertificate=yes;"
    user = "SA"
    password = "My@Test@Password1"

    reader = read_arrow_batches_from_odbc(
        query=query,
        batch_size=100,
        connection_string=connection_string,
        user=user,
        password=password,
    )

    it = iter(reader)
    _result = next(it)
    with raises(StopIteration):
        next(it)


def test_query_char():
    """
    Query a string those UTF-16 representation is larger than the maximum binary column length on
    the database.
    """
    # 'ab' is char(2) => 2 bytes on database. Yet, only one UTF-16 character can fit into 2 bytes.
    query = "SELECT 'ab' as a"
    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)

    it = iter(reader)
    batch = next(it)
    actual = batch.to_pydict()
    expected = {"a": ["ab"]}

    assert expected == actual


def test_query_wchar():
    """
    Query a string those UTF-8 representation is larger than the maximum binary column length on
    the database.
    """
    # '™' is 3 bytes in UTF-8, but only 2 bytes in UTF-16
    query = "SELECT CAST('™' AS NCHAR(1)) as a"
    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)

    it = iter(reader)
    batch = next(it)
    actual = batch.to_pydict()
    expected = {"a": ["™"]}

    assert expected == actual


def test_query_umlaut():
    """
    Query a string those UTF-8 representation is larger in bytes than in
    characters.
    """
    query = "SELECT CAST('Ü' AS VARCHAR(1)) as a"
    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)

    it = iter(reader)
    batch = next(it)
    actual = batch.to_pydict()
    expected = {"a": ["Ü"]}

    assert expected == actual


def test_query_zero_sized_column():
    """
    Query a string those UTF-8 representation is larger in bytes than in
    characters.
    """
    query = "SELECT CAST('a' AS VARCHAR(MAX)) as a"
    with raises(Error, match="ODBC driver did not specify a sensible upper bound for the column"):
        read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)


def test_query_with_string_parameter():
    """
    Use a string parameter in a where clause and verify that the result is
    filtered accordingly
    """
    table = "QueryWithStringParameter"
    connection = pyodbc.connect(MSSQL)
    connection.execute(f"DROP TABLE IF EXISTS {table};")
    connection.execute(f"CREATE TABLE {table} (a CHAR(1), b INTEGER);")
    connection.execute(f"INSERT INTO {table} (a,b) VALUES ('A', 1),('B',2),('C',3),('D',4);")
    connection.commit()
    connection.close()
    query = f"SELECT b FROM {table} WHERE a=?;"

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=10, connection_string=MSSQL, parameters=["B"]
    )
    it = iter(reader)

    actual = next(it)

    schema = pa.schema([("b", pa.int32())])
    expected = pa.RecordBatch.from_pydict({"b": [2]}, schema)
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_query_with_none_parameter():
    """
    Use a string parameter in a where clause and verify that the result is
    filtered accordingly
    """
    table = "QueryWithNoneParameter"
    connection = pyodbc.connect(MSSQL)
    connection.execute(f"DROP TABLE IF EXISTS {table};")
    connection.execute(f"CREATE TABLE {table} (a CHAR(1), b INTEGER);")
    connection.execute(f"INSERT INTO {table} (a,b) VALUES ('A', 1),('B',2),('C',3),('D',4);")
    connection.commit()
    connection.close()

    query = f"SELECT b FROM {table} WHERE a=?;"

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=10, connection_string=MSSQL, parameters=[None]
    )
    it = iter(reader)

    with raises(StopIteration):
        next(it)


def test_query_with_int_parameter():
    """
    Use an int parameter in a where clause and verify that the result is filtered accordingly
    """
    table = "QueryWithIntParameter"
    connection = pyodbc.connect(MSSQL)
    connection.execute(f"DROP TABLE IF EXISTS {table};")
    connection.execute(f"CREATE TABLE {table} (a CHAR(1), b INTEGER);")
    connection.execute(f"INSERT INTO {table} (a,b) VALUES ('A', 1),('B',2),('C',3),('D',4);")
    connection.commit()
    connection.close()

    query = f"SELECT a FROM {table} WHERE #b=?;"
    with raises(
        TypeError,
        match="read_arrow_batches_from_odbc only supports string arguments for SQL query parameters",
    ):
        read_arrow_batches_from_odbc(
            query=query, batch_size=10, connection_string=MSSQL, parameters=[2]
        )


def test_query_timestamp_as_date():
    """
    Query a timestamp as date by providing an arrow schema
    """
    query = "SELECT CAST('2023-12-24' AS DATETIME2) as a"

    schema = pa.schema([("a", pa.date32())])
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=1, connection_string=MSSQL, schema=schema
    )
    it = iter(reader)
    batch = next(it)
    value = batch.to_pydict()

    assert value == {"a": [datetime.date(2023, 12, 24)]}


def test_allocation_erros():
    """
    Avoids unrecoverable allocation errors, if querying an image column
    """
    table = "AllocationError"
    setup_table(table=table, column_type="Image", values=[])

    query = f"SELECT * FROM {table}"

    with raises(Error, match="Column buffer is too large to be allocated."):
        _reader = read_arrow_batches_from_odbc(
            query=query,
            batch_size=1000,
            # Deactivate size limit, so we have an easier time triggering allocation errors
            max_bytes_per_batch=None,
            connection_string=MSSQL,
            falliable_allocations=True,
        )


def test_iris():
    """
    Validate usage works like in the readme
    """
    table = "Iris"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(
        f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (sepal_length REAL, sepal_width REAL, petal_length REAL, petal_width REAL, variety VARCHAR(20) )"'
    )
    os.system(f'odbcsv insert -c "{MSSQL}" -i ./tests/iris.csv {table}')

    query = f"SELECT * FROM {table}"

    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)

    for batch in reader:
        _df = batch.to_pydict()


def test_image():
    """
    Avoids error allocating image column by using casts.
    """
    table = "Image"
    setup_table(table=table, column_type="Image", values=[])
    query = f"SELECT CAST(a as VARBINARY(2048)) FROM {table}"

    _reader = read_arrow_batches_from_odbc(
        query=query,
        batch_size=1000,
        connection_string=MSSQL,
    )


def test_support_varchar_max():
    """
    Support fetching values from a VARCHAR(max) column, by specifying an upper
    bound for the values in it.
    """
    # Given
    table = "SupportVarcharMax"
    setup_table(table=table, column_type="VARCHAR(max)", values=["Hello, World!"])
    query = f"SELECT (a) FROM {table}"

    # When
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=1000, connection_string=MSSQL, max_text_size=1024
    )
    it = iter(reader)
    batch = next(it)

    # Then
    actual = batch.to_pydict()
    expected = {"a": ["Hello, World!"]}

    assert expected == actual


def test_support_varbinary_max():
    """
    Support fetching values from a VARBINARY(max) column, by specifying an upper
    bound for the values in it.
    """
    # Given
    table = "SupportVarbinaryMax"
    setup_table(table=table, column_type="VARBINARY(max)", values=[])
    query = f"SELECT (a) FROM {table}"

    # When
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=1000, connection_string=MSSQL, max_binary_size=1024
    )
    it = iter(reader)

    # Then
    # Implicitly we assert that we could allocate a buffer to hold values for the columns. Better
    # assertion is to check for an inserted value, but inserting binaries is hard with the current
    # test setup.
    with raises(StopIteration):
        next(it)


def test_text_encoding_utf8():
    """
    Smoke test for explicitly choosing narrow encoding
    """
    # Given
    query = "SELECT 'Hello, World!' as a"

    # When
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=1, connection_string=MSSQL, payload_text_encoding=TextEncoding.UTF8
    )
    it = iter(reader)
    batch = next(it)

    # Then
    actual = batch.to_pydict()
    expected = {"a": ["Hello, World!"]}

    assert expected == actual


def test_text_encoding_utf16():
    """
    Smoke test for explicitly choosing wide encoding
    """
    # Given
    query = "SELECT 'Hello, World!' as a"

    # When
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=1, connection_string=MSSQL, payload_text_encoding=TextEncoding.UTF16
    )
    it = iter(reader)
    batch = next(it)

    # Then
    actual = batch.to_pydict()
    expected = {"a": ["Hello, World!"]}

    assert expected == actual


def test_map_f32_to_f64():
    """
    ODBC drivers for PostgreSQL seem to have some trouble reporting the precision of floating point
    types correctly. Using schema mapping users of this wheel which know this quirk can adopt to it
    while still staying generic over the database schema.

    See issue: https://github.com/pacman82/arrow-odbc-py/issues/73
    """
    # Given
    table = "MapF32ToF64"
    # MS driver is pretty good, so we actually create a 32Bit float by setting precision to 17. This
    # way we simulate a driver reporting a too small floating point.
    setup_table(table=table, column_type="Float(17)", values=[])
    query = f"SELECT (a) FROM {table}"

    # When
    def map_schema(schema):
        return pa.schema(
            [
                (
                    name,
                    (
                        pa.float64()
                        if schema.field(name).type == pa.float32()
                        else schema.field(name).type
                    ),
                )
                for name in schema.names
            ]
        )

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=1, connection_string=MSSQL, map_schema=map_schema
    )

    # Then
    expected = pa.schema([("a", pa.float64())])
    assert expected == reader.schema


def test_insert_should_raise_on_invalid_connection_string():
    """
    Insert should raise on invalid connection string
    """
    # Given
    invalid_connection_string = "FOO"
    schema = pa.schema([("a", pa.int64())])

    def iter_record_batches():
        yield pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=schema)

    reader = pa.RecordBatchReader.from_batches(schema, iter_record_batches())

    # When / Then
    with raises(Error, match="Data source name not found"):
        insert_into_table(
            connection_string=invalid_connection_string,
            chunk_size=20,
            table="MyTable",
            reader=reader,
        )


def test_insert_should_raise_on_unsupported_column_type():
    """
    Insert should raise on unsupported column type
    """
    # Given
    schema = pa.schema([("a", pa.dictionary(pa.int32(), pa.int32()))])

    def iter_record_batches():
        yield pa.RecordBatch.from_arrays([pa.array([(1, 1)])], schema=schema)

    reader = pa.RecordBatchReader.from_batches(schema, iter_record_batches())

    # When / Then
    with raises(
        Error,
        match=r"The arrow data type Dictionary\(Int32, Int32\) is not supported for insertion.",
    ):
        insert_into_table(
            connection_string=MSSQL,
            chunk_size=20,
            table="MyTable",
            reader=reader,
        )


def test_insert_batches():
    """
    Insert data into database
    """
    # Given
    table = "InsertBatches"
    empty_table(table, "BIGINT")
    schema = pa.schema([("a", pa.int64())])

    def iter_record_batches():
        for i in range(2):
            yield pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=schema)

    reader = pa.RecordBatchReader.from_batches(schema, iter_record_batches())

    # When
    insert_into_table(connection_string=MSSQL, chunk_size=20, table=table, reader=reader)

    # Then
    actual = check_output(
        ["odbcsv", "fetch", "-c", MSSQL, "-q", f"SELECT a FROM {table} ORDER BY id"]
    )
    assert "a\n1\n2\n3\n1\n2\n3\n" == actual.decode("utf8")


def test_insert_multiple_small_batches():
    """
    Insert multiple batches into the database, using one roundtrip.

    For this test we are sending two batches, each containing one string for the same column. The
    second string is longer than the first one. Is reproduces an issue which occurred in the context
    of chunked arrays.

    See issue: https://github.com/pacman82/arrow-odbc-py/issues/115
    """
    # Given
    table = "InsertBatchesMultipleSmallBatches"
    empty_table(table, "VARCHAR(10)")
    schema = pa.schema([("a", pa.utf8())])

    def iter_record_batches():
        yield pa.RecordBatch.from_arrays([pa.array(["a"])], schema=schema)
        yield pa.RecordBatch.from_arrays([pa.array(["bc"])], schema=schema)

    reader = pa.RecordBatchReader.from_batches(schema, iter_record_batches())

    # When
    insert_into_table(connection_string=MSSQL, chunk_size=20, table=table, reader=reader)

    # Then
    actual = check_output(
        ["odbcsv", "fetch", "-c", MSSQL, "-q", f"SELECT a FROM {table} ORDER BY id"]
    )
    assert "a\na\nbc\n" == actual.decode("utf8")


def test_insert_from_parquet():
    """
    Insert data into database from a parquet file
    """
    # Given
    table = "InsertFromParquet"
    connection = pyodbc.connect(MSSQL)
    connection.execute(f"DROP TABLE IF EXISTS {table};")
    connection.execute(
        f"CREATE TABLE {table} (sepal_length REAL, sepal_width REAL, petal_length REAL, petal_width REAL, variety VARCHAR(20) );"
    )
    connection.commit()
    connection.close()

    # When
    arrow_table = pq.read_table("./tests/iris.parquet")
    from_table_to_db(source=arrow_table, target=table, connection_string=MSSQL)

    # Then
    after_roundtrip = read_arrow_batches_from_odbc(
        query=f"SELECT * FROM {table}", batch_size=1000, connection_string=MSSQL
    )
    assert after_roundtrip.schema == arrow_table.schema
    assert len(next(after_roundtrip)) == 150


def test_insert_large_string():
    """
    Insert an arrow table whose schema contains a "large string". Intention of this test is
    to verify that arrow schemas large utf-8 strings. Not necessarily that actually large strings
    are working (although they do).
    """
    # Given
    table = "InsertLargeString"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (a NVARCHAR(max))"')
    schema = pa.schema([("a", pa.large_string())])
    large_string = "H" * 2000

    def iter_record_batches():
        # The string value is not actually large, just the schema information allows it to be
        yield pa.RecordBatch.from_arrays([pa.array([large_string])], schema=schema)

    reader = pa.RecordBatchReader.from_batches(schema, iter_record_batches())

    # When
    insert_into_table(connection_string=MSSQL, chunk_size=20, table=table, reader=reader)

    # Then
    actual = check_output(
        [
            "odbcsv",
            "fetch",
            "-c",
            MSSQL,
            "--max-str-len",
            "2000",
            "-q",
            f"SELECT a FROM {table}",
        ]
    )
    assert f"a\n{large_string}\n" == actual.decode("utf8")


def test_reinitalizing_logger_should_raise():
    """
    Reinitializin logger should raise
    """
    # When / Then
    with raises(
        Error,
        match=r"attempted to set a logger after the logging system was already initialized",
    ):
        log_to_stderr()


@pytest.mark.xfail(reason="Bug in MS driver cutting column name with umlaut one letter short.")
def test_umlaut_in_column_name():
    """
    Query a row with an umlaut in it. The column name should be unchanged in the arrow schema
    """
    query = "SELECT 42 AS hällo"
    reader = read_arrow_batches_from_odbc(query=query, batch_size=100, connection_string=MSSQL)
    it = iter(reader)
    actual = next(it)

    expected = pa.schema([("hällo", pa.int32(), False)])
    assert expected == actual.schema

    with raises(StopIteration):
        next(it)


def test_odbc_to_duckdb():
    """
    We want to see how arrow odbc links into the Arrow Record Batch Reader interface. To do so we
    look at integrating it with DuckDB which utilizes that interface and knowns nothing about
    arrow-odbc.
    """
    # Given an arrow record batch reader
    arrow_reader = read_arrow_batches_from_odbc(query="SELECT 42 as a", connection_string=MSSQL)

    # When we transform the arrow record batch reader into a pyarrow record batch reader
    pyarrow_reader = arrow_reader.into_pyarrow_record_batch_reader()

    # Then we can consume the pyarrow record batch reader with duckdb and expect the resulting
    # table to mirror the contents of the original query.
    with duckdb.connect(":memory:") as db:
        db.from_arrow(pyarrow_reader).create("my_table")
        table = db.sql("SELECT * FROM my_table").to_arrow_table()
    expected = {"a": [42]}
    assert expected == table.to_pydict()


def test_into_pyarrow_record_batch_reader_transfers_ownership():
    """
    In order to avoid the created instance of ```PyArrow RecordBatchReader``` to be suprisingly
    influenced by calls to the original Record batch reader we want to fully transfer ownership to
    the new type. Since there are no destructive move semantics in Python we express this as the
    original instance being in an empty state.
    """
    # Given an arrow record batch reader
    arrow_reader = read_arrow_batches_from_odbc(query="SELECT 42 as a", connection_string=MSSQL)

    # When we transform the arrow record batch reader into a pyarrow record batch reader
    _ = arrow_reader.into_pyarrow_record_batch_reader()

    # Then the original record batch reader is empty. I.e. it behaves like a consumed arrow_reader
    with raises(StopIteration):
        next(iter(arrow_reader))


def test_chunked_arrays_of_variable_length_strings():
    """
    See issue: <https://github.com/pacman82/arrow-odbc-py/issues/115>
    """
    # Given
    table = "ChunkedArraysOfVariableLengthStrings"
    empty_table(table, "VARCHAR(3)")

    # When
    arrow_table = pa.table({"a": pa.chunked_array([["a"], ["bc"]])})
    # It would work if we would combine the chunks
    from_table_to_db(arrow_table, target=table, connection_string=MSSQL)

    # Then
    actual = check_output(
        ["odbcsv", "fetch", "-c", MSSQL, "-q", f"SELECT a FROM {table} ORDER BY id"]
    )
    assert "a\na\nbc\n" == actual.decode("utf8")


def test_query_timeout():
    """
    Send a query which takes three seconds and set a timeout of one second. Verify we get a timeout
    error.
    """
    # Given a long running query
    long_running_query = "WAITFOR DELAY '0:0:03'; SELECT 42 as a"

    # When setting a query timeout of 1 second and fetching data, then we expect a timeout
    with raises(Error, match="Query timeout expired"):
        _arrow_reader = read_arrow_batches_from_odbc(
            query=long_running_query, connection_string=MSSQL, query_timeout_sec=1
        )


@pytest.mark.slow
def test_should_not_leak_memory_for_each_batch():
    """
    Read a bunch of arrow batches and see if total memory usage went over a
    threshold after running GC. Currently I let this run manually and see if
    the process takes more memory over time as an assertion.
    """
    # Given
    table = "ShouldNotLeakMemoryForEachBatch"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(
        f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (sepal_length REAL, sepal_width REAL, petal_length REAL, petal_width REAL, variety VARCHAR(20) )"'
    )
    os.system(f'odbcsv insert -c "{MSSQL}" -i ./tests/iris.csv {table}')

    for _ in range(1):
        # When, create an individual batch for each row
        reader = read_arrow_batches_from_odbc(
            query=f"SELECT * FROM {table}", batch_size=1, connection_string=MSSQL
        )

        for batch in reader:
            del batch
