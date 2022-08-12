import gc
import os

import pyarrow as pa
import pyarrow.csv as csv

import pytest

from subprocess import run, check_output

from pytest import raises

from arrow_odbc import read_arrow_batches_from_odbc, Error
from arrow_odbc.writer import insert_into_table

MSSQL = "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;"


def test_should_report_error_on_invalid_connection_string():
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
        read_arrow_batches_from_odbc(
            query=query, batch_size=100, connection_string=MSSQL
        )


def test_insert_statement():
    """
    BatchReader should be `None` if statement does not produce a result set.
    """
    table = "EmptyResult"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (a int);"')

    # This statement does not produce a result set
    query = f"INSERT INTO {table} (a) VALUES (42);"

    assert (
        read_arrow_batches_from_odbc(
            query=query, batch_size=100, connection_string=MSSQL
        )
        is None
    )


def test_empty_table():
    """
    Should return an empty iterator querying an empty table.
    """
    table = "Empty"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (a int);"')

    query = f"SELECT * FROM {table}"

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL
    )

    with raises(StopIteration):
        next(iter(reader))


def test_one_row():
    """
    Query a table with one row. Should return one batch
    """
    table = "OneRow"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (a int);"')
    rows = "a\n42"
    run(["odbcsv", "insert", "-c", MSSQL, table], input=rows, encoding="ascii")

    query = f"SELECT * FROM {table}"

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL
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
    Query a table with one row. Should return one batch
    """
    table = "TestSchema"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(
        f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (a INT, b VARCHAR(50));"'
    )

    query = f"SELECT * FROM {table}"
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL
    )

    expected = pa.schema([("a", pa.int32()), ("b", pa.string())])
    assert expected == reader.schema


def test_timestamp_us():
    """
    Query a table with one row. Should return one batch
    """
    table = "OneRow"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (a DATETIME2(6));"')
    rows = "a\n2014-04-14 21:25:42.074841"
    run(["odbcsv", "insert", "-c", MSSQL, table], input=rows, encoding="ascii")

    query = f"SELECT * FROM {table}"

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL
    )
    it = iter(reader)

    actual = next(it)

    schema = pa.schema([("a", pa.timestamp("us"))])
    expected = pa.RecordBatch.from_pydict({"a": [1397510742074841]}, schema)
    print(expected[0])
    print(actual[0])
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_specify_user_and_password_separatly():
    """
    Query a table with one row. Should return one batch
    """

    query = f"SELECT 42 as a;"

    # Connection string without credentials
    connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=localhost;"
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

    actual = next(it)

    schema = pa.schema([("a", pa.int32())])
    expected = pa.RecordBatch.from_pydict({"a": [42]}, schema)
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_query_char():
    """
    Query a string those UTF-16 representation is larger than the maximum binary column length on
    the database.
    """
    # 'ab' is char(2) => 2 bytes on database. Yet, only one UTF-16 character can fit into 2 bytes.
    query = "SELECT 'ab' as a"
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL
    )

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
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL
    )

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
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL
    )

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
    with raises(Error, match="ODBC reported a size of '0' for the column"):
        read_arrow_batches_from_odbc(
            query=query, batch_size=100, connection_string=MSSQL
        )


def test_query_with_string_parameter():
    """
    Use a string parameter in a where clause and verify that the result is
    filtered accordingly
    """
    table = "QueryWithStringParameter"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(
        f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (column_a CHAR(1), column_b INTEGER);"'
    )
    rows = "column_a,column_b\nA,1\nB,2\nC,3\nD,4\n"
    run(["odbcsv", "insert", "-c", MSSQL, table], input=rows, encoding="ascii")

    query = f"SELECT column_b FROM {table} WHERE column_a=?;"

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=10, connection_string=MSSQL, parameters=["B"]
    )
    it = iter(reader)

    actual = next(it)

    schema = pa.schema([("column_b", pa.int32())])
    expected = pa.RecordBatch.from_pydict({"column_b": [2]}, schema)
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_query_with_none_parameter():
    """
    Use a string parameter in a where clause and verify that the result is
    filtered accordingly
    """
    table = "QueryWithNoneParameter"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(
        f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (column_a CHAR(1), column_b INTEGER);"'
    )
    rows = "column_a,column_b\nA,1\nB,2\nC,3\nD,4\n"
    run(["odbcsv", "insert", "-c", MSSQL, table], input=rows, encoding="ascii")

    query = f"SELECT column_b FROM {table} WHERE column_a=?;"

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=10, connection_string=MSSQL, parameters=[None]
    )
    it = iter(reader)

    with raises(StopIteration):
        next(it)


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

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL
    )

    for batch in reader:
        df = batch.to_pydict()


def test_allocation_erros():
    """
    Avoids unrecoverable allocation errors, if querying an image column
    """
    table = "AllocationError"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (my_image Image)"')

    query = f"SELECT * FROM {table}"

    with raises(Error, match="Column buffer is too large to be allocated."):
        read_arrow_batches_from_odbc(
            query=query,
            batch_size=1000,
            connection_string=MSSQL,
            falliable_allocations=True,
        )


def test_image():
    """
    Avoids error allocating image column by using casts.
    """
    table = "Image"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (my_image Image)"')

    query = f"SELECT CAST(my_image as VARBINARY(2048)) FROM {table}"

    reader = read_arrow_batches_from_odbc(
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
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (a VARCHAR(max))"')
    rows = "a\nHello World!\n"
    run(["odbcsv", "insert", "-c", MSSQL, table], input=rows, encoding="ascii")

    query = f"SELECT (a) FROM {table}"

    # When
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=1000, connection_string=MSSQL, max_text_size=1024
    )
    it = iter(reader)
    batch = next(it)

    # Then
    actual = batch.to_pydict()
    expected = {"a": ["Hello World!"]}

    assert expected == actual


def test_support_varbinary_max():
    """
    Support fetching values from a VARBINARY(max) column, by specifying an upper
    bound for the values in it.
    """
    # Given
    table = "SupportVarcharMax"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (a VARBINARY(max))"')

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


def test_insert_should_raise_on_invalid_connection_string():
    """
    Insert should raise on invalid connection string
    """
    # Given
    invalid_connection_string = "FOO"
    schema = pa.schema([("a", pa.int64())])

    def iter_record_batches():
        for i in range(2):
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


def test_insert_batches():
    """
    Insert data into database
    """
    # Given
    table = "InsertBatches"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(
        f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (id int IDENTITY(1,1), a BIGINT)"'
    )
    schema = pa.schema([("a", pa.int64())])

    def iter_record_batches():
        for i in range(2):
            yield pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=schema)

    reader = pa.RecordBatchReader.from_batches(schema, iter_record_batches())

    # When
    insert_into_table(
        connection_string=MSSQL, chunk_size=20, table=table, reader=reader
    )

    # Then
    actual = check_output(
        ["odbcsv", "fetch", "-c", MSSQL, "-q", f"SELECT a FROM {table} ORDER BY id"]
    )
    assert "a\n1\n2\n3\n1\n2\n3\n" == actual.decode("utf8")


@pytest.mark.slow
def test_should_not_leak_memory_for_each_batch():
    """
    Read a bunch of arrow batches and see if total memory usage went over a
    threshold after running GC.
    """
    # Given
    table = "ShouldNotLeakMemoryForEachBatch"
    os.system(f'odbcsv fetch -c "{MSSQL}" -q "DROP TABLE IF EXISTS {table};"')
    os.system(
        f'odbcsv fetch -c "{MSSQL}" -q "CREATE TABLE {table} (sepal_length REAL, sepal_width REAL, petal_length REAL, petal_width REAL, variety VARCHAR(20) )"'
    )
    os.system(f'odbcsv insert -c "{MSSQL}" -i ./tests/iris.csv {table}')

    # When, create an individual batch for each row
    reader = read_arrow_batches_from_odbc(
        query=f"SELECT * FROM {table}", batch_size=1, connection_string=MSSQL
    )

    for batch in reader:
        del batch
