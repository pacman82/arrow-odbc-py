import os

import pyarrow as pa

from subprocess import run

from pytest import raises

from arrow_odbc import read_arrow_batches_from_odbc, Error

MSSQL = "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;"


def test_should_report_error_on_invalid_connection_string():
    """
    We want to forward the original ODBC errors to the end user. Of course foo
    is not a valid connection string. Therefore we want to see the creation of
    this connection fail, but with a nice error.
    """
    with raises(
        Error, match="Data source name not found and no default driver specified"
    ):
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
    os.system(f'odbcsv query -c "{MSSQL}" "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv query -c "{MSSQL}" "CREATE TABLE {table} (a int);"')

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
    os.system(f'odbcsv query -c "{MSSQL}" "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv query -c "{MSSQL}" "CREATE TABLE {table} (a int);"')

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
    os.system(f'odbcsv query -c "{MSSQL}" "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv query -c "{MSSQL}" "CREATE TABLE {table} (a int);"')
    rows = "a\n42"
    run(["odbcsv", "insert", "-c", MSSQL, table], input=rows, encoding="ascii")

    query = f"SELECT * FROM {table}"

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL
    )
    it = iter(reader)

    actual = next(it)

    expected = pa.StructArray.from_arrays([pa.array([42], pa.int32())], names=["a"])
    assert expected == actual

    with raises(StopIteration):
        next(it)


def test_schema():
    """
    Query a table with one row. Should return one batch
    """
    table = "TestSchema"
    os.system(f'odbcsv query -c "{MSSQL}" "DROP TABLE IF EXISTS {table};"')
    os.system(
        f'odbcsv query -c "{MSSQL}" "CREATE TABLE {table} (a INT, b VARCHAR(50));"'
    )

    query = f"SELECT * FROM {table}"
    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL
    )

    actual = reader.schema()

    # Todo: understand difference:
    # expected = pa.schema([('a', pa.int32()), ('b', pa.string())])
    # assert expected == actual


def test_usage():
    """
    Validate usage works like in the readme
    """
    table = "Usage"
    os.system(f'odbcsv query -c "{MSSQL}" "DROP TABLE IF EXISTS {table};"')
    os.system(f'odbcsv query -c "{MSSQL}" "CREATE TABLE {table} (a int);"')

    query = f"SELECT * FROM {table}"

    reader = read_arrow_batches_from_odbc(
        query=query, batch_size=100, connection_string=MSSQL
    )

    for batch in reader:

        pass
