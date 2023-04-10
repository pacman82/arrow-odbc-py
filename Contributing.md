# Contributions

Whether they be in code, interesting feature suggestions, design critique or bug reports, all contributions are welcome. Please start an issue, before investing a lot of work. This helps avoid situations there I would feel the need to reject a large body of work, and a lot of your time has been wasted. `odbc-arrow-py` is a pet project and a work of love, which implies that I maintain it in my spare time. Please understand that I may not always react immediately. If you contribute code to fix a Bug, please also contribute the test to fix it. Happy contributing.

## Local build and test setup

Running local tests currently requires:

* Docker and Docker compose.
* An ODBC driver manager
* A driver for Microsoft SQL Server
* Rust toolchain (cargo)
* Python

You can install these requirements from here:

* Docker: <https://www.docker.com/get-started>
* Install Rust compiler and Cargo. Follow the instructions on [this site](https://www.rust-lang.org/en-US/install.html).
* [Microsoft ODBC Driver 17 for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15).
* An ODBC Driver manager if you are not on windows: <http://www.unixodbc.org/>
* There are many ways to setup Python on a system here is one: <https://www.python.org/downloads/>

With docker installed we start the Microsoft SQL Server used for testing:

```shell
docker-compose up
```

Tests rely on `odbcsv` to fill the test db with data:

```shell
cargo install odbcsv
```

Inside a virtual environment install the requirements for developing/testing.

```shell
pip install -e .[test]
```

We now can execute the tests using:

```shell
pytest
```

## Build wheels

```shell
python -m pip install build
python -m build
```

## Generate documentation

### Posix (with make installed)

```shell
cd docs
make html
```

### Windows (without make)

```shell
sphinx-build -M html ./doc/source ./doc/build
```