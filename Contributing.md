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


With docker installed run:

```shell
docker-compose up
```

This starts the Microsoft SQL Server used for testing.

```shell
pip install -e .[test]
```

To install this package with the requirements for testing.

We now can execute the tests in typical fashion using:

```shell
pytest
```
