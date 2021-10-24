from typing import Type, TypeVar

from ._arrow_odbc_c import lib, ffi

T = TypeVar('T')

class Connection:
    '''
    An ODBC connection. Can be used to create Arrow Odbc readers.
    '''

    def __init__(self):
        pass

    @classmethod
    def from_connection_string(cls: Type[T], connection_string: str) -> T:
        self = cls()
        lib.arrow_odbc_connect_with_connection_string()
        return self
