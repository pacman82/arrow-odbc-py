# auto-generated file
__all__ = ['lib', 'ffi']

import os
from arrow_odbc._arrow_odbc_c__ffi import ffi

lib = ffi.dlopen(os.path.join(os.path.dirname(__file__), '_arrow_odbc_c__lib.cp310-win_amd64.pyd'), 0)
del os
