from os import system

import cffi
from cffi import recompiler

HEADER = "native.h"
FFI_PY = "ffi.py"

if __name__ == "__main__":
    # Generate header file using bindgen
    system("cbindgen -o native.h")

    ffi = cffi.FFI()

    # Read header file generated with cbindgen and remove include directives
    with open(f"{HEADER}") as header:
        declarations = [
            line for line in header.read().splitlines() if not line.startswith("#")
        ]
    source = "\n".join(declarations)
    # Strip include directives
    ffi.cdef(source)
    recompiler.make_py_source(ffi, "ffi", f"arrow_odbc/_native/{FFI_PY}")
