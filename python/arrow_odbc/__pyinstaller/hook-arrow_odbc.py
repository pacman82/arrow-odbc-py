# PyInstaller build-time hook. Only loaded by PyInstaller's analyzer when a
# user freezes an app that depends on arrow_odbc — never imported at runtime.
# Tells PyInstaller to bundle the compiled cdylib (arrow_odbc.dll / .so /
# .dylib) that lives inside the package, which static import analysis misses
# because it is loaded via cffi's `ffi.dlopen`, not `import`.
from PyInstaller.utils.hooks import collect_dynamic_libs

binaries = collect_dynamic_libs("arrow_odbc")
