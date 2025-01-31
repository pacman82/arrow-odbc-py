#!/bin/bash -xe

# Compile wheels
PYBIN="/opt/python/cp310-cp310/bin"

"${PYBIN}/pip" install --upgrade pip
"${PYBIN}/pip" wheel /io/ -w wheelhouse/


mkdir /io/dist

for f in /wheelhouse/arrow_odbc-*.whl
do
    # Do not bundle unixODBC with the manylinux wheel
    #
    # * Won't work well, because ODBC lib has to dynamically load drivers from the
    #  system anyway.
    # * Unclear (to me) if we could redistribute unixODBC that way, since it is
    #   GPL.
    #
    # Other than that auditwheel will change the tag fro linux_x86_64 to manylinux_2_17_x86_64 and
    # manylinux2016_x86_64
    auditwheel repair --exclude libodbc.so.* "${f}" -w /io/dist/
done
