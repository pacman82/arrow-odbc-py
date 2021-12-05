#!/bin/bash -xe

# Compile wheels
PYBIN="/opt/python/cp310-cp310/bin"

"${PYBIN}/pip" install --upgrade pip
"${PYBIN}/pip" wheel /io/ -w wheelhouse/

# Bundle external shared libraries into the wheels
for whl in /wheelhouse/arrow_odbc-*.whl
do
    auditwheel repair "${whl}" -w /io/dist/
done
