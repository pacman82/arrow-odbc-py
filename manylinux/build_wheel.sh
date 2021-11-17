#!/bin/bash -xe

# Compile wheels
#for PYBIN in /opt/python/*/bin
for PYBIN in /opt/python/cp37-cp37m/bin
do
    "${PYBIN}/pip" install --upgrade pip
    "${PYBIN}/pip" wheel /io/ -w wheelhouse/
done

# Bundle external shared libraries into the wheels
for whl in /wheelhouse/arrow_odbc-*.whl
do
    auditwheel repair "${whl}" -w /io/dist/
done
