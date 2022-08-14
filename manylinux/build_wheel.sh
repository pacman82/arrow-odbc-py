#!/bin/bash -xe

# Compile wheels
PYBIN="/opt/python/cp310-cp310/bin"

"${PYBIN}/pip" install --upgrade pip
"${PYBIN}/pip" wheel /io/ -w wheelhouse/

# Do not bundle unixODBC with the manylinux wheel
#
# * Won't work well, because ODBC lib has to dynamically load drivers from the
#  system anyway.
# * Unclear (to me) if we could redistribute unixODBC that way, since it is
#   GPL.
#
# So we won't call auditwheel repair

mkdir /io/dist

for f in /wheelhouse/arrow_odbc-*.whl
do
    # We won't do that, see above
    # auditwheel repair "${whl}" -w /io/dist/

    # Instead we unzip the wheel edit the dist info and rename it ourselfs, so we can upload it to PyPi

    # f looks like e.g. /wheelhouse/arrow_odbc-0.1.8-py3-none-linux_x86_64.whl
    # trunk e.g. /wheelhouse/arrow_odbc-0.1.8
    trunk=${f%-py3-none-linux_x86_64.whl}
    # E.g. 0.1.8
    ver=${trunk#*-}

    mv $f /wheelhouse/arrow_odbc-${ver}.zip
    unzip /wheelhouse/arrow_odbc-${ver}.zip -d /wheelhouse/arrow_odbc-${ver}-edit

    cp -f /io/manylinux/WHEEL "/wheelhouse/arrow_odbc-${ver}-edit/arrow_odbc-${ver}.dist-info/WHEEL"

    cd /wheelhouse/arrow_odbc-${ver}-edit/
    zip -rv /io/dist/arrow_odbc-${ver}-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl .
    cd -
done

