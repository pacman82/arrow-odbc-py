FROM quay.io/pypa/manylinux2014_x86_64

# Install unix-odbc from source. Version installed via YUM is too old
RUN curl https://www.unixodbc.org/unixODBC-2.3.9.tar.gz > unixODBC-2.3.9.tar.gz
RUN tar -xzf unixODBC-2.3.9.tar.gz
RUN cd unixODBC-2.3.9 && ./configure && make && make install

# RUN yum search unixodbc
# RUN yum install unixODBC.x86_64 -y

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable

# Wheels are zip files. Needed to zip it again after editing it
RUN yum install zip -y

ENV PATH="/root/.cargo/bin:$PATH"