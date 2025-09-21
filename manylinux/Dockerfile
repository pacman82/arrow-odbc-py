FROM quay.io/pypa/manylinux_2_28_x86_64

RUN yum search unixodbc
RUN yum install unixODBC.x86_64 -y

# We want a current rust toolchain to build the rust part of the wheel
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:$PATH"
