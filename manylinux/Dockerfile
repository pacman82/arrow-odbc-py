FROM quay.io/pypa/manylinux2010_x86_64

RUN yum search unixodbc
RUN yum install unixODBC.x86_64 -y

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:$PATH"