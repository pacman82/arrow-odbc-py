# Docker image intended for local development using vscode/zed
FROM mcr.microsoft.com/devcontainers/rust:2-1-trixie

# Download the package to configure the Microsoft repo
RUN curl -sSL -O https://packages.microsoft.com/config/debian/13/packages-microsoft-prod.deb
# Install the package
RUN dpkg -i packages-microsoft-prod.deb
RUN rm packages-microsoft-prod.deb

# Install packages
RUN echo msodbcsql18 msodbcsql/ACCEPT_EULA boolean true | debconf-set-selections
RUN apt update && export DEBIAN_FRONTEND=noninteractive \
&& apt-get -y install --no-install-recommends \
    build-essential \
    unixodbc-dev \
    msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

USER vscode

# Install uv (Python package/runtime manager used by the project)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# odbcsv is used by the test suite to seed test tables
RUN cargo install odbcsv
