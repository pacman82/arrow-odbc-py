from setuptools import setup


def build_native(spec):
    # build rust library
    build = spec.add_external_build(cmd=["cargo", "build", "--release"], path=".")

    spec.add_cffi_module(
        module_path="arrow_odbc._native",
        dylib=lambda: build.find_dylib("native", in_path="/target/release"),
        header_filename=lambda: build.find_header("native.h", in_path="rust"),
        rtld_flags=["NOW", "NODELETE"],
    )


def readme():
    with open("README.md") as f:
        return f.read()


setup(
    name="arrow-odbc",
    packages=["arrow_odbc"],
    zip_safe=False,
    platforms="any",
    setup_requires=["milksnake"],
    install_requires=["pyarrow", "milksnake"],
    extras_require={
        "test": ["pytest"],
    },
    milksnake_tasks=[build_native],
    url="https://github.com/pacman82/arrow-odbc-py",
    author="Markus Klein",
    version="0.1.11",
    license="MIT",
    description="Read the data of an ODBC data source as sequence of Apache Arrow record batches.",
    long_description=readme(),
    long_description_content_type="text/markdown",
)
