from setuptools import setup

# from setuptools_rust import Binding, RustExtension

extras = {}
extras["test"] = ["pytest"]
# extras["docs"] = ["sphinx", "sphinx_rtd_theme", "setuptools_rust"]


def build_native(spec):
    # build rust library
    build = spec.add_external_build(
        cmd=["cargo", "build", "--release"], path="./arrow_odbc_c"
    )

    spec.add_cffi_module(
        module_path="arrow_odbc._arrow_odbc_c",
        dylib=lambda: build.find_dylib("arrow_odbc_c", in_path="target/release"),
        header_filename=lambda: build.find_header("arrow_odbc.h", in_path="."),
        rtld_flags=["NOW", "NODELETE"],
    )

def readme():
    with open('README.md') as f:
        return f.read()

setup(
    name="arrow-odbc",
    version="0.1.0",
    description="Read the data of an ODBC data source as sequence of Apache Arrow record batches.",
    long_description=readme(),
    long_description_content_type="text/markdown",
    keywords="arrow odbc",
    author="Markus Klein",
    url="https://github.com/pacman82/arrow-odbc-py",
    license="MIT",
    # rust_extensions=[
    #     RustExtension(
    #         "arrow_odbc.arrow_odbc_c",
    #         path="./Cargo.toml",
    #         rust_version="1.56.0",
    #         binding=Binding.NoBinding,
    #         debug=False,
    #     )
    # ],
    extras_require=extras,
    install_requires=["milksnake", "pyarrow"],
    milksnake_tasks=[build_native],
    zip_safe=False,
    include_package_data = True
)
