from setuptools import setup
# from setuptools_rust import Binding, RustExtension

extras = {}
extras["test"] = ["pytest"]
# extras["docs"] = ["sphinx", "sphinx_rtd_theme", "setuptools_rust"]

def build_native(spec):
    # build rust library
    build = spec.add_external_build(
        cmd=['cargo', 'build', '--release'],
        path='arrow_odbc_c'
    )

    spec.add_cffi_module(
        module_path='arrow_odbc._arrow_odbc_c',
        dylib=lambda: build.find_dylib('arrow_odbc_c', in_path='target/release'),
        header_filename=lambda: build.find_header('arrow_odbc.h', in_path='.'),
        rtld_flags=['NOW', 'NODELETE']
    )

setup(
    name="arrow-odbc",
    version="0.1.0",
    description="Read the data of an ODBC data source as sequence of Apache Arrow record batches.",
    long_description=open("Readme.md", "r", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    keywords="arrow odbc",
    author="Markus Klein",
    url="https://github.com/pacman82/odbc-api",
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
    setup_requires=["milksnake"],
    install_requires=["milksnake"],
    milksnake_tasks=[build_native],
    zip_safe=False,
)
