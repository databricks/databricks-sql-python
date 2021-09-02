import setuptools

setuptools.setup(
    name="databricks-sql-connector",
    version="0.0.0",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=[
        "grpcio",  # TODO: Minimum versions
        "pyarrow",
        "protobuf",
        "cryptography",
    ],
    author="Databricks",
)
