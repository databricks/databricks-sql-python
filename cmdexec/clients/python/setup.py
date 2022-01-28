import setuptools

setuptools.setup(
    name="databricks-sql-connector",
    version="2.0.0rc2",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=["pyarrow", 'thrift>=0.13.0', "pandas"],
    author="Databricks",
)
