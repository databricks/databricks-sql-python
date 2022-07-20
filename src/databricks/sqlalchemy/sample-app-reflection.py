# sample-app-reflection
#
# Program to demonstrate use of reflection instead of explicit declaration
#

import os

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, BigInteger, Float, Boolean
from sqlalchemy import select

# pickup settings from the env
server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path       = os.getenv("DATABRICKS_HTTP_PATH")
access_token    = os.getenv("DATABRICKS_TOKEN")
default_schema  = os.getenv("DATABRICKS_SCHEMA")

# use echo=True for verbose log
with create_engine(f"databricks+thrift://token:{access_token}@{server_hostname}/{default_schema}?http_path={http_path}", echo=False, future=True).connect() as conn:
    metadata_obj = MetaData()

    # NB: sample_numtypes is a pre-created/populated table 
    tableName = "sample_numtypes"

    # use reflection here to discover the schema dynamically
    numtypes = Table(
        "sample_numtypes", metadata_obj, autoload_with=conn
    )

    # SELECT * FROM t WHERE f_byte = -125
    stmt = select(numtypes).where(numtypes.c.f_byte == -125)
    print(f"Attempting to execute: {stmt}\n")

    print(f"Rows from table {tableName}")

    for row in conn.execute(stmt):
        print(row)
