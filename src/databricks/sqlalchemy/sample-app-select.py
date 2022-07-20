# sample-app-select.py
#
# Program to demonstrate the simplest SELECT statement
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
engine = create_engine(f"databricks+thrift://token:{access_token}@{server_hostname}/{default_schema}?http_path={http_path}", echo=False, future=True)

metadata_obj = MetaData()

# NB: sample_numtypes is a pre-created/populated table 
tableName = "sample_numtypes"

numtypes = Table(
    tableName,
    metadata_obj,
    Column('f_byte', Integer),
    Column('f_short', Integer),
    Column('f_int', Integer),
    Column('f_long', BigInteger),
    Column('f_float', Float),
    Column('f_decimal', Float),
    Column('f_boolean', Boolean)
)

# SELECT * FROM t WHERE f_byte = -125
stmt = select(numtypes).where(numtypes.c.f_byte == -125)
print(f"Attempting to execute: {stmt}\n")

print(f"Rows from table {tableName}")

with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(row)
