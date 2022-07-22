# sample-app-select.py
#
# Program to demonstrate the simplest SELECT statement
#

import os

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import select, Table, Column
from sqlalchemy import SMALLINT, Integer, BigInteger, Float, DECIMAL, BOOLEAN
from sqlalchemy.dialects.mysql.types import TINYINT, DOUBLE # borrow MySQL's impls
from sqlalchemy import String, DATE, TIMESTAMP

# pickup settings from the env
server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path       = os.getenv("DATABRICKS_HTTP_PATH")
access_token    = os.getenv("DATABRICKS_TOKEN")
default_schema  = os.getenv("DATABRICKS_SCHEMA")

# provide a way to break in
debugbreakpoint = os.getenv("DATABRICKS_DIALECT_DEBUG") or False

# use echo=True for verbose log
engine = create_engine(f"databricks+thrift://token:{access_token}@{server_hostname}/{default_schema}?http_path={http_path}", echo=False, future=True)

metadata_obj = MetaData()

# NB: sample_numtypes is a pre-created/populated table 
numtypes = "sample_numtypes"

t1 = Table(
    numtypes,
    metadata_obj,
    Column('f_byte', TINYINT),
    Column('f_short', SMALLINT),
    Column('f_int', Integer),
    Column('f_long', BigInteger),
    Column('f_float', Float),
    Column('f_double', DOUBLE),
    Column('f_decimal', DECIMAL),
    Column('f_boolean', BOOLEAN)
)

# SELECT * FROM t WHERE f_byte = -125
stmt = select(t1).where(t1.c.f_byte == -125)
print(f"Attempting to execute: {stmt}\n")

print(f"Rows from table {numtypes}")

with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(row)


# NB: sample_strtypes is a pre-created/populated table 
strtypes = "sample_strtypes"

with engine.connect() as conn:
    t2 = Table(
        strtypes,
        metadata_obj,
        autoload_with=conn
    )

    # SELECT * FROM t
    stmt = select(t2)
    print(f"Attempting to execute: {stmt}\n")

    print(f"Rows from table {strtypes}")
    if debugbreakpoint:
        breakpoint()        
    for row in conn.execute(stmt):
        if debugbreakpoint:
            breakpoint()        
        print(row)
