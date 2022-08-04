# sample-app-insert.py
#
# Program to demonstrate the simplest INSERT statement
#

import os
import random

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import select, insert, Table, Column
from sqlalchemy import SMALLINT, Integer, BigInteger, Float, DECIMAL, BOOLEAN

from sqlalchemy.dialects.mysql.types import TINYINT, DOUBLE # borrow MySQL's impls
from sqlalchemy import String, DATE, TIMESTAMP

# pickup settings from the env
server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path       = os.getenv("DATABRICKS_HTTP_PATH")
access_token    = os.getenv("DATABRICKS_TOKEN")
default_schema  = os.getenv("DATABRICKS_SCHEMA")

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

with engine.connect() as conn:
    stmt = insert(t1).values(f_byte=42, 
                                f_short=31415, 
                                f_int=random.randint(1,1001002003), 
                                f_long=4001002003004005006,
                                f_float=1.41,
                                f_double=1.6666,
                                f_decimal=2.71828,
                                f_boolean=False)

    print(f"Attempting to execute: {stmt}\n")

    print(f"Rows from table {numtypes}")
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

    # stmt = insert(t2).values(f_string='Antarctic expedition', f_date='1911-12-14', f_timestamp='1911-12-14T15:00', f_interval='4 0:00:00' )
    stmt = insert(t2).values(f_string='Antarctic expedition', f_date='1911-12-14', f_timestamp='1911-12-14T15:00')
    print(f"Attempting to execute: {stmt}\n")

    print(f"Rows from table {strtypes}")
    for row in conn.execute(stmt):
        print(row)
