# SELECT statement
import os

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, BigInteger, Float, Boolean
from sqlalchemy import select

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path       = os.getenv("DATABRICKS_HTTP_PATH")
access_token    = os.getenv("DATABRICKS_TOKEN")
default_schema  = os.getenv("DATABRICKS_SCHEMA")

engine = create_engine("databricks+thrift://token:{access_token}@{server_hostname}/{default_schema}?http_path={http_path}", echo=True, future=True)

metadata_obj = MetaData()
numtypes = Table(
    "sample_numtypes",
    metadata_obj,
    Column('f_byte', Integer),
    Column('f_short', Integer),
    Column('f_int', Integer),
    Column('f_long', BigInteger),
    Column('f_float', Float),
    Column('f_decimal', Float),
    Column('f_boolean', Boolean)
)

stmt = select(numtypes).where(numtypes.c.f_byte == -125)
print(stmt)
with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(row)
