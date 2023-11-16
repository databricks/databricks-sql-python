"""
databricks-sql-connector includes a SQLAlchemy 2.0 dialect compatible with Databricks SQL. To install
its dependencies you can run `pip install databricks-sql-connector[sqlalchemy]`.

The expected connection string format which you can pass to create_engine() is:

databricks://token:dapi***@***.cloud.databricks.com?http_path=/sql/***&catalog=**&schema=**

Our dialect implements the majority of SQLAlchemy 2.0's API. Because of the extent of SQLAlchemy's
capabilities it isn't feasible to provide examples of every usage in a single script, so we only
provide a basic one here. Learn more about usage in README.sqlalchemy.md in this repo. 
"""

# fmt: off

import os
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import UUID

# By convention, backend-specific SQLA types are defined in uppercase
# This dialect exposes Databricks SQL's TIMESTAMP and TINYINT types
# as these are not covered by the generic, camelcase types shown below
from databricks.sqlalchemy import TIMESTAMP, TINYINT

# Beside the CamelCase types shown below, line comments reflect
# the underlying Databricks SQL / Delta table type
from sqlalchemy import (
    BigInteger,      # BIGINT
    Boolean,         # BOOLEAN
    Column,
    Date,            # DATE
    DateTime,        # TIMESTAMP_NTZ
    Integer,         # INTEGER
    Numeric,         # DECIMAL
    String,          # STRING
    Time,            # STRING
    Uuid,            # STRING
    create_engine,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Session

host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")
catalog = os.getenv("DATABRICKS_CATALOG")
schema = os.getenv("DATABRICKS_SCHEMA")


# Extra arguments are passed untouched to databricks-sql-connector
# See src/databricks/sql/thrift_backend.py for complete list
extra_connect_args = {
    "_tls_verify_hostname": True,
    "_user_agent_entry": "PySQL Example Script",
}


engine = create_engine(
    f"databricks://token:{access_token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}",
    connect_args=extra_connect_args, echo=True,
)


class Base(DeclarativeBase):
    pass


# This object gives a usage example for each supported type
# for more details on these, see README.sqlalchemy.md
class SampleObject(Base):
    __tablename__ = "pysql_sqlalchemy_example_table"

    bigint_col = Column(BigInteger, primary_key=True)
    string_col = Column(String)
    tinyint_col = Column(TINYINT)
    int_col = Column(Integer)
    numeric_col = Column(Numeric(10, 2))
    boolean_col = Column(Boolean)
    date_col = Column(Date)
    datetime_col = Column(TIMESTAMP)
    datetime_col_ntz = Column(DateTime)
    time_col = Column(Time)
    uuid_col = Column(Uuid)

# This generates a CREATE TABLE statement against the catalog and schema
# specified in the connection string
Base.metadata.create_all(engine)

# Output SQL is:
# CREATE TABLE pysql_sqlalchemy_example_table (
#         bigint_col BIGINT NOT NULL, 
#         string_col STRING, 
#         tinyint_col SMALLINT, 
#         int_col INT, 
#         numeric_col DECIMAL(10, 2), 
#         boolean_col BOOLEAN, 
#         date_col DATE, 
#         datetime_col TIMESTAMP, 
#         datetime_col_ntz TIMESTAMP_NTZ, 
#         time_col STRING, 
#         uuid_col STRING, 
#         PRIMARY KEY (bigint_col)
# ) USING DELTA

# The code that follows will INSERT a record using SQLAlchemy ORM containing these values
# and then SELECT it back out. The output is compared to the input to demonstrate that
# all type information is preserved.
sample_object = {
    "bigint_col": 1234567890123456789,
    "string_col": "foo",
    "tinyint_col": -100,
    "int_col": 5280,
    "numeric_col": Decimal("525600.01"),
    "boolean_col": True,
    "date_col": date(2020, 12, 25),
    "datetime_col": datetime(
        1991, 8, 3, 21, 30, 5, tzinfo=timezone(timedelta(hours=-8))
    ),
    "datetime_col_ntz": datetime(1990, 12, 4, 6, 33, 41),
    "time_col": time(23, 59, 59),
    "uuid_col": UUID(int=255),
}
sa_obj = SampleObject(**sample_object)

session = Session(engine)
session.add(sa_obj)
session.commit()

# Output SQL is:
# INSERT INTO
#   pysql_sqlalchemy_example_table (
#     bigint_col,
#     string_col,
#     tinyint_col,
#     int_col,
#     numeric_col,
#     boolean_col,
#     date_col,
#     datetime_col,
#     datetime_col_ntz,
#     time_col,
#     uuid_col
#   )
# VALUES
#   (
#     :bigint_col,
#     :string_col,
#     :tinyint_col,
#     :int_col,
#     :numeric_col,
#     :boolean_col,
#     :date_col,
#     :datetime_col,
#     :datetime_col_ntz,
#     :time_col,
#     :uuid_col
#   )

# Here we build a SELECT query using ORM
stmt = select(SampleObject).where(SampleObject.int_col == 5280)

# Then fetch one result with session.scalar()
result = session.scalar(stmt)

# Finally, we read out the input data and compare it to the output
compare = {key: getattr(result, key) for key in sample_object.keys()}
assert compare == sample_object

# Then we drop the demonstration table
Base.metadata.drop_all(engine)

# Output SQL is:
# DROP TABLE pysql_sqlalchemy_example_table
