"""
databricks-sql-connector includes a SQLAlchemy dialect compatible with Databricks SQL.
It aims to be a drop-in replacement for the crflynn/sqlalchemy-databricks project, that implements
more of the Databricks API, particularly around table reflection, Alembic usage, and data
ingestion with pandas.

Expected URI format is: databricks+thrift://token:dapi***@***.cloud.databricks.com?http_path=/sql/***

Because of the extent of SQLAlchemy's capabilities it isn't feasible to provide examples of every
usage in a single script, so we only provide a basic one here. More examples are found in our test
suite at tests/e2e/sqlalchemy/test_basic.py and in the PR that implements this change: 

https://github.com/databricks/databricks-sql-python/pull/57

# What's already supported

Most of the functionality is demonstrated in the e2e tests mentioned above. The below list we
derived from those test method names:

    - Create and drop tables with SQLAlchemy Core
    - Create and drop tables with SQLAlchemy ORM
    - Read created tables via reflection
    - Modify column nullability
    - Insert records manually
    - Insert records with pandas.to_sql (note that this does not work for DataFrames with indexes)

This connector also aims to support Alembic for programmatic delta table schema maintenance. This
behaviour is not yet backed by integration tests, which will follow in a subsequent PR as we learn
more about customer use cases there. That said, the following behaviours have been tested manually:

    - Autogenerate revisions with alembic revision --autogenerate
    - Upgrade and downgrade between revisions with `alembic upgrade <revision hash>` and
      `alembic downgrade <revision hash>`

# Known Gaps
    - MAP, ARRAY, and STRUCT types: this dialect can read these types out as strings. But you cannot
      define a SQLAlchemy model with databricks.sqlalchemy.dialect.types.DatabricksMap (e.g.) because
      we haven't implemented them yet.
    - Constraints: with the addition of information_schema to Unity Catalog, Databricks SQL supports
      foreign key and primary key constraints. This dialect can write these constraints but the ability
      for alembic to reflect and modify them programmatically has not been tested.
"""

import os
import sqlalchemy
from sqlalchemy.orm import Session
from sqlalchemy import Column, String, Integer, BOOLEAN, create_engine, select

try:
    from sqlalchemy.orm import declarative_base
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base

host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")
catalog = os.getenv("DATABRICKS_CATALOG")
schema = os.getenv("DATABRICKS_SCHEMA")


# Extra arguments are passed untouched to the driver
# See thrift_backend.py for complete list
extra_connect_args = {
    "_tls_verify_hostname": True,
    "_user_agent_entry": "PySQL Example Script",
}

if sqlalchemy.__version__.startswith("1.3"):
    # SQLAlchemy 1.3.x fails to parse the http_path, catalog, and schema from our connection string
    # Pass these in as connect_args instead

    conn_string = f"databricks://token:{access_token}@{host}"
    connect_args = dict(catalog=catalog, schema=schema, http_path=http_path)
    all_connect_args = {**extra_connect_args, **connect_args}
    engine = create_engine(conn_string, connect_args=all_connect_args)
else:
    engine = create_engine(
        f"databricks://token:{access_token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}",
        connect_args=extra_connect_args,
    )

session = Session(bind=engine)
base = declarative_base(bind=engine)


class SampleObject(base):

    __tablename__ = "mySampleTable"

    name = Column(String(255), primary_key=True)
    episodes = Column(Integer)
    some_bool = Column(BOOLEAN)


base.metadata.create_all()

sample_object_1 = SampleObject(name="Bim Adewunmi", episodes=6, some_bool=True)
sample_object_2 = SampleObject(name="Miki Meek", episodes=12, some_bool=False)

session.add(sample_object_1)
session.add(sample_object_2)

session.commit()

# SQLAlchemy 1.3 has slightly different methods
if sqlalchemy.__version__.startswith("1.3"):
    stmt = select([SampleObject]).where(SampleObject.name.in_(["Bim Adewunmi", "Miki Meek"]))
    output = [i for i in session.execute(stmt)]
else:
    stmt = select(SampleObject).where(SampleObject.name.in_(["Bim Adewunmi", "Miki Meek"]))
    output = [i for i in session.scalars(stmt)]

assert len(output) == 2

base.metadata.drop_all()
