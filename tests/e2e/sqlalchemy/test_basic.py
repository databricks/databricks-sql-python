import os, datetime, decimal
import pytest
from unittest import skipIf
from sqlalchemy import create_engine, select, insert, Column, MetaData, Table
from sqlalchemy.orm import Session
from sqlalchemy.types import SMALLINT, Integer, BOOLEAN, String, DECIMAL, Date

try:
    from sqlalchemy.orm import declarative_base
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base


USER_AGENT_TOKEN = "PySQL e2e Tests"

def sqlalchemy_1_3():
    import sqlalchemy
    return sqlalchemy.__version__.startswith("1.3")

def version_agnostic_select(object_to_select, *args, **kwargs):
    """
    SQLAlchemy==1.3.x requires arguments to select() to be a Python list
    
    https://docs.sqlalchemy.org/en/20/changelog/migration_14.html#orm-query-is-internally-unified-with-select-update-delete-2-0-style-execution-available
    """

    if sqlalchemy_1_3():
        return select([object_to_select], *args, **kwargs)
    else:
        return select(object_to_select, *args, **kwargs)
    


@pytest.fixture
def db_engine():

    HOST = os.environ.get("host")
    HTTP_PATH = os.environ.get("http_path")
    ACCESS_TOKEN = os.environ.get("access_token")
    CATALOG = os.environ.get("catalog")
    SCHEMA = os.environ.get("schema")

    connect_args = {"_user_agent_entry": USER_AGENT_TOKEN}
    
    connect_args = {
        **connect_args,
        "http_path": HTTP_PATH,
        "server_hostname": HOST,
        "catalog": CATALOG,
        "schema": SCHEMA
    }

    engine = create_engine(
        f"databricks://token:{ACCESS_TOKEN}@{HOST}",
        connect_args=connect_args,
    )
    return engine

    engine = create_engine(
        f"databricks://token:{ACCESS_TOKEN}@{HOST}?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}",
        connect_args=connect_args,
    )
    return engine


@pytest.fixture()
def base(db_engine):
    return declarative_base(bind=db_engine)


@pytest.fixture()
def session(db_engine):
    return Session(bind=db_engine)


@pytest.fixture()
def metadata_obj(db_engine):
    return MetaData(bind=db_engine)


def test_can_connect(db_engine):
    simple_query = "SELECT 1"
    result = db_engine.execute(simple_query).fetchall()
    assert len(result) == 1


def test_connect_args(db_engine):
    """Verify that extra connect args passed to sqlalchemy.create_engine are passed to DBAPI

    This will most commonly happen when partners supply a user agent entry
    """

    conn = db_engine.connect()
    connection_headers = conn.connection.thrift_backend._transport._headers
    user_agent = connection_headers["User-Agent"]

    expected = f"(sqlalchemy + {USER_AGENT_TOKEN})"
    assert expected in user_agent


@pytest.mark.skipif(sqlalchemy_1_3(), reason="Pandas requires SQLAlchemy >= 1.4")
def test_pandas_upload(db_engine, metadata_obj):

    import pandas as pd

    SCHEMA = os.environ.get("schema")
    try:
        df = pd.read_excel("tests/sqlalchemy/demo_data/MOCK_DATA.xlsx")
        df.to_sql(
            "mock_data",
            db_engine,
            schema=SCHEMA,
            index=False,
            method="multi",
            if_exists="replace",
        )

        df_after = pd.read_sql_table("mock_data", db_engine, schema=SCHEMA)
        assert len(df) == len(df_after)
    except Exception as e:
        raise e
    finally:
        db_engine.execute("DROP TABLE mock_data")


def test_create_table_not_null(db_engine, metadata_obj):

    table_name = "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s"))

    SampleTable = Table(
        table_name,
        metadata_obj,
        Column("name", String(255)),
        Column("episodes", Integer),
        Column("some_bool", BOOLEAN(create_constraint=False), nullable=False),
    )

    metadata_obj.create_all()

    columns = db_engine.dialect.get_columns(
        connection=db_engine.connect(), table_name=table_name
    )

    name_column_description = columns[0]
    some_bool_column_description = columns[2]

    assert name_column_description.get("nullable") is True
    assert some_bool_column_description.get("nullable") is False

    metadata_obj.drop_all()


def test_bulk_insert_with_core(db_engine, metadata_obj, session):

    import random

    num_to_insert = random.choice(range(10_000, 20_000))

    table_name = "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s"))

    names = ["Bim", "Miki", "Sarah", "Ira"]

    SampleTable = Table(
        table_name, metadata_obj, Column("name", String(255)), Column("number", Integer)
    )

    rows = [
        {"name": names[i % 3], "number": random.choice(range(10000))}
        for i in range(num_to_insert)
    ]

    metadata_obj.create_all()
    db_engine.execute(insert(SampleTable).values(rows))

    rows = db_engine.execute(version_agnostic_select(SampleTable)).fetchall()

    assert len(rows) == num_to_insert


def test_create_insert_drop_table_core(base, db_engine, metadata_obj: MetaData):
    """ """

    SampleTable = Table(
        "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s")),
        metadata_obj,
        Column("name", String(255)),
        Column("episodes", Integer),
        Column("some_bool", BOOLEAN(create_constraint=False)),
        Column("dollars", DECIMAL(10, 2)),
    )

    metadata_obj.create_all()

    insert_stmt = insert(SampleTable).values(
        name="Bim Adewunmi", episodes=6, some_bool=True, dollars=decimal.Decimal(125)
    )

    with db_engine.connect() as conn:
        conn.execute(insert_stmt)

    select_stmt = version_agnostic_select(SampleTable)
    resp = db_engine.execute(select_stmt)

    result = resp.fetchall()

    assert len(result) == 1

    metadata_obj.drop_all()


# ORM tests are made following this tutorial
# https://docs.sqlalchemy.org/en/14/orm/quickstart.html


@skipIf(False, "Unity catalog must be supported")
def test_create_insert_drop_table_orm(base, session: Session):
    """ORM classes built on the declarative base class must have a primary key.
    This is restricted to Unity Catalog.
    """

    class SampleObject(base):

        __tablename__ = "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s"))

        name = Column(String(255), primary_key=True)
        episodes = Column(Integer)
        some_bool = Column(BOOLEAN(create_constraint=False))

    base.metadata.create_all()

    sample_object_1 = SampleObject(name="Bim Adewunmi", episodes=6, some_bool=True)
    sample_object_2 = SampleObject(name="Miki Meek", episodes=12, some_bool=False)
    session.add(sample_object_1)
    session.add(sample_object_2)
    session.commit()

    stmt = version_agnostic_select(SampleObject).where(
        SampleObject.name.in_(["Bim Adewunmi", "Miki Meek"])
    )

    if sqlalchemy_1_3():
        output = [i for i in session.execute(stmt)]
    else:
        output = [i for i in session.scalars(stmt)]

    assert len(output) == 2

    base.metadata.drop_all()


def test_dialect_type_mappings(base, db_engine, metadata_obj: MetaData):
    """Confirms that we get back the same time we declared in a model and inserted using Core"""

    SampleTable = Table(
        "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s")),
        metadata_obj,
        Column("string_example", String(255)),
        Column("integer_example", Integer),
        Column("boolean_example", BOOLEAN(create_constraint=False)),
        Column("decimal_example", DECIMAL(10, 2)),
        Column("date_example", Date),
    )

    string_example = ""
    integer_example = 100
    boolean_example = True
    decimal_example = decimal.Decimal(125)
    date_example = datetime.date(2013, 1, 1)

    metadata_obj.create_all()

    insert_stmt = insert(SampleTable).values(
        string_example=string_example,
        integer_example=integer_example,
        boolean_example=boolean_example,
        decimal_example=decimal_example,
        date_example=date_example,
    )

    with db_engine.connect() as conn:
        conn.execute(insert_stmt)

    select_stmt = version_agnostic_select(SampleTable)
    resp = db_engine.execute(select_stmt)

    result = resp.fetchall()
    this_row = result[0]

    assert this_row["string_example"] == string_example
    assert this_row["integer_example"] == integer_example
    assert this_row["boolean_example"] == boolean_example
    assert this_row["decimal_example"] == decimal_example
    assert this_row["date_example"] == date_example

    metadata_obj.drop_all()
