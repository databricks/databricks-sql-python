import datetime
import decimal
from typing import Tuple, Union, List
from unittest import skipIf

import pytest
from sqlalchemy import (
    Column,
    MetaData,
    Table,
    Text,
    create_engine,
    insert,
    select,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.schema import DropColumnComment, SetColumnComment
from sqlalchemy.types import BOOLEAN, DECIMAL, Date, Integer, String

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


def version_agnostic_connect_arguments(connection_details) -> Tuple[str, dict]:
    HOST = connection_details["host"]
    HTTP_PATH = connection_details["http_path"]
    ACCESS_TOKEN = connection_details["access_token"]
    CATALOG = connection_details["catalog"]
    SCHEMA = connection_details["schema"]

    ua_connect_args = {"_user_agent_entry": USER_AGENT_TOKEN}

    if sqlalchemy_1_3():
        conn_string = f"databricks://token:{ACCESS_TOKEN}@{HOST}"
        connect_args = {
            **ua_connect_args,
            "http_path": HTTP_PATH,
            "server_hostname": HOST,
            "catalog": CATALOG,
            "schema": SCHEMA,
        }

        return conn_string, connect_args
    else:
        return (
            f"databricks://token:{ACCESS_TOKEN}@{HOST}?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}",
            ua_connect_args,
        )


@pytest.fixture
def db_engine(connection_details) -> Engine:
    conn_string, connect_args = version_agnostic_connect_arguments(connection_details)
    return create_engine(conn_string, connect_args=connect_args)


def run_query(db_engine: Engine, query: Union[str, Text]):
    if not isinstance(query, Text):
        _query = text(query)  # type: ignore
    else:
        _query = query  # type: ignore
    with db_engine.begin() as conn:
        return conn.execute(_query).fetchall()


@pytest.fixture
def samples_engine(connection_details) -> Engine:
    details = connection_details.copy()
    details["catalog"] = "samples"
    details["schema"] = "nyctaxi"
    conn_string, connect_args = version_agnostic_connect_arguments(details)
    return create_engine(conn_string, connect_args=connect_args)


@pytest.fixture()
def base(db_engine):
    return declarative_base()


@pytest.fixture()
def session(db_engine):
    return Session(db_engine)


@pytest.fixture()
def metadata_obj(db_engine):
    return MetaData()


def test_can_connect(db_engine):
    simple_query = "SELECT 1"
    result = run_query(db_engine, simple_query)
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
@pytest.mark.skip(
    reason="DBR is currently limited to 256 parameters per call to .execute(). Test cannot pass."
)
def test_pandas_upload(db_engine, metadata_obj):
    import pandas as pd

    SCHEMA = "default"
    try:
        df = pd.read_excel(
            "src/databricks/sqlalchemy/test_local/e2e/demo_data/MOCK_DATA.xlsx"
        )
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


def test_create_table_not_null(db_engine, metadata_obj: MetaData):
    table_name = "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s"))

    SampleTable = Table(
        table_name,
        metadata_obj,
        Column("name", String(255)),
        Column("episodes", Integer),
        Column("some_bool", BOOLEAN, nullable=False),
    )

    metadata_obj.create_all(db_engine)

    columns = db_engine.dialect.get_columns(
        connection=db_engine.connect(), table_name=table_name
    )

    name_column_description = columns[0]
    some_bool_column_description = columns[2]

    assert name_column_description.get("nullable") is True
    assert some_bool_column_description.get("nullable") is False

    metadata_obj.drop_all(db_engine)


def test_column_comment(db_engine, metadata_obj: MetaData):
    table_name = "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s"))

    column = Column("name", String(255), comment="some comment")
    SampleTable = Table(table_name, metadata_obj, column)

    metadata_obj.create_all(db_engine)
    connection = db_engine.connect()

    columns = db_engine.dialect.get_columns(
        connection=connection, table_name=table_name
    )

    assert columns[0].get("comment") == "some comment"

    column.comment = "other comment"
    connection.execute(SetColumnComment(column))

    columns = db_engine.dialect.get_columns(
        connection=connection, table_name=table_name
    )

    assert columns[0].get("comment") == "other comment"

    connection.execute(DropColumnComment(column))

    columns = db_engine.dialect.get_columns(
        connection=connection, table_name=table_name
    )

    assert columns[0].get("comment") == None

    metadata_obj.drop_all(db_engine)


def test_bulk_insert_with_core(db_engine, metadata_obj, session):
    import random

    # Maximum number of parameter is 256. 256/4 == 64
    num_to_insert = 64

    table_name = "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s"))

    names = ["Bim", "Miki", "Sarah", "Ira"]

    SampleTable = Table(
        table_name, metadata_obj, Column("name", String(255)), Column("number", Integer)
    )

    rows = [
        {"name": names[i % 3], "number": random.choice(range(64))}
        for i in range(num_to_insert)
    ]

    metadata_obj.create_all(db_engine)
    with db_engine.begin() as conn:
        conn.execute(insert(SampleTable).values(rows))

    with db_engine.begin() as conn:
        rows = conn.execute(version_agnostic_select(SampleTable)).fetchall()

    assert len(rows) == num_to_insert


def test_create_insert_drop_table_core(base, db_engine, metadata_obj: MetaData):
    """ """

    SampleTable = Table(
        "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s")),
        metadata_obj,
        Column("name", String(255)),
        Column("episodes", Integer),
        Column("some_bool", BOOLEAN),
        Column("dollars", DECIMAL(10, 2)),
    )

    metadata_obj.create_all(db_engine)

    insert_stmt = insert(SampleTable).values(
        name="Bim Adewunmi", episodes=6, some_bool=True, dollars=decimal.Decimal(125)
    )

    with db_engine.connect() as conn:
        conn.execute(insert_stmt)

    select_stmt = version_agnostic_select(SampleTable)
    with db_engine.begin() as conn:
        resp = conn.execute(select_stmt)

    result = resp.fetchall()

    assert len(result) == 1

    metadata_obj.drop_all(db_engine)


# ORM tests are made following this tutorial
# https://docs.sqlalchemy.org/en/14/orm/quickstart.html


@skipIf(False, "Unity catalog must be supported")
def test_create_insert_drop_table_orm(db_engine):
    """ORM classes built on the declarative base class must have a primary key.
    This is restricted to Unity Catalog.
    """

    class Base(DeclarativeBase):
        pass

    class SampleObject(Base):
        __tablename__ = "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s"))

        name: Mapped[str] = mapped_column(String(255), primary_key=True)
        episodes: Mapped[int] = mapped_column(Integer)
        some_bool: Mapped[bool] = mapped_column(BOOLEAN)

    Base.metadata.create_all(db_engine)

    sample_object_1 = SampleObject(name="Bim Adewunmi", episodes=6, some_bool=True)
    sample_object_2 = SampleObject(name="Miki Meek", episodes=12, some_bool=False)

    session = Session(db_engine)
    session.add(sample_object_1)
    session.add(sample_object_2)
    session.flush()

    stmt = version_agnostic_select(SampleObject).where(
        SampleObject.name.in_(["Bim Adewunmi", "Miki Meek"])
    )

    if sqlalchemy_1_3():
        output = [i for i in session.execute(stmt)]
    else:
        output = [i for i in session.scalars(stmt)]

    assert len(output) == 2

    Base.metadata.drop_all(db_engine)


def test_dialect_type_mappings(db_engine, metadata_obj: MetaData):
    """Confirms that we get back the same time we declared in a model and inserted using Core"""

    class Base(DeclarativeBase):
        pass

    SampleTable = Table(
        "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s")),
        metadata_obj,
        Column("string_example", String(255)),
        Column("integer_example", Integer),
        Column("boolean_example", BOOLEAN),
        Column("decimal_example", DECIMAL(10, 2)),
        Column("date_example", Date),
    )

    string_example = ""
    integer_example = 100
    boolean_example = True
    decimal_example = decimal.Decimal(125)
    date_example = datetime.date(2013, 1, 1)

    metadata_obj.create_all(db_engine)

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
    with db_engine.begin() as conn:
        resp = conn.execute(select_stmt)

    result = resp.fetchall()
    this_row = result[0]

    assert this_row.string_example == string_example
    assert this_row.integer_example == integer_example
    assert this_row.boolean_example == boolean_example
    assert this_row.decimal_example == decimal_example
    assert this_row.date_example == date_example

    metadata_obj.drop_all(db_engine)


def test_inspector_smoke_test(samples_engine: Engine):
    """It does not appear that 3L namespace is supported here"""

    schema, table = "nyctaxi", "trips"

    try:
        inspector = Inspector.from_engine(samples_engine)
    except Exception as e:
        assert False, f"Could not build inspector: {e}"

    # Expect six columns
    columns = inspector.get_columns(table, schema=schema)

    # Expect zero views, but the method should return
    views = inspector.get_view_names(schema=schema)

    assert (
        len(columns) == 6
    ), "Dialect did not find the expected number of columns in samples.nyctaxi.trips"
    assert len(views) == 0, "Views could not be fetched"


@pytest.mark.skip(reason="engine.table_names has been removed in sqlalchemy verison 2")
def test_get_table_names_smoke_test(samples_engine: Engine):
    with samples_engine.connect() as conn:
        _names = samples_engine.table_names(schema="nyctaxi", connection=conn)  # type: ignore
        _names is not None, "get_table_names did not succeed"


def test_has_table_across_schemas(
    db_engine: Engine, samples_engine: Engine, catalog: str, schema: str
):
    """For this test to pass these conditions must be met:
    - Table samples.nyctaxi.trips must exist
    - Table samples.tpch.customer must exist
    - The `catalog` and `schema` environment variables must be set and valid
    """

    with samples_engine.connect() as conn:
        # 1) Check for table within schema declared at engine creation time
        assert samples_engine.dialect.has_table(connection=conn, table_name="trips")

        # 2) Check for table within another schema in the same catalog
        assert samples_engine.dialect.has_table(
            connection=conn, table_name="customer", schema="tpch"
        )

        # 3) Check for a table within a different catalog
        # Create a table in a different catalog
        with db_engine.connect() as conn:
            conn.execute(text("CREATE TABLE test_has_table (numbers_are_cool INT);"))

            try:
                # Verify that this table is not found in the samples catalog
                assert not samples_engine.dialect.has_table(
                    connection=conn, table_name="test_has_table"
                )
                # Verify that this table is found in a separate catalog
                assert samples_engine.dialect.has_table(
                    connection=conn,
                    table_name="test_has_table",
                    schema=schema,
                    catalog=catalog,
                )
            finally:
                conn.execute(text("DROP TABLE test_has_table;"))


def test_user_agent_adjustment(db_engine):
    # If .connect() is called multiple times on an engine, don't keep pre-pending the user agent
    # https://github.com/databricks/databricks-sql-python/issues/192
    c1 = db_engine.connect()
    c2 = db_engine.connect()

    def get_conn_user_agent(conn):
        return conn.connection.dbapi_connection.thrift_backend._transport._headers.get(
            "User-Agent"
        )

    ua1 = get_conn_user_agent(c1)
    ua2 = get_conn_user_agent(c2)
    same_ua = ua1 == ua2

    c1.close()
    c2.close()

    assert same_ua, f"User agents didn't match \n {ua1} \n {ua2}"


@pytest.fixture
def sample_table(metadata_obj: MetaData, db_engine: Engine):
    """This fixture creates a sample table and cleans it up after the test is complete."""
    from databricks.sqlalchemy._parse import GET_COLUMNS_TYPE_MAP

    table_name = "PySQLTest_{}".format(datetime.datetime.utcnow().strftime("%s"))

    args: List[Column] = [
        Column(colname, coltype) for colname, coltype in GET_COLUMNS_TYPE_MAP.items()
    ]

    SampleTable = Table(table_name, metadata_obj, *args)

    metadata_obj.create_all(db_engine)

    yield table_name

    metadata_obj.drop_all(db_engine)


def test_get_columns(db_engine, sample_table: str):
    """Created after PECO-1297 and Github Issue #295 to verify that get_columsn behaves like it should for all known SQLAlchemy types"""

    inspector = Inspector.from_engine(db_engine)

    # this raises an exception if `parse_column_info_from_tgetcolumnsresponse` fails a lookup
    columns = inspector.get_columns(sample_table)

    assert True


class TestCommentReflection:
    @pytest.fixture(scope="class")
    def engine(self, connection_details: dict):
        HOST = connection_details["host"]
        HTTP_PATH = connection_details["http_path"]
        ACCESS_TOKEN = connection_details["access_token"]
        CATALOG = connection_details["catalog"]
        SCHEMA = connection_details["schema"]

        connection_string = f"databricks://token:{ACCESS_TOKEN}@{HOST}?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}"
        connect_args = {"_user_agent_entry": USER_AGENT_TOKEN}

        engine = create_engine(connection_string, connect_args=connect_args)
        return engine

    @pytest.fixture
    def inspector(self, engine: Engine) -> Inspector:
        return Inspector.from_engine(engine)

    @pytest.fixture(scope="class")
    def table(self, engine):
        md = MetaData()
        tbl = Table(
            "foo",
            md,
            Column("bar", String, comment="column comment"),
            comment="table comment",
        )
        md.create_all(bind=engine)

        yield tbl

        md.drop_all(bind=engine)

    def test_table_comment_reflection(self, inspector: Inspector, table: Table):
        comment = inspector.get_table_comment(table.name)
        assert comment == {"text": "table comment"}

    def test_column_comment(self, inspector: Inspector, table: Table):
        result = inspector.get_columns(table.name)[0].get("comment")
        assert result == "column comment"
