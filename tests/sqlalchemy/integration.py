import os, datetime, pytest
import sqlalchemy


@pytest.fixture
def db_engine():

    host = os.getenv("host")
    http_path = os.getenv("http_path")
    access_token = os.getenv("access_token")
    schema = os.getenv("schema") or "default"
    engine = sqlalchemy.create_engine(
        f"databricks+thrift://token:{access_token}@{host}/{schema}?http_path={http_path}"
    )
    return engine


def test_constraints(db_engine):
    """Make sure we can handle any constraints that's passed in during table declaration.
    In the immediate term, the dialect silently ignores them. But when information constraint is
    complete, constraints need to be handled.
    """

    mdo = sqlalchemy.MetaData()
    this_moment = datetime.datetime.utcnow().strftime("%s")

    tname = f"integration_test_table_{this_moment}"
    tname2 = f"integration_test_table_{this_moment}_items"

    t1 = sqlalchemy.Table(
        tname,
        mdo,
        sqlalchemy.Column("f_primary", sqlalchemy.types.Integer, primary_key=True),
        sqlalchemy.Column("f_nullable", sqlalchemy.types.Integer, nullable=False),
        sqlalchemy.Column("f_unique", sqlalchemy.types.Integer, unique=True),
    )

    t2 = sqlalchemy.Table(
        tname2,
        mdo,
        sqlalchemy.Column(
            "f_foreign",
            sqlalchemy.types.Integer,
            sqlalchemy.ForeignKey(f"{tname}.f_primary"),
            nullable=False,
        ),
    )

    mdo.create_all(bind=db_engine, checkfirst=True)

    check_it_exists = db_engine.execute(f"DESCRIBE TABLE EXTENDED {tname}")

    mdo.drop_all(db_engine, checkfirst=True)


def test_basic_connection(db_engine):
    """Make sure we can connect and run basic query"""

    curs = db_engine.execute("SELECT id FROM RANGE(100)")
    result = curs.fetchall()
    assert len(result) == 100


def test_create_and_drop_table(db_engine):
    """Make sure we can automatically create and drop a table defined with SQLAlchemy's MetaData object
    while exercising all supported types.
    """

    mdo = sqlalchemy.MetaData()
    this_moment = datetime.datetime.utcnow().strftime("%s")

    tname = f"integration_test_table_{this_moment}"

    t1 = sqlalchemy.Table(
        tname,
        mdo,
        sqlalchemy.Column("f_short", sqlalchemy.types.SMALLINT),
        sqlalchemy.Column("f_int", sqlalchemy.types.Integer),
        sqlalchemy.Column("f_long", sqlalchemy.types.BigInteger),
        sqlalchemy.Column("f_float", sqlalchemy.types.Float),
        sqlalchemy.Column("f_decimal", sqlalchemy.types.DECIMAL),
        sqlalchemy.Column("f_boolean", sqlalchemy.types.BOOLEAN),
        sqlalchemy.Column("f_str", sqlalchemy.types.String),
    )

    mdo.create_all(bind=db_engine, checkfirst=False)

    check_it_exists = db_engine.execute(f"DESCRIBE TABLE EXTENDED {tname}")

    mdo.drop_all(db_engine, checkfirst=True)
