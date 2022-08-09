import os, datetime, pytest
import sqlalchemy


@pytest.fixture
def db_engine():

    host = os.getenv("host")
    http_path = os.getenv("http_path")
    api_token = os.getenv("api_token")
    engine = sqlalchemy.create_engine(f"databricks+thrift://token:{api_token}@{host}?http_path={http_path}")
    return engine

def test_basic_connection(db_engine):
    """Make sure we can connect and run basic query
    """

    curs = db_engine.execute("SELECT id FROM RANGE(100)")
    result = curs.fetchall()
    assert len(result) == 100

def test_create_and_drop_table(db_engine):
    """Make sure we can automatically create and drop a table defined with SQLAlchemy's MetaData object
    """
    
    mdo = sqlalchemy.MetaData()
    this_moment = datetime.datetime.utcnow().strftime("%s")
    
    tname = f"integration_test_table_{this_moment}"

    t1 = sqlalchemy.Table(
        tname,
        mdo,
        sqlalchemy.Column('f_short', sqlalchemy.types.SMALLINT),
        sqlalchemy.Column('f_int', sqlalchemy.types.Integer),
        sqlalchemy.Column('f_long', sqlalchemy.types.BigInteger),
        sqlalchemy.Column('f_float', sqlalchemy.types.Float),
        sqlalchemy.Column('f_decimal', sqlalchemy.types.DECIMAL),
        sqlalchemy.Column('f_boolean', sqlalchemy.types.BOOLEAN)
    )

    mdo.create_all(bind=db_engine,checkfirst=True)

    check_it_exists = db_engine.execute(f"DESCRIBE TABLE EXTENDED {tname}")
    
    mdo.drop_all(db_engine, checkfirst=True)
