import os

import pytest
from sqlalchemy import (
    BigInteger,
    Column,
    Engine,
    Identity,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session


class IdentityTestBase:
    @pytest.fixture(scope="module")
    def engine(self):
        HOST = os.environ.get("host")
        HTTP_PATH = os.environ.get("http_path")
        ACCESS_TOKEN = os.environ.get("access_token")
        CATALOG = os.environ.get("catalog")
        SCHEMA = os.environ.get("schema")

        connection_string = f"databricks://token:{ACCESS_TOKEN}@{HOST}?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}"
        connect_args = {"_user_agent_entry": "pysql e2e tests"}

        engine = create_engine(connection_string, connect_args=connect_args)
        return engine


class TestIdentityBehaviorORM(IdentityTestBase):
    class Base(DeclarativeBase):
        pass

    class Foo(Base):
        __tablename__ = "foo"

        id = Column(BigInteger, Identity(), primary_key=True)
        value = Column(String)

    @pytest.fixture
    def session(self, engine):
        with Session(engine) as session:
            self.Base.metadata.create_all(bind=engine)
            yield session
            self.Base.metadata.drop_all(bind=engine)

    def test_autoincrement_identity(self, session):
        sample = self.Foo(value="test")
        session.add(sample)
        
        with pytest.raises(Exception):
            session.commit()



class TestIdentityColumnBehavior(IdentityTestBase):
    @pytest.fixture
    def table(self, engine):
        md = MetaData()
        tbl = Table(
            "foo",
            md,
            Column("bar", String, comment="column comment"),
            Column("id", BigInteger, Identity()),
        )
        md.create_all(bind=engine)

        yield tbl

        md.drop_all(bind=engine)

    def test_identity_column_autoincrements(self, engine: Engine, table: Table):
        stmt1 = table.insert().values(bar="baz")
        stmt2 = table.select()
        with engine.begin() as conn:
            conn.execute(stmt1)
            result = conn.execute(stmt2).fetchone()

        assert result.id == 1
