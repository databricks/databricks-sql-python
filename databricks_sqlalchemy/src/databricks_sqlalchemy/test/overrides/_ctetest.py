"""The default test setup uses a self-referential foreign key. With our dialect this requires
`use_alter=True` and the fk constraint to be named. So we override this to make the test pass.
"""

from sqlalchemy.testing.suite import CTETest

from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String


class CTETest(CTETest):  # type: ignore
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "some_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
            Column(
                "parent_id", ForeignKey("some_table.id", name="fk_test", use_alter=True)
            ),
        )

        Table(
            "some_other_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
            Column("parent_id", Integer),
        )
