from sqlalchemy.testing.suite import *


class ComponentReflectionTest():
   
    @testing.skip("Databricks hasn't validated this behaviour")
    def test_get_pk_constraint(self):
        return