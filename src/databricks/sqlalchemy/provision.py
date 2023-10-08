from sqlalchemy.testing.provision import create_db, drop_db

@create_db.for_db("databricks")
def _databricks_create_db(cfg, eng, ident):
    with eng.begin() as conn:
        create_string = "CREATE SCHEMA `main`.`%s`" % ident
        conn.exec_driver_sql(create_string)

@drop_db.for_db("databricks")
def _databricks_drop_db(cfg, eng, ident):
    with eng.begin() as conn:
        conn.exec_driver_sql("DROP SCHEMA `main`.`%s`" % ident)