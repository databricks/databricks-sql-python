#
# Clearinghouse for initiating adhoc PyTest runs to test the Databricks SQLAlchemy dialect
#
# NB: At present, the database fixtures aren't being cleaned up after each run.
#     Use the clean pseudo-targets to remove these after your run.
#
# NB2: the target system (the parameter to --dburi) is specified via environment variables.
#     See env.template.


DBSCLI=dbsqlcli 
PYTEST=poetry run python3 -m pytest 

SUITE_PATH=tests/sqlalchemy

SUITE=test_suite.py

REQ=--requirements src.databricks.sqlalchemy.requirements:Requirements
DBURI=--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)"

.PHONY=all clean showtables full reflection simple str num drop_simpletest drop_reflectiontest


all: full

clean: drop_simpletest drop_reflectiontest \
	drop_booleantest drop_datetest drop_datetimetest drop_integertest drop_numerictest drop_stringtest drop_tableddl

showtables:
	$(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); show tables;"
	$(DBSCLI) -e "USE test_schema; show tables;"
	$(DBSCLI) -e "USE test_schema_2; show tables;"

full:
	$(PYTEST) $(SUITE_PATH) \
		$(DBURI) \
		--log-file=~/.pytestlogs/full.log

sa-bool: drop_booleantest drop_t
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::BooleanTest \
		$(DBURI) 

sa-date: drop_datetest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::DateTest \
		$(DBURI) 

sa-dt: drop_datetimetest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::DateTimeTest \
		$(DBURI) 

sa-int: drop_integertest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::IntegerTest \
		$(DBURI) 

sa-num: drop_numerictest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::NumericTest \
		$(DBURI) 

sa-str: drop_stringtest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::StringTest \
		$(DBURI) 

sa-ddl: drop_tableddl
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::TableDDLTest \
		$(REQ) \
		$(DBURI) 

sa-ddl1: drop_tableddl
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::TableDDLTest:test_create_table \
		$(DBURI) 

sa-ddl2: drop_tableddl
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::TableDDLTest:test_create_table_schema \
		$(DBURI) 

sa-ddl3: drop_tableddl
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::TableDDLTest:test_drop_table \
		$(DBURI) 

sa-join: drop_jointest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::JoinTest \
		$(DBURI) 

reflection:
	$(PYTEST) $(SUITE_PATH)/$(SUITE)::ReflectionTest \
		$(DBURI) 

num:
	$(PYTEST) $(SUITE_PATH)/$(SUITE)::ReflectionTest::test_numtypes \
		$(DBURI) 

str:
	$(PYTEST) $(SUITE_PATH)/$(SUITE)::ReflectionTest::test_strtypes \
		$(DBURI) 

simple:
	$(PYTEST) $(SUITE_PATH)/$(SUITE)::SimpleTest \
		$(DBURI) 

# clean up after SimpleTest run  
drop_simpletest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS simpletest_num;"
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS simpletest_str;"

# clean up after ReflectionTest run  
drop_reflectiontest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS reflectiontest_all_num_types;"
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS reflectiontest_all_str_types;"

# clean up after SQLAlchemy test suite

drop_booleantest: drop_boolean_table

drop_datetest: drop_date_table

drop_datetimetest: drop_date_table

drop_integertest: drop_t drop_tabletest drop_integer_table

drop_numerictest: drop_t drop_tabletest

drop_stringtest: drop_t drop_boolean_table

drop_tableddl: drop__test_table drop_test_table

drop_jointest: drop_a drop_b 


drop_t:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS t;"

drop_tabletest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS tabletest;"

drop_boolean_table:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS boolean_table;"

drop__test_table:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS _test_table;"

drop_test_table:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS test_table;"

drop_a:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS a;"

drop_b:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS b;"

drop_date_table:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS date_table;"

drop_integer_table:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS integer_table;"


# these two schemas are baked into SQLAlchemy's test suite
satestdb:
	$(DBSCLI) -e "CREATE DATABASE test_schema;"
	$(DBSCLI) -e "CREATE DATABASE test_schema_2;"

