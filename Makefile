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
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" \
		--log-file=~/.pytestlogs/full.log

sa-bool: drop_booleantest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::BooleanTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" 

sa-date: drop_datetest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::DateTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" 

sa-dt: drop_datetimetest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::DateTimeTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" 

sa-int: drop_integertest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::IntegerTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" 

sa-num: drop_numerictest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::NumericTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" 

sa-str: drop_stringtest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::StringTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" 

sa-ddl: drop_tableddl
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::TableDDLTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" 

sa-ddl1: drop_tableddl
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::TableDDLTest:test_create_table \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" 

sa-ddl2: drop_tableddl
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::TableDDLTest:test_create_table_schema \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" 

sa-ddl3: drop_tableddl
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::TableDDLTest:test_drop_table \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" 

sa-join: drop_jointest
	$(PYTEST) $(SUITE_PATH)/test_full_sa.py::JoinTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)" 

reflection:
	$(PYTEST) $(SUITE_PATH)/$(SUITE)::ReflectionTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)"

num:
	$(PYTEST) $(SUITE_PATH)/$(SUITE)::ReflectionTest::test_numtypes \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)"

str:
	$(PYTEST) $(SUITE_PATH)/$(SUITE)::ReflectionTest::test_strtypes \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)"

simple:
	$(PYTEST) $(SUITE_PATH)/$(SUITE)::SimpleTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)"

# clean up after SimpleTest run  
drop_simpletest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS simpletest_num;"
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS simpletest_str;"

# clean up after ReflectionTest run  
drop_reflectiontest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS reflectiontest_all_num_types;"
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS reflectiontest_all_str_types;"

# clean up after SQLAlchemy test suite

drop_booleantest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS boolean_table;"

drop_datetest:
drop_datetimetest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS date_table;"


drop_integertest:
drop_numerictest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS t;"
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS tabletest;"


drop_stringtest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS boolean_table;"
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS t;"

drop_tableddl:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS _test_table;"
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS test_table;"

drop_jointest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS a;"
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS b;"


# these two schemas are baked into SQLAlchemy's test suite
satestdb:
	$(DBSCLI) -e "CREATE DATABASE test_schema;"
	$(DBSCLI) -e "CREATE DATABASE test_schema_2;"

