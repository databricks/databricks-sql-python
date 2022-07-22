DBSCLI=dbsqlcli 
PYTEST=poetry run python3 -m pytest 

SUITE_PATH=tests/sqlalchemy

SUITE=test_suite.py

all: full

clean: drop_simpletest drop_reflectiontest

showtables:
	$(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); show tables;"


full:
	$(PYTEST) $(SUITE_PATH) \
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

