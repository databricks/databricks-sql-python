DBSCLI=dbsqlcli 
PYTEST=poetry run python3 -m pytest 

SUITE_PATH=tests/sqlalchemy

SUITE=test_suite.py

# NB: add noglob when issuing this iteractively in zsh if you have globbing set
#--dburi "databricks+thrift://token:dapie08893e611277fabdd78a186a1331278@e2-dogfood.staging.cloud.databricks.com?http_path=/sql/protocolv1/o/6051921418418893/0819-204509-hill72"

all: full

clean: drop_simpletest drop_reflectiontest

showtables:
	$(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); show tables;"


full:
	$(PYTEST) $(SUITE_PATH) \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)"

reflection:
	$(PYTEST) $(SUITE_PATH)/test_query.py::ReflectionTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)"

simple:
	$(PYTEST) $(SUITE_PATH)/test_query.py::SimpleTest \
		--dburi "databricks+thrift://token:$(DATABRICKS_TOKEN)@$(DATABRICKS_SERVER_HOSTNAME)/$(DATABRICKS_SCHEMA)?http_path=$(DATABRICKS_HTTP_PATH)"


drop_simpletest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS simpletest_num;"

drop_reflectiontest:
	echo y | $(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); DROP TABLE IF EXISTS reflectiontest;"

