#
# Temporary(?) home for sample apps to guide development of the Databricks SQLAlchemy dialect
#
# NB: This really ought to be using the standard Golden tables. For the while, there are a number
#     of pre-defined tables that is created. 
#
# NB2: the target system is specified via environment variables. The sample apps looks this up at run-time.
#     See env.template.
# 

DBSCLI=dbsqlcli
PY=poetry run python3

.PHONY=sample1 sample2 sample3 clean \
	create create-reseq create-extra create-db drop-db \
	showtables describe select drop

sample1: 
	$(PY) sample-app-select.py

sample2:
	$(PY) sample-app-reflection.py

sample3:
	$(PY) sample-app-insert.py

clean: drop

# caution: create your table in the correct schema!
create: 
	$(DBSCLI) -e create-table.sql

create-reseq: 
	$(DBSCLI) -e create-table-reseq.sql

create-extra: 
	$(DBSCLI) -e create-table-extra.sql

create-wrong: 
	$(DBSCLI) -e create-table-wrong.sql

create-db:
	$(DBSCLI) -e "CREATE DATABASE george_chow_satest;"

drop-db:
	$(DBSCLI) -e "DROP DATABASE IF EXISTS george_chow_satest;"

showtables: 
	$(DBSCLI) -e "USE $(DATABRICKS_SCHEMA); show tables;"

describe: 
	$(DBSCLI) -e "DESCRIBE $(DATABRICKS_SCHEMA).sample_numtypes;"
	$(DBSCLI) -e "DESCRIBE $(DATABRICKS_SCHEMA).sample_strtypes;"
	
select: 
	$(DBSCLI) -e "SELECT * FROM $(DATABRICKS_SCHEMA).sample_numtypes LIMIT 10;"
	$(DBSCLI) -e "SELECT * FROM $(DATABRICKS_SCHEMA).sample_strtypes LIMIT 10;"
	
drop:
	echo y | $(DBSCLI) -e "DROP TABLE IF EXISTS $(DATABRICKS_SCHEMA).sample_numtypes;"
	echo y | $(DBSCLI) -e "DROP TABLE IF EXISTS $(DATABRICKS_SCHEMA).sample_strtypes;"