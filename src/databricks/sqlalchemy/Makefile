DBSCLI=dbsqlcli
PY=poetry run python3

sample1: 
	$(PY) sample-app-select.py

sample2:
	$(PY) sample-app-reflection.py

clean: drop

# caution: create table in the correct schema!
create: 
	# $(DBSCLI) -e create-table-sa.sql
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