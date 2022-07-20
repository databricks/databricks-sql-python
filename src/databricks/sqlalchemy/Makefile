DBSCLI=dbsqlcli
PY=poetry run python3

sample1: 
	$(PY) sample-app-select.py

sample2:
	$(PY) sample-app-reflection.py

clean: drop

create: 
	$(DBSCLI) -e create-table.sql

create-reseq: 
	$(DBSCLI) -e create-table-reseq.sql

create-extra: 
	$(DBSCLI) -e create-table-extra.sql

create-wrong: 
	$(DBSCLI) -e create-table-wrong.sql

describe: 
	$(DBSCLI) -e "DESCRIBE george_chow_dbtest.sample_numtypes;"
	
select: 
	$(DBSCLI) -e "SELECT * FROM george_chow_dbtest.sample_numtypes LIMIT 10;"
	
drop:
	echo y | $(DBSCLI) -e "DROP TABLE IF EXISTS george_chow_dbtest.sample_numtypes;"