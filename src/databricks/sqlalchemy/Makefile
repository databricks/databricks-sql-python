DBSCLI=dbsqlcli
PY=poetry run python3

sample1: 
	$(PY) sample-app-select.py

sample2:

clean: drop

create: 
	$(DBSCLI) -e create-table.sql

describe: 
	$(DBSCLI) -e describe-table.sql
	
select: 
	$(DBSCLI) -e select-table.sql
	
drop:
	echo y | $(DBSCLI) -e drop-table.sql