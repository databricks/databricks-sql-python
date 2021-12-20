RUN pip install -r dev_requirements.txt

ENTRYPOINT ["./docker-entrypoint.sh"]
