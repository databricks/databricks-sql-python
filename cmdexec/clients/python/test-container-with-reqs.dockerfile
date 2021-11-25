FROM python:3.7-slim-buster

RUN pip install grpcio==1.41.0 \
                pyarrow==5.0.0 \
                protobuf==3.18.1 \
                cryptography==35.0.0 \
                thrift==0.13.0 \
                pandas==1.3.4 \
                future==0.18.2 \
                python-dateutil==2.8.2

ENTRYPOINT ["./docker-entrypoint.sh"]
