FROM apache/superset:4.1.2

USER root

RUN apt-get update && \
    apt-get install -y build-essential && \
    pip install --no-cache-dir \
      pyhive[hive] \
      thrift \
      thrift-sasl \
      sasl==0.3.1 \
      pure-sasl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER superset

ENV SUPERSET_SECRET_KEY="superset-secret"
RUN superset db upgrade && \
    superset fab create-admin \
      --username admin \
      --firstname Superset \
      --lastname Admin \
      --email admin@superset.com \
      --password admin && \
    superset init
