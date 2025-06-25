FROM apache/spark:3.4.1
USER root
ENV SPARK_HOME=/opt/spark

RUN apt-get update && apt-get install -y gettext && rm -rf /var/lib/apt/lists/*

COPY spark/conf/spark-defaults.conf.template /opt/spark/conf/spark-defaults.conf.template
COPY spark/entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
