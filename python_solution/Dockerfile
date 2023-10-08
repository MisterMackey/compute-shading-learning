FROM eclipse-temurin:17-jre

WORKDIR /spark
RUN curl https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz --output /spark/spark.tgz
RUN mkdir /opt/spark
RUN tar -xzf spark.tgz -C /opt/spark

ENV SPARK_MASTER_PORT=7077 \
	SPARK_MASTER_WEBUI_PORT=8080 \
	SPARK_LOG_DIR=/opt/spark/logs \
	SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
	SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
	SPARK_WORKER_WEBUI_PORT=8080 \
	SPARK_WORKER_PORT=7000 \
	SPARK_MASTER="spark://spark-master:7077" \
	SPARK_WORKLOAD="master"

RUN mkdir /opt/spark/logs
EXPOSE 8080 7077 7000 6066

COPY start.sh /start.sh

CMD ["/bin/bash", "/start.sh"]