#!/bin/bash

. "/opt/spark/bin/load-spark-env.sh"

if [ "$SPARK_WORKLOAD" == "master" ];
then
export SPARK_MASTER_HOST=`hostname`

/opt/spark/spark-3.5.0-bin-hadoop3/sbin/start-master.sh

elif [ "$SPARK_WORKLOAD" == "worker" ];
then

sleep 5
/opt/spark/spark-3.5.0-bin-hadoop3/sbin/start-worker.sh $SPARK_MASTER

elif [ "$SPARK_WORKLOAD" == "slave" ];
then

sleep 5
/opt/spark/spark-3.5.0-bin-hadoop3/sbin/start-slave.sh $SPARK_MASTER

else
	echo "wut"
fi

if [ $? != 0 ];
then
exit 1
fi

tail -f /dev/null