#!/usr/bin/bash

#$1 will contain the name of the spark namenode
#$2 will contain the port number
#$3 will be the class that contains the job
#$4 name of the jar
#$5 will be the number relating to who's HDFS to connect to. 1=anthony 2=daniel 3=bruce

if [ "$#" -ne 5 ]; then
    echo "Usage: ./run-job.sh [namenode] [portnumber] [class of driver] [jar name in target dir] [java args]"
	exit
fi
echo $#
echo $SPARK_HOME/bin/spark-submit --master spark://$1:$2 --deploy-mode cluster --class $3 --supervise target/scala-2.11/$4_2.11-1.0.jar $5
$SPARK_HOME/bin/spark-submit --master spark://$1:$2 --deploy-mode cluster --class $3 --supervise target/scala-2.11/$4_2.11-1.0.jar $5
