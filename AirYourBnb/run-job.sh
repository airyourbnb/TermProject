#!/usr/bin/bash

#$0 will contain the name of the spark namenode
#$1 will contain the port number
#$2 will be the class that contains the job
#$3 name of the jar 

#FIXME this doesnt work 

if [ "$#" -ne 4 ]; then
    echo "Usage: ./run-job.sh [namenode] [portnumber] [class of driver] [jar name in target dir]"
	exit
fi
echo $#

$SPARK_HOME/bin/spark-submit --master spark://"$0":"$1" --deploy-mode cluster --class "$2" --supervise target/scala-2.11/"$3"_2.11-1.0.jar 
