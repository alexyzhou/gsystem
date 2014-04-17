#!/bin/bash

# Environmental Variables
HADOOP_LIB=$HADOOP_PREFIX/hadoop-core-1.2.1.jar:$HADOOP_PREFIX/lib/*
ZK_LIB=$ZOOKEEPER/zookeeper-3.4.5.jar:$ZOOKEEPER/lib/*
GNODE_LIB=./GNode.jar
GNODE_LOG=./etc/GNode.log
GNODE_PID=./etc/GNode.pid

# ClassName
GN_MAINCLASS=system.GSystemEntry
# ConfigName
GN_CONFIGNAME=systemconf.properties

# Test Parameters
case $1 in
start)
echo -n "Starting GNode on [$HOSTNAME] ... "
if [ -f $GNODE_PID ]; then
	if kill -0 `cat $GNODE_PID` > /dev/null 2>&1; then
        	echo GNode already running as process `cat $ZOOPIDFILE`. 
        	exit 0
      	fi
fi
nohup "java -cp $HADOOP_LIB:$ZK_LIB:$GNODE_LIB $GN_MAINCLASS $GN_CONFIGNAME" > $GNODE_LOG 2>&1 &
if [ $? -eq 0 ] then
	if /bin/echo -n $! > "$GNODE_PID" then
		sleep 1
	echo STARTED
	else
        	echo FAILED TO WRITE PID
        	exit 1
      	fi
else
      	echo SERVER DID NOT START
      	exit 1
fi
;;
stop)
echo -n "Stopping GNode on [$HOSTNAME] ... " 
if kill -9 `cat $GNODE_PID` then
        sleep 1
        echo Stopped
	rm -f $GNODE_PID
        else
                echo FAILED
                exit 1
fi

;;
*)
echo "Usage: GNode.sh [start|stop]"
;;
esac
