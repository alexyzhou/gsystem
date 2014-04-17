#!/bin/bash

#Check Parameters
case $1 in
start)
ZKCMD=start
;;
stop)
ZKCMD=stop
;;
*)
echo "Usage: zkAll.sh [start|stop]"
exit 1
;;
esac

$ZOOKEEPER/bin/zkServer.sh $ZKCMD

ssh slave1 "$ZOOKEEPER/bin/zkServer.sh $ZKCMD"
ssh slave2 "$ZOOKEEPER/bin/zkServer.sh $ZKCMD"
