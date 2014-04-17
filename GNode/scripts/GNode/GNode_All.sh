#!/bin/bash

# Environmental Parameters
GnodeBase=~/GNode/
GnodeBin=$GnodeBase/GNode.sh

# Log Parameters
GDate=$(date +%Y%m%d%H%M%S)

# Check Parameters
case $1 in
start)
GCMD=start
;;
stop)
GCMD=stop
;;
*)
echo "Usage: GNode_All.sh [start|stop]"
exit 1
;;
esac

# Commands
$GnodeBin
ssh slave1 "nohup $GnodeBin $GCMD > $GnodeBase/logs/$GDate.$GCMD.log 2>&1 &"
ssh slave2 "nohup $GnodeBin $GCMD > $GnodeBase/logs/$GDate.$GCMD.log 2>&1 &"
