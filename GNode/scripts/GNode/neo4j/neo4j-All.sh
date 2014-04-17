#!/bin/bash

Neo4jBin=/usr/local/neo4j-2.0.1/bin

case $1 in
start)
NCMD=start
;;
stop)
NCMD=stop
;;
*)
echo "Usage: neo4j-All.sh [start|stop]"
exit 1
;;
esac

$Neo4jBin/neo4j $NCMD
ssh slave1 "nohup $Neo4jBin/neo4j $NCMD >> ~/GNode/neo4j/neo4j.log 2>>&1 &"
ssh slave2 "nohup $Neo4jBin/neo4j $NCMD >> ~/GNode/neo4j/neo4j.log 2>>&1 &"
