#!/bin/bash

# Environmental Variables
HADOOP_LIB=$HADOOP_PREFIX/hadoop-core-1.2.1.jar:$HADOOP_PREFIX/lib/*
ZK_LIB=$ZOOKEEPER/zookeeper-3.4.5.jar:$ZOOKEEPER/lib/*
GNODE_LIB=./GNode-nocp.jar

# ClassName
GN_MAINCLASS=system.GSystemEntry
# ConfigName
GN_CONFIGNAME=systemconf.properties

# Command
java -cp $HADOOP_LIB:$ZK_LIB:$GNODE_LIB $GN_MAINCLASS $GN_CONFIGNAME
