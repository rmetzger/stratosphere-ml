#!/bin/bash

# TODO: FINISH AND TEST

INPUT_DIR=../testdata/books
#HDFS_NAMENODE=desktop:9000
HDFS_NAMENODE=localhost:9000 # if running dfs and stratosphere in local(host) mode
HDFS_DIR=pact-test

STRATOSPHERE_HOME=/home/andre/dev/ozone-repo/stratosphere-dist/target/stratosphere-dist-0.2-ozone-bin.dir/stratosphere-0.2-ozone

ARG_TRAIN_FILE="file:///home/andre/dev/datasets/libsvm-rcv1/rcv1_train.binary";
ARG_TEST_FILE="file:///home/andre/dev/datasets/libsvm-rcv1/rcv1_test_20000.binary";
ARG_OUTPUT="file:///home/andre/output-ensemble";

JAR_PATH=logreg-pact/target/logreg-pact-0.0.1-SNAPSHOT-job.jar

# Run task
set -x verbose

# Clean up
#${HADOOP_PREFIX}/bin/hadoop fs -rmr $HDFS_DIR

# Copy input to hadoop
#${HADOOP_PREFIX}/bin/hadoop fs -mkdir $HDFS_DIR/input
#${HADOOP_PREFIX}/bin/hadoop fs -put $INPUT_DIR/* $HDFS_DIR/input

#$STRATOSPHERE_HOME/bin/pact-client.sh run -w -j $JAR_PATH -a 4 hdfs://${HDFS_NAMENODE}/user/andre/${HDFS_DIR}/input hdfs://${HDFS_NAMENODE}/user/andre/${HDFS_DIR}/output
$STRATOSPHERE_HOME/bin/pact-client.sh run -w -j $JAR_PATH -a 4 hdfs://${HDFS_NAMENODE}/user/andre/${HDFS_DIR}/input hdfs://${HDFS_NAMENODE}/user/andre/${HDFS_DIR}/output

# Show running tasks
# $STRATOSPHERE_HOME/bin/pact-client.sh list -r -s

# Show output
${HADOOP_PREFIX}/bin/hadoop fs -cat $HDFS_DIR/output/* | wc -l
