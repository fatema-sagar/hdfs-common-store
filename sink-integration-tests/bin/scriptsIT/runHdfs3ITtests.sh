#!/bin/bash
#
# Copyright [2019 - 2019] Confluent Inc.
#

#HDFS3 Sink - Souce mvn commands
hdfs3SinkITCommand="mvn -Dtest=Hdfs3SinkIT test"
hdfs3SourceITCommand="mvn -Dtest=Hdfs3SourceIT test"

echo " \n\n >> Update your properties under the properties folder 'sink-integration-tests/bin/scriptsIT/properties' if not done already."
echo " \n\n >> Running HDFS3 Sink IT tests."
cd ../../
$hdfs3SinkITCommand

echo " \n\n >> Running HDFS3 Source IT tests."
cd ../../kafka-connect-hdfs3-source
$hdfs3SourceITCommand