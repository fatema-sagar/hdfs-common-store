#!/bin/bash
#
# Copyright [2019 - 2019] Confluent Inc.
#


#GCS Sink - Souce mvn commands
gcsSinkITCommand="mvn -Dtest=SinkIT test"
gcsSourceITCommand="mvn -Dtest=SourceIT test"

echo " \n\n >> Update your credentials under the creds folder if not done already."
echo " \n\n >> Running GCS Sink IT tests."
cd ../../
$gcsSinkITCommand

echo " \n\n >> Running GCS Source IT tests."
cd ../../kafka-connect-gcs-source
$gcsSourceITCommand
