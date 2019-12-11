#!/usr/bin/env bash
#
# Copyright [2019 - 2019] Confluent Inc.
#

# ---------------------------------------
# Run from the project's parent directory
# ---------------------------------------

#[
: ${DEBUG:='n'}
: ${SUSPEND:='n'}
: ${BUILD:='n'}
set -e

if [ "$DEBUG" = "y" ]; then
	echo "Enabling debug on address 5005 with suspend=${SUSPEND}"
	export KAFKA_JMX_OPTS="-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=${SUSPEND},address=5005"
fi

if [ "$BUILD" = "y" ]; then
    echo "Building the module"
    mvn clean package
fi

export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$(pwd)/bin/connect-log4j.properties"

echo "Starting standalone..."
echo "Using log configuration ${KAFKA_LOG4J_OPTS}"
connect-standalone config/connect-avro-local.properties config/S3SourceConnector.properties