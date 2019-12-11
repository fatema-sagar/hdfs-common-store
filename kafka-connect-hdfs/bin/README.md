# For Developers

The `bin` directory in this project contains several files useful for testing:

* `run-source-connector.sh` - script to run a local source connector in a standalone worker.
* `run-sink-connector.sh` - script to run a local sink connector in a standalone worker.

The `config` directory in this project contains the following:

* `connect-log4j.properties` - the Log4J configuration used by the `run-source-connector.sh` script.
* `connect-avro-standalone-local.properties` - the Connect standalone worker properties
that use the *Avro converter* for keys and values,
and that requires Zookeeper, Kafka, and Schema Registry are running locally.
* `my-source-quickstart.properties` - a sample configuration file for the source connector,
*which must be edited with your HDFS Source Connector credentials*.
* `my-sink-quickstart.properties` - a sample configuration file for the source connector,
*which must be edited with your HDFS Source Connector credentials*.


 ##  HDFS Source Connector Source Connector

Start a terminal and change to the top-level directory of this project.


### Running the Source Connector


Then, use the following command to run a Connect standalone worker with your locally-built HDFS Source Connector source connector:

```
$ bin/run-source-connector.sh
```

This sets up Log4J to use the `config/connect-log4j.properties` in this directory and then
runs a Connect standalone worker with the `config/connect-avro-standalone-local.properties` worker configuration
and the `config/my-source-quickstart.properties` connector configuration file.

Use CTRL-C to terminate the worker.


### Building and Running the Source Connector


If you want to have the script clean, compile, package and then run the connector, use the following command:

```
$ BUILD=y ./bin/run-source-connector.sh
```

The `BUILD` environment variable can be used with any of the other environment variables.

Use CTRL-C to terminate the worker.



### Debugging the Source Connector


To debug the source connector, use the following command to run a Connect standalone worker with your locally-built HDFS Source Connector source connector:

```
$ DEBUG=y SUSPEND=y ./bin/run-source-connector.sh
```

This configures the JVM for remote debugging, suspends the JVM (allowing you to connect with your IDE to port 5005),
sets up Log4J to use the `config/connect-log4j.properties` in this directory and then
runs a Connect standalone worker with the `config/connect-avro-standalone-local.properties` worker configuration
and the `config/my-source-quickstart.properties` connector configuration file.

Use CTRL-C to terminate the worker.


## HDFS Source Connector Sink Connector


Start a terminal and change to the top-level directory of this project.


### Running the Sink Connector


Then, use the following command to run a Connect standalone worker with your locally-built HDFS Source Connector sink connector:

```
$ bin/run-sink-connector.sh
```

This sets up Log4J to use the `config/connect-log4j.properties` in this directory and then
runs a Connect standalone worker with the `config/connect-avro-standalone-local.properties` worker configuration
and the `config/my-sink-quickstart.properties` connector configuration file.

Use CTRL-C to terminate the worker.


### Building and Running the Sink Connector


If you want to have the script clean, compile, package and then run the connector, use the following command:

```
$ BUILD=y ./bin/run-sink-connector.sh
```

The `BUILD` environment variable can be used with any of the other environment variables.

Use CTRL-C to terminate the worker.



### Debugging the Sink Connector


To debug the sink connector, use the following command to run a Connect standalone worker with your locally-built HDFS Source Connector sink connector:

```
$ DEBUG=y SUSPEND=y ./bin/run-sink-connector.sh
```

This configures the JVM for remote debugging, suspends the JVM (allowing you to connect with your IDE to port 5005),
sets up Log4J to use the `config/connect-log4j.properties` in this directory and then
runs a Connect standalone worker with the `config/connect-avro-standalone-local.properties` worker configuration
and the `config/my-sink-quickstart.properties` connector configuration file.

Use CTRL-C to terminate the worker.
