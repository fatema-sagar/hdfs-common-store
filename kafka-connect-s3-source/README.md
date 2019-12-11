# For Developers

The `bin` directory in this project contains several files useful for testing:

* `run-source-connector.sh` - script to run a local Connect standalone worker.
* `connect-log4j.properties` - the Log4J configuration used by the `run-source-connector.sh` script.
* `connect-json-standalone.properties` - the Connect standalone worker properties
that use the *JSON converter* for keys and values,
and that requires Kafka and Zookeeper are running locally.
* `connect-avro-standalone.properties` - the Connect standalone worker properties
that use the *Avro converter* for keys and values,
and that requires Kafka and Zookeeper are running locally.

The `config` directory in this project contains the following:

* `salesforce-example.properties` - a sample configuration file for the Salesforce connector,
*which must be edited with your Salesforce credentials*.



## S3 Source Connector


Start a terminal and change to the top-level directory of this project.


### Running the Source Connector


Then, use the following command to run a Connect standalone worker with your locally-built S3 source connector:

```
$ bin/run-source-connector.sh
```

This sets up Log4J to use the `bin/connect-log4j.properties` in this directory and then
runs a Connect standalone worker with the `bin/connect-avro-standalone.properties` worker configuration
and the `config/S3SourceConnector.properties` connector configuration file.

Use CTRL-C to terminate the worker.


### Building and Running the Source Connector


If you want to have the script clean, compile, package and then run the connector, use the following command:

```
$ BUILD=y ./bin/run-source-connector.sh
```

The `BUILD` environment variable can be used with any of the other environment variables.

Use CTRL-C to terminate the worker.



### Debugging the Source Connector


To debug the source connector, use the following command to run a Connect standalone worker with your locally-built S3 source connector:

```
$ DEBUG=y SUSPEND=y ./bin/run-source-connector.sh
```

This configures the JVM for remote debugging, suspends the JVM (allowing you to connect with your IDE to port 5005),
sets up Log4J to use the `bin/connect-log4j.properties` in this directory and then
runs a Connect standalone worker with the `bin/connect-avro-standalone.properties` worker configuration
and the `config/S3SourceConnector.properties` connector configuration file.

Use CTRL-C to terminate the worker.

