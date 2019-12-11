# Quickstart

This quickstart will show how to setup kafka-connect-gcs-source connector that reads GCS blob object from Google Cloud Storage to Kafka topic.

## Preliminary Setup

Navigate to your Confluent Platform installation directory and run this command to
install the latest connector version.

```
confluent-hub install confluentinc/kafka-connect-gcs-source:latest
```

You can install a specific version by replacing latest with a version number of a _released_ connector version. For example:

```
confluent-hub install confluentinc/kafka-connect-gcs-source:1.0.0-preview
```

Adding a new connector plugin requires restarting Connect. Use the Confluent CLI to restart Connect:

```
$ confluent stop connect && confluent start connect
Starting zookeeper
zookeeper is [UP]
Starting kafka
kafka is [UP]
Starting schema-registry
schema-registry is [UP]
Starting kafka-rest
kafka-rest is [UP]
Starting connect
connect is [UP]
```

Check if the kafka-connect-gcs-source connector plugin has been installed correctly and picked up by the plugin loader:

```
$ curl -sS localhost:8083/connector-plugins | jq .[].class | grep kafka-connect-gcs-source
"io.confluent.connect.gcs.GcsSourceConnector"
```


## Source Connector

The Google Cloud Storage source connector is used to read from Google Cloud Storage, and write records into a Kafka topic. To start a connector which communicates with an anonymous broker, use the following connector config:

```
{
    "name": "gcs-source-connector",
    "config": {
        "tasks.max":"1"
        "connector.class":"io.confluent.connect.gcs.GcsSourceConnector"
        "gcs.credentials.path":"path-where-your-gcs-credentials-file"
        "gcs.bucket.name":"bucket-from-where-need-to-read"
        "format.clas":"io.confluent.connect.gcs.format.avro.AvroFormat"
        "confluent.topic.bootstrap.servers":"localhost:9092"
        "confluent.topic.replication.factor":"1"
    }
}
```

The important configs used here are:

* **tasks.max**: The maximum number of tasks that should be created for this connector. The connector may create fewer tasks if it cannot achieve this level of parallelism.
* **connector.class**: The Java class for the connector.
* **gcs.credentials.path**: Path for the file where your gcs credentials are present.
* **gcs.bucket.name**: The bucket name from where data need to read.
* **format.clas**: Class responsible for converting Azure Blob Storage objects to source records.


Save this config in a file (kafka-connect-gcs-source.json, for example), and run the command to start the connector:

```
curl -X POST -d @kafka-connect-gcs-source.json http://localhost:8083/connectors -H "Content-Type: application/json"
```

## Advanced Debugging


### Trace Logging

Both the sink and source connectors have trace logs which show in greater detail what records are passing through them. To enable them, add the following lines to your log4j.properties and restart the connect worker (*important*: we have to restart the worker otherwise these changes will not be picked up):

```
log4j.logger.io.confluent.connect.gcs.source.GcsSourceTask=TRACE
```

For Confluent packages, the default log4j file resides at `/etc/kafka/connect-log4j.properties`, and adding the above lines makes the file look as follows:

```
log4j.rootLogger=INFO, stdout


log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c:%L)%n

log4j.logger.org.apache.zookeeper=ERROR
log4j.logger.org.I0Itec.zkclient=ERROR
log4j.logger.org.reflections=ERROR
log4j.logger.org.eclipse.jetty=ERROR

log4j.logger.io.confluent.connect.gcs=TRACE
```
