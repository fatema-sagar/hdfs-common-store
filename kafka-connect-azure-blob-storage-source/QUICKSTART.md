# Quickstart

This quickstart will show how to setup kafka-connect-azure-blob-storage-source that reads blob objects from Azure Blob Storage to Kafka topic.

## Preliminary Setup

Navigate to your Confluent Platform installation directory and run this command to
install the latest connector version.

```
# run from your Confluent Platform installation directory
confluent-hub install confluentinc/kafka-connect-azure-blob-storage-source:latest
```

You can install a specific version by replacing latest with a version number of a _released_ connector version. For example:

```
confluent-hub install confluentinc/kafka-connect-azure-blob-storage-source:1.0.0-preview
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

Check if the kafka-connect-azure-blob-storage-source connector plugin has been installed correctly and picked up by the  plugin loader:

```
$ curl -sS localhost:8083/connector-plugins | jq .[].class | grep kafka-connect-azure-blob-storage-source
"io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnector"
```
## Source Connector

The Azure Blob Storage source connector is used to read from Azure Blob Storage, and write records into a Kafka topic. To start a connector which communicates with an anonymous broker, use the following connector config:

```
{
    "name": "azure-bolb-storage-source",
    "config": {
        "tasks.max":"1",
        "connector.class":"io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnector",
        "azblob.account.name":"your-account-name",
        "azblob.account.key":"your-key",
        "azblob.container.name":"container-from-where-need-to-read",
        "format.clas":"io.confluent.connect.azure.blob.storage.format.avro.AvroFormat",
        "confluent.topic.bootstrap.servers":"localhost:9092",
        "confluent.topic.replication.factor":"1"
    }
}
```

The important configs used here are:

* **tasks.max**: The maximum number of tasks that should be created for this connector. The connector may create fewer tasks if it cannot achieve this level of parallelism.
* **connector.class**: The Java class for the connector.
* **azblob.account.name**: Name of the Azure Account, required Azure credentials. Must be between 3-23 alphanumeric characters.
* **azblob.account.key**: Key of the Azure Account, required Azure credentials.
* **azblob.container.name**: The container name from where data need to read. Must be between 3-63 alphanumeric and '-' characters.
* **format.clas**: Class responsible for converting Azure Blob Storage objects to source records.

Save this config in a file (kafka-connect-azure-blob-storage-source.json, for example), and run the command to start the connector:

```
curl -X POST -d @kafka-connect-azure-blob-storage-source.json http://localhost:8083/connectors -H "Content-Type: application/json"
```

## Advanced Debugging


### Trace Logging

Source connectors have trace logs which show in greater detail what records are passing through them. To enable them, add the following lines to your log4j.properties and restart the connect worker (*important*: we have to restart the worker otherwise these changes will not be picked up):

```
log4j.logger.io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceTask=TRACE
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

log4j.logger.io.confluent=TRACE
```
