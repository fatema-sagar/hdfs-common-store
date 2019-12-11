# Kafka Cloud Storage Source Common

Shared software modules among Cloud Storage Source connector specific [Kafka Connectors](http://kafka.apache.org/documentation.html#connect).

# Development

To build a development version you'll need a recent version of Kafka. You can build
*kafka-connect-cloud-storage-source-parent* with Maven using the standard lifecycle phases.

Code which is common across all of the Azure storage connectors should be here including shared dependencies.
 
- formatters: 
	- Avro
	- Json
	- ByteArray
   
- Storage Object: This interface have common methods needed to process storage objects retrieved from cloud storage.
- CloudSourceStorage: This interface which have methods needed to read data/object from a cloud storage.
- AbstractCloudStorageSourceConnector - This abstract class have common logic related to storage source connector.
- AbstractCloudStorageSourceTask - This abstract class have common logic related to storage source task.
- CloudStorageSourceConnectorCommonConfig - This abstract class have common configurations related to Cloud source storage connector.

There are other various utility classes added which are common across all cloud storage source connector.   
   
# Documentation

Documentation on the connector is hosted on Confluent's
[docs site](https://docs.confluent.io/current/connect/kafka-connect-s3-source/).

Source code is located in Confluent's
[docs repo](https://github.com/confluentinc/docs/tree/master/connect/kafka-connect-s3-source). If changes
are made to configuration options for the connector, be sure to generate the RST docs (as described
below) and open a PR against the docs repo to publish those changes!

# Contribute

- Source Code: https://github.com/confluentinc/kafka-connect-s3-source
- Issue Tracker: https://github.com/confluentinc/kafka-connect-s3-source/issues
