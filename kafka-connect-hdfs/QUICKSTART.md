# Quickstart

This quickstart will show how to setup the HDFS Source Connector Connector against a Dockerized HDFS Source Connector.


## Preliminary Setup

This repo provides sample docker compose scripts to bring up HDFS Source Connector located in `src/test/docker/`.
Check these files before continuing, and make sure they point to a valid Docker image for HDFS Source Connector.


To start HDFS Source Connector in the "configA" mode, use the docker-compose
command as follows:

```
cd src/test/docker/configA
docker-compose up
```

This should start a docker container running HDFS Source Connector, with the following logs:
```
$ docker-compose up
Starting ...
```

Running `docker ps` will show you the exposed ports, which should look something like the following:
```
$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                                              NAMES
26cc6d47efe3        replace-with-image-name   "/docker-entrypoint.…"   4 hours ago         Up 23 seconds       0.0.0.0:32777->1883/tcp, 0.0.0.0:32776->9001/tcp   anonymous_my_1
```

Navigate to your Confluent Platform installation directory and run this command to
install the latest connector version.

```
confluent-hub install confluentinc/kafka-connect-hdfs:latest
```

You can install a specific version by replacing latest with a version number of a _released_ connector version. For example:

```
confluent-hub install confluentinc/kafka-connect-hdfs:0.1.0
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

Check if the kafka-connect-hdfs connector plugin has been installed correctly and picked up by the plugin loader:

```
$ curl -sS localhost:8083/connector-plugins | jq .[].class | grep My
"io.confluent.MySinkConnector"
"io.confluent.connect.hdfs.source.HDSourceConnector"
```


## Source Connector

The HDFS Source Connector source connector is used to read from HDFS Source Connector, and write records into a Kafka topic. To start a connector which communicates with an anonymous broker, use the following connector config:

```
{
    "name": "my-source-connector",
    "config": {
        "connector.class": "io.confluent.connect.hdfs.source.HDSourceConnector",
        "tasks.max": "1",
        "some.setting": "some value"
    }
}
```

The important configs used here are:

* **tasks.max**: The maximum number of tasks that should be created for this connector. The connector may create fewer tasks if it cannot achieve this level of parallelism.
* **some.setting**: TODO

TODO: For examples on how to setup licensing, look at the connect-plugins-common/connect-licensing-extensions [QUICKSTART](https://github.com/confluentinc/connect-plugins-common/blob/master/connect-licensing-extensions/QUICKSTART.md), and add the required properties to this config.

Save this config in a file (kafka-connect-hdfs.json, for example), and run the command to start the connector:

```
curl -X POST -d @kafka-connect-hdfs.json http://localhost:8083/connectors -H "Content-Type: application/json"
```


## Sink Connector

The HDFS Source Connector sink connector is used to publish records from a Kafka topic into HDFS Source Connector. Here is a sample configuration:

```
{
    "name": "my-sink-connector",
    "config": {
        "connector.class": "io.confluent.MySinkConnector",
        "tasks.max": "1",
        "topics":"kafka-topic-name",
        "some.setting": "some value"
    }
}
```

The property names are very similar to the source connector above, with the following difference:

* **topics**: a list of Kafka topics to consume records from.

TODO: For examples on how to setup licensing, look at the connect-plugins-common/connect-licensing-extensions [QUICKSTART](https://github.com/confluentinc/connect-plugins-common/blob/master/connect-licensing-extensions/QUICKSTART.md), and add the required properties to this config.

Save this config in a file (kafka-connect-hdfs.json, for example), and run the command to start the connector:

```
curl -X POST -d @kafka-connect-hdfs.json http://localhost:8083/connectors -H "Content-Type: application/json"
```


## Advanced Debugging


### Trace Logging

Both the sink and source connectors have trace logs which show in greater detail what records are passing through them. To enable them, add the following lines to your log4j.properties and restart the connect worker (*important*: we have to restart the worker otherwise these changes will not be picked up):

```
log4j.logger.io.confluent.connect.hdfs.source.HDSourceTask=TRACE
log4j.logger.io.confluent.MySinkTask=TRACE
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


## Troubleshooting

TODO