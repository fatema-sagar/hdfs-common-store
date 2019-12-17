/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source.integration;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

@Category(IntegrationTest.class)
public class HDSourceConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(HDSourceConnectorIT.class);

  private static final String CONNECTOR_NAME = "hdfs-source-connector";

  private static final int NUMBER_OF_PARTITIONS = 1;
  private static final String KAFKA_TOPIC = "destination";

  @Before
  public void setup() throws IOException {
    startConnect();
    //TODO: Start proxy or external system
  }

  @After
  public void close() {
    //TODO: Stop the proxy or external system

    stopConnect();
  }

  @Test
  public void testAvroDataWithDefaultPartitioner() throws Exception {
    // TODO: Set up a proxy or use an endpoint

    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    // setup up props for the source connector
    Map<String, String> props = connectorConfiguration();
    props.put("format.class", AVRO_FORMAT_CLASS);
    //TODO: put connector-specific properties

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // consume all records from the source topic or fail, to ensure that they were correctly
    // produced.
    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
            15,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC
    );
  }
}
