/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source.integration;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static io.confluent.connect.hdfs.source.HDAvroTestUtils.writeAvroFile;
import static io.confluent.connect.hdfs.source.HDJsonTestUtils.writeJsonFile;
import static java.lang.String.format;

@Category(IntegrationTest.class)
public class HDSourceConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(HDSourceConnectorIT.class);

  private static final String CONNECTOR_NAME = "hdfs-source-connect";

  private static final int NUMBER_OF_PARTITIONS = 1;
  private static final String KAFKA_TOPIC = "avro_it_test";

  @Before
  public void setup() throws IOException {
    startConnect();
  }

  @After
  public void close() {
    stopConnect();
  }

  @Test
  public void testAvroDataWithDefaultPartitioner() throws Exception {
    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    createHDTopicAndUploadFile("avro", "DefaultPartitioner");

    // setup up props for the source connector
    Map<String, String> props = connectorConfiguration();
    props.put("format.class", AVRO_FORMAT_CLASS);

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // consume all records from the source topic or fail, to ensure that they were correctly
    // produced.
    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
            9,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC
    );
    Assert.assertEquals(records.count(), 9);
  }

  private void createHDTopicAndUploadFile(final String fileExtension, final String partitioner) throws IOException {
    final String filePrefix = "topics/" + KAFKA_TOPIC;

    Path hdFile1 = null;
    Path hdFile2 = null;
    Path hdFile3 = null;

    if (fileExtension.equalsIgnoreCase("avro")) {
      // add files
      hdFile1 = writeAvroFile(5);
      hdFile2 = writeAvroFile(5);
      hdFile3 = writeAvroFile(5);
    } else if (fileExtension.equalsIgnoreCase("json")) {
      hdFile1 = writeJsonFile(5);
      hdFile2 = writeJsonFile(5);
      hdFile3 = writeJsonFile(5);
    }

    uploadFileToHDWithDefaultPartition(fileExtension, filePrefix, hdFile1, hdFile2, hdFile3);

  }

  private void uploadFileToHDWithDefaultPartition(String fileExtension, String filePrefix,
        Path hdFile1, Path hdFile2, Path hdFile3) {
    final String hdKey1 = format("%s/partition=0/"+ KAFKA_TOPIC +"+0+0000000000." + fileExtension, filePrefix);
    final String hdKey2 = format("%s/partition=1/"+ KAFKA_TOPIC +"+0+0000000000." + fileExtension, filePrefix);
    final String hdKey3 = format("%s/partition=2/"+ KAFKA_TOPIC +"+0+0000000000." + fileExtension, filePrefix);
  }

  @Test
  public void testJsonDataWithDefaultPartitioner() throws Exception{

  }
}
