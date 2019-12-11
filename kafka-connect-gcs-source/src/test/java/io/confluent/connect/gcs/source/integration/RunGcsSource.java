/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source.integration;

import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.storage.Storage;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Class used to start the cluster and start the connectors.
 */
public class RunGcsSource extends BaseConnectorIT implements AutoCloseable{

    private static final Logger log = LoggerFactory.getLogger(RunGcsSource.class);

    private String pathToCredentials;
    private String kafkaTopic;
    private int numOfPartitions;
    private String bucketName;
    private int numberOfRecords;
    private Map<String, String> propsToOverride;
    private Storage gcs;

  /**
   * Instantiates a new Run gcs source.
   *
   * @param storage           the storage
   * @param pathToCredentials the path to credentials
   * @param kafkaTopic        the kafka topic
   * @param numOfPartitions   the num of partitions
   * @param bucketName        the bucket name
   * @param numberOfRecords   the number of records
   * @param propsToOverride   the props to override
   * @throws Exception the exception
   */
  public RunGcsSource(Storage storage, String pathToCredentials, String kafkaTopic,
                        int numOfPartitions,
                        String bucketName, int numberOfRecords, Map<String, String> propsToOverride) throws Exception {
        this.pathToCredentials = pathToCredentials;
        this.kafkaTopic = kafkaTopic;
        this.numberOfRecords = numberOfRecords;
        this.numOfPartitions = numOfPartitions;
        this.propsToOverride = propsToOverride;
        this.gcs = storage;

        startConnect();
        // create topic in Kafka
        connect.kafka().createTopic(this.kafkaTopic, this.numOfPartitions);
    }

  /**
   * Start the connectors based on the configured properties.
   *
   * @throws Exception the exception
   */
  public void start() throws Exception {

    // connector properties
    Map<String, String> props = getCommonConfig();
    props.put("flush.size", "100");
    props.put(TASKS_MAX_CONFIG, "1");
    props.putAll(propsToOverride);

    // start a GCS source connector
    connect.configureConnector(kafkaTopic, props);

    // wait for tasks to spin up
    waitForConnectorToStart(kafkaTopic, TASKS_MAX);

    // consume all records from the source topic or fail, to ensure that they were correctly
    // produced.
    log.info("Waiting for records in {} topic...", kafkaTopic);
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(numberOfRecords,
        TimeUnit.SECONDS.toMillis(180),
        kafkaTopic);

    assertEquals(numberOfRecords, records.count());

    StringBuilder builder = new StringBuilder();
    records.forEach(r -> {
      builder.append(new String(r.value()));
    });
    String finalString = builder.toString();
    if(!props.get("format.class").contains("ByteArrayFormat")) {
      for (int empId = 0; empId < numberOfRecords; empId++) {
        assertTrue(finalString.contains(Integer.toString(empId)));
      }
    } else {
      log.info("Skipping verification of records for byte arrays.");
    }
  }

  @Override
  public void close() throws Exception {
    stopConnect();
  }
}
