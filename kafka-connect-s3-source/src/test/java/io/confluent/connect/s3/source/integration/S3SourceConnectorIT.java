/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source.integration;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.connect.s3.source.S3AvroTestUtils.writeAvroFile;
import static io.confluent.connect.s3.source.S3ByteArrayTestUtils.writeByteArrayFile;
import static io.confluent.connect.s3.source.S3JsonTestUtils.writeJsonFile;

import static java.lang.String.format;

@Category(IntegrationTest.class)
public class S3SourceConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(S3SourceConnectorIT.class);

  private static final String CONNECTOR_NAME = "s3-source-conn";

  private static final String KAFKA_TOPIC = "destination";
  private static final int NUMBER_OF_PARTITIONS = 3;

  @Before
  public void setup() throws Exception {
    startConnect();

    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
        .withPathStyleAccessEnabled(true)
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(
                "http://localhost:" + S3_MOCK_RULE.getHttpPort(), ""));
    s3 = builder.build();
  }

  @After
  public void close() throws Exception {
    s3.deleteBucket(S3_BUCKET);
    stopConnect();
  }
  
  @Test
  public void testAvroDataWithDefaultPartitionFileProcessing() throws Exception {
    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);
    
    createS3BucketAndUploadFile("avro", "DefaultPartitioner");
    
    Map<String, String> props = new HashMap<>();
    props.put("format.class", AVRO_FORMAT_CLASS);

    connect.configureConnector(CONNECTOR_NAME, connectorConfiguration(props));

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

  @Test
  public void testBasicByteArrayDefaultPartitionFileProcessing() throws Exception {
    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    createS3BucketAndUploadFile("bin", "DefaultPartitioner");
    
    Map<String, String> props = new HashMap<>();
    props.put("format.class", BYTE_ARRAY_FORMAT_CLASS);

    connect.configureConnector(CONNECTOR_NAME, connectorConfiguration(props));

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // consume all records from the source topic or fail, to ensure that they were correctly
    // produced.
    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        60,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC
    );
    
    uploadMoreDataToS3("bin");
    
    /**
     * This sleep is added so that more data are uploaded and partition checker 
     * gets time to update no. of partitions and reconfigure task config.
     */
    Thread.sleep(1000*5);
    
    log.info("Waiting for extra records in destination topic ...");
    ConsumerRecords<byte[], byte[]> moreRecords = connect.kafka().consume(
        100,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC
    );
  }

  @Test
  public void testJsonDataWithDefaultPartitionFileProcessing() throws Exception {
    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);
    
    createS3BucketAndUploadFile("json", "DefaultPartitioner");
    
    Map<String, String> props = new HashMap<>();
    props.put("format.class", JSON_FORMAT_CLASS);
    connect.configureConnector(CONNECTOR_NAME, connectorConfiguration(props));

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
    
    records.forEach(r -> log.info(new String(r.value())));
  }
  
  @Test
  public void testAvroDataWithTimeBasedPartitioner() throws Exception {
    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    createS3BucketAndUploadFile("avro", "TimeBasedPartitioner");

    Map<String, String> props = new HashMap<>();
    props.put("format.class", AVRO_FORMAT_CLASS);
    props.put("partitioner.class", "io.confluent.connect.storage.partitioner.TimeBasedPartitioner");
    props.put("path.format", "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH");
    props.put("partition.duration.ms", "600000");
    props.put("locale","en_US");
    props.put("timezone","UTC");
    props.put("timestamp.extractor", "Record");

    connect.configureConnector(CONNECTOR_NAME, connectorConfiguration(props));

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

  private void createS3BucketAndUploadFile(final String fileExtension, final String partition) throws IOException {
    s3.createBucket(S3_BUCKET);

    final String filePrefix = "topics/" + KAFKA_TOPIC;
    
    Path s3File1 =null;
    Path s3File2 = null;
    Path s3File3 = null;
    
    if(fileExtension.equalsIgnoreCase("avro")) {
     // add files
      s3File1 = writeAvroFile(5);
      s3File2 = writeAvroFile(5);
      s3File3 = writeAvroFile(5);
    } else if(fileExtension.equalsIgnoreCase("bin")) {
      s3File1 = writeByteArrayFile(25, 0);
      s3File2 = writeByteArrayFile(25, 0);
      s3File3 = writeByteArrayFile(10, 0);
    } else if(fileExtension.equalsIgnoreCase("json")) {
      s3File1 = writeJsonFile(25);
      s3File2 = writeJsonFile(25);
      s3File3 = writeJsonFile(10);
    }
    
    if(partition.equalsIgnoreCase("DefaultPartitioner")) {
      uploadFileToS3WithDefaultPartition(fileExtension, filePrefix, s3File1, s3File2, s3File3);
    } else if (partition.equalsIgnoreCase("TimeBasedPartitioner")) {
      uploadFileToS3WithTimeBasedPartition(fileExtension, filePrefix, s3File1, s3File2, s3File3);
    }

  }

  private void uploadFileToS3WithDefaultPartition(final String fileExtension, final String filePrefix, Path s3File1, Path s3File2,
      Path s3File3) {
    final String s3ObjectKey1 =
        format("%s/partition=0/" + KAFKA_TOPIC + "+0+0000000000." + fileExtension, filePrefix);
    final String s3ObjectKey2 =
        format("%s/partition=1/" + KAFKA_TOPIC + "+1+0000000000." + fileExtension, filePrefix);
    final String s3ObjectKey3 =
        format("%s/partition=1/" + KAFKA_TOPIC + "+1+0000000025." + fileExtension, filePrefix);

    s3.putObject(S3_BUCKET, s3ObjectKey1, s3File1.toFile());
    s3.putObject(S3_BUCKET, s3ObjectKey2, s3File2.toFile());
    s3.putObject(S3_BUCKET, s3ObjectKey3, s3File3.toFile());
  }

  private void uploadFileToS3WithTimeBasedPartition(final String fileExtension, final String filePrefix, Path s3File1, Path s3File2,
      Path s3File3) {

    final String s3ObjectKey1 =
        format("%s/year=2019/month=09/day=05/hour=13/" + KAFKA_TOPIC + "+0+0000000000." + fileExtension, filePrefix);
    final String s3ObjectKey2 =
        format("%s/year=2019/month=10/day=05/hour=13/" + KAFKA_TOPIC + "+1+0000000001." + fileExtension, filePrefix);
    final String s3ObjectKey3 =
        format("%s/year=2019/month=11/day=05/hour=13/" + KAFKA_TOPIC + "+2+0000000002." + fileExtension, filePrefix);

    s3.putObject(S3_BUCKET, s3ObjectKey1, s3File1.toFile());
    s3.putObject(S3_BUCKET, s3ObjectKey2, s3File2.toFile());
    s3.putObject(S3_BUCKET, s3ObjectKey3, s3File3.toFile());
  }

  @SuppressWarnings("deprecation")
  private void uploadMoreDataToS3(final String fileExtension) throws IOException {
    final String filePrefix = "topics/" + KAFKA_TOPIC;
    
    Path s3File3 = writeByteArrayFile(100, 60); // int startingIndex
    final String s3ObjectKey3 =
        format("%s/partition=2/" + KAFKA_TOPIC + "+2+0000000060." + fileExtension, filePrefix);

    s3.putObject(S3_BUCKET, s3ObjectKey3, s3File3.toFile());
    log.info("Sent more records to S3");
  }

}
