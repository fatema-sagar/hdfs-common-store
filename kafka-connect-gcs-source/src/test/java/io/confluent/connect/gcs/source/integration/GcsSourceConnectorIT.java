/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source.integration;

import static io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig.STORE_URL_CONFIG;
import static java.lang.String.format;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.StorageObject;
import io.confluent.connect.gcs.format.json.JsonFormat;
import io.confluent.connect.cloud.storage.source.util.ValueOffset;
import io.confluent.connect.gcs.source.GcsAvroTestUtils;
import io.confluent.connect.gcs.source.GcsByteArrayTestUtils;
import io.confluent.connect.gcs.source.GcsJsonTestUtils;
import io.confluent.connect.gcs.source.GcsSourceConnectorConfig;
import io.confluent.connect.gcs.source.GcsSourceStorage;
/**
 * To run the integration tests, you will have to update these files with GCP credentials
 * 1. src/test/resources/gcpcreds.properties
 * 2. src/test/resources/gcpcreds.json 
 * 
 * These credentials are used to create GCS bucket and upload files during IT tests.
 * 
 * Remove @Ignore for running IT tests.
 *
 */
@Category(IntegrationTest.class)
public class GcsSourceConnectorIT extends BaseConnectorIT {
  
  private static final Logger log = LoggerFactory.getLogger(GcsSourceConnectorIT.class);

  private static final String AVRO_FORMAT_CLASS = "io.confluent.connect.gcs.format.avro.AvroFormat";
  private static final String JSON_FORMAT_CLASS = "io.confluent.connect.gcs.format.json.JsonFormat";
  private static final String BYTE_ARRAY_FORMAT_CLASS = 
      "io.confluent.connect.gcs.format.bytearray.ByteArrayFormat";

  private static final String FIELD_PARTITIONER = "io.confluent.connect.storage.partitioner.FieldPartitioner";
  private static final String TIME_BASED_PARTITIONER = 
      "io.confluent.connect.storage.partitioner.TimeBasedPartitioner";

  private static final String CONNECTOR_NAME = "GCSSourceconnector";
  private static final String KAFKA_TOPIC = "gcs_topic";
  private static final int NUMBER_OF_PARTITIONS = 3;
  private final int Records_200 = 200;
  private final int Records_800 = 800;
  private final int Records_1600 = 1600;
  private final int Records_2000 = 2000;
  private final int Records_2300 = 2300;

  protected Storage gcs;
  private final String bucketName = "gcs-source";

  @Before
  public void setup() throws Exception {
    startConnect();

    // Create connection to GCS.
    StorageOptions.Builder builder = StorageOptions.newBuilder();
    GoogleCredentials credentials = GoogleCredentials
        .fromStream(new FileInputStream(PATH_TO_CREDENTIALS))
        .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

    builder.setCredentials(credentials);
    gcs = builder.build().getService();
  }

  @After
  public void close() {
    stopConnect();
  }

  @Test
  public void testWithAvroDataSource() throws Exception {

    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    /**
     * Create GCS bucket and upload test data. 
     * Delete already existing bucket with same name
     */
    createGcsBucketAndUploadFile(bucketName, "avro");

    // connector properties
    Map<String, String> props = getCommonConfig();
    props.put("gcs.bucket.name", bucketName);
    props.put("format.class", AVRO_FORMAT_CLASS);

    props.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    props.put("value.converter.schema.registry.url", registryServer.getURI().toString());
    props.put("key.converter", "io.confluent.connect.avro.AvroConverter");
    props.put("key.converter.schema.registry.url", registryServer.getURI().toString());

    // start a GCS source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up    
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // consume all records from the source topic or fail, to ensure that they were correctly
    // produced.
    log.info("Waiting for records in {} topic...", KAFKA_TOPIC);
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(1600,
        CONSUME_MAX_DURATION_MS, KAFKA_TOPIC);

    assertEquals(Records_1600, records.count());
  }

  @Test
  public void testWithJsonDataSource() throws Exception {

    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    /**
     * Create GCS bucket and upload test data. 
     * Delete already existing bucket with same name
     */
    createGcsBucketAndUploadFile(bucketName, "json");

    // connector properties
    Map<String, String> props = getCommonConfig();
    props.put("gcs.bucket.name", bucketName);
    props.put("format.class", JSON_FORMAT_CLASS);

    // start a GCS source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up    
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // consume all records from the source topic or fail, to ensure that they were correctly
    // produced.
    log.info("Waiting for records in {} topic...", KAFKA_TOPIC);
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(Records_1600, CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);

    assertEquals(Records_1600, records.count());
    AtomicInteger counter = new AtomicInteger(0);
    records.forEach(r -> {
      log.info(new String(r.value()));
      assertTrue(new String(r.value()).contains("age=" + counter.getAndAdd(1)));
    });

    // Add more files to GCS bucket.
    uploadMoreDataToGCS(bucketName, "json");

    log.info("Waiting for more records in {} topic...", KAFKA_TOPIC);
    records = connect.kafka().consume(Records_2300, CONSUME_MAX_DURATION_MS, KAFKA_TOPIC);
    assertEquals(Records_2300, records.count());
  }

  @Test
  public void testWithByteArrayDataSource() throws Exception {

    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    /**
     * Create GCS bucket and upload test data. 
     * Delete already existing bucket with same name
     */
    createGcsBucketAndUploadFile(bucketName, "bin");

    // connector properties
    Map<String, String> props = getCommonConfig();
    props.put("gcs.bucket.name", bucketName);
    props.put("format.class", BYTE_ARRAY_FORMAT_CLASS);

    // start a GCS source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up.
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // consume all records from the source topic or fail, to ensure that they were correctly
    // produced.
    log.info("Waiting for records in {} topic...", KAFKA_TOPIC);
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(Records_1600,
        CONSUME_MAX_DURATION_MS, KAFKA_TOPIC);

  }

  @Test
  public void testWithAvroDataSourceWithFieldPartitioner() throws Exception {

    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    /**
     * Create GCS bucket and upload test data. 
     * Delete already existing bucket with same name
     */
    uploadDataToGCSWithFieldPartitioner(bucketName, "avro");

    // connector properties
    Map<String, String> props = getCommonConfig();
    props.put("gcs.bucket.name", bucketName);
    props.put("format.class", AVRO_FORMAT_CLASS);
    props.put("partitioner.class", FIELD_PARTITIONER);
    props.put("partition.field.name", "firstname,age");

    props.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    props.put("value.converter.schema.registry.url", registryServer.getURI().toString());
    props.put("key.converter", "io.confluent.connect.avro.AvroConverter");
    props.put("key.converter.schema.registry.url", registryServer.getURI().toString());

    // start a GCS source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // consume all records from the source topic or fail, to ensure that they were correctly
    // produced.
    log.info("Waiting for records in {} topic...", KAFKA_TOPIC);
    ConsumerRecords<byte[], byte[]> records =
        connect.kafka().consume(1500, CONSUME_MAX_DURATION_MS, KAFKA_TOPIC);
  }

  @Test
  public void testWithAvroDataSourceWithTimeBasedPartitioner() throws Exception {

    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    /**
     * Create GCS bucket and upload test data. 
     * Delete already existing bucket with same name
     */
    uploadDataToGCSWithTimeBasedPartitioner(bucketName, "avro");

    // connector properties
    Map<String, String> props = getCommonConfig();
    props.put("gcs.bucket.name", bucketName);
    props.put("format.class", AVRO_FORMAT_CLASS);
    props.put("partitioner.class", TIME_BASED_PARTITIONER);
    props.put("path.format", "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH");
    props.put("partition.duration.ms", "600000");
    props.put("locale", "en_US");
    props.put("timezone", "UTC");
    props.put("timestamp.extractor", "Record");

    props.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    props.put("value.converter.schema.registry.url", registryServer.getURI().toString());
    props.put("key.converter", "io.confluent.connect.avro.AvroConverter");
    props.put("key.converter.schema.registry.url", registryServer.getURI().toString());

    // start a GCS source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // consume all records from the source topic or fail, to ensure that they were correctly
    // produced.
    log.info("Waiting for records in {} topic...", KAFKA_TOPIC);
    connect.kafka().consume(Records_1600, CONSUME_MAX_DURATION_MS, KAFKA_TOPIC);
  }

  /**
   * This test is to check if JSON formatter in case of GSC connect.
   * It does not start connector. it directly reads file from GCS bucket.
   */
  @Test
  public void testJsonFormaterFromGcs() throws IOException {
    createGcsBucketAndUploadFile(bucketName, "json");

    Map<String, String> props = getCommonConfig();
    props.put("format.class", JSON_FORMAT_CLASS);
    props.put("gcs.bucket.name", bucketName);
    props.put("s3.region", "us-east-2");
    props.put(TASKS_MAX_CONFIG, "1");
    props.put("schemas.enable", "false");
    CloudStorageSourceConnectorCommonConfig sourceConfig = new GcsSourceConnectorConfig(props);
    GcsSourceConnectorConfig config = new GcsSourceConnectorConfig(props);
    GcsSourceStorage storage = new GcsSourceStorage(config, config.getString(STORE_URL_CONFIG));

    StorageObject blob = storage.open("topics/gcs_topic/partition=0/gcs_topic+0+0000000000.json");

    JsonFormat format = new JsonFormat(sourceConfig);
    InputStream dataStream = blob.getObjectContent();
    long offset = 0;
    ValueOffset valueOffset;
    List<SchemaAndValue> records = new ArrayList<>();
    do {
      valueOffset = format.extractRecord(dataStream, blob.getContentLength(), offset);
      records.add(valueOffset.getRecord());
      offset = valueOffset.getOffset();
    } while (valueOffset != null && !valueOffset.isEof());

    assertEquals(800, records.size());
    assertTrue(records.get(0).value().toString().contains("age=0"));
    assertTrue(records.get(658).value().toString().contains("age=658"));
    assertTrue(records.get(799).value().toString().contains("age=799"));
  }

  @Test
  public void testUsingLiveConsumer() throws Exception {

    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    /**
     * Create GCS bucket and upload test data.
     * Delete already existing bucket with same name
     */
    createGcsBucketAndUploadFile(bucketName, "json");

    // connector properties
    Map<String, String> props = getCommonConfig();
    props.put("gcs.bucket.name", bucketName);
    props.put("format.class", JSON_FORMAT_CLASS);

    // start a GCS source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    //Create a consumer to consume the records.
    KafkaConsumer<byte[], byte[]> consumer =
        connect.kafka().createConsumerAndSubscribeTo(Collections.emptyMap(), KAFKA_TOPIC);

    // Consume records
    ConsumerRecords<byte[], byte[]> records = consumeRecordsFromConsumer(consumer,
        TimeUnit.SECONDS.toMillis(10));

    assertEquals(Records_1600, records.count());
    AtomicInteger counter = new AtomicInteger(0);
    records.forEach(r -> {
      log.info(new String(r.value()));
      assertTrue(new String(r.value()).contains("age=" + counter.getAndAdd(1)));
    });

    // Commit the read records.
    consumer.commitSync();

    // Add more files to GCS bucket.
    uploadMoreDataToGCS(bucketName, "json");

    log.info("Waiting for more records in {} topic...", KAFKA_TOPIC);

    // Consume more records.
    records = consumeRecordsFromConsumer(consumer, TimeUnit.SECONDS.toMillis(10));

    // The consumer should have consumed only the new records and none of the older records.
    assertEquals(700, records.count());
  }

  @Test
  public void testConsumerAbleToReadNewRecordsOnceConnectorIsRestarted() throws Exception {

    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    /**
     * Create GCS bucket and upload test data.
     * Delete already existing bucket with same name
     */
    createGcsBucketAndUploadFile(bucketName, "json");

    // connector properties
    Map<String, String> props = getCommonConfig();
    props.put("gcs.bucket.name", bucketName);
    props.put("format.class", JSON_FORMAT_CLASS);

    // start a GCS source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    //Create a consumer to consume the records.
    KafkaConsumer<byte[], byte[]> consumer =
        connect.kafka().createConsumerAndSubscribeTo(Collections.emptyMap(), KAFKA_TOPIC);

    // Consume records
    ConsumerRecords<byte[], byte[]> records = consumeRecordsFromConsumer(consumer,
        TimeUnit.SECONDS.toMillis(10));

    assertEquals(Records_1600, records.count());
    AtomicInteger counter = new AtomicInteger(0);
    records.forEach(r -> {
      log.info(new String(r.value()));
      assertTrue(new String(r.value()).contains("age=" + counter.getAndAdd(1)));
    });

    // Commit the read records.
    consumer.commitSync();

    // Stop the connector.
    connect.deleteConnector(CONNECTOR_NAME);

    // Consume more records.
    records = consumeRecordsFromConsumer(consumer, TimeUnit.SECONDS.toMillis(10));

    // No new records should be present since the connector isnt running.
    assertEquals(0, records.count());

    // Add more files to GCS bucket.
    uploadMoreDataToGCS(bucketName, "json");

    // Reconfigure and start connector.
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Consume more records.
    records = consumeRecordsFromConsumer(consumer, TimeUnit.SECONDS.toMillis(10));

    // The consumer should have consumed only the new records and none of the older records.
    assertEquals(700, records.count());
  }

  /**
   * The following has been picked up from embedde cluster to simulate the consume consume records
   * for testing purposes.
   * @param consumer
   * @param consumeMaxDurationMs
   * @return
   */
  private ConsumerRecords<byte[], byte[]> consumeRecordsFromConsumer(
                              KafkaConsumer<byte[], byte[]> consumer, long consumeMaxDurationMs) {
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> consumedRecords = new HashMap();
    //Retry until the given max time.
    while (stopWatch.getTime(TimeUnit.MILLISECONDS) < consumeMaxDurationMs){
      ConsumerRecords<byte[], byte[]> pollRecords = consumer.poll(consumeMaxDurationMs);
      List record;
      for(Iterator iterator = pollRecords.partitions().iterator(); iterator.hasNext();) {
        TopicPartition partition = (TopicPartition)iterator.next();
        record = pollRecords.records(partition);
        ((List)consumedRecords.computeIfAbsent(partition, (t) -> {
          return new ArrayList();
        })).addAll(record);
      }
    }
    return new ConsumerRecords(consumedRecords);
  }

  @SuppressWarnings("deprecation")
  private void createGcsBucketAndUploadFile(final String bucketName, final String fileExtension)
      throws IOException, FileNotFoundException {

    createNewGcsBucket(gcs, bucketName);

    // Create file with respective dummy data.
    Path fileName1 = null;
    Path fileName2 = null;
    if (fileExtension.equalsIgnoreCase("avro")) {
      fileName1 = GcsAvroTestUtils.writeDataToAvroFile(Records_800, 0);
      fileName2 = GcsAvroTestUtils.writeDataToAvroFile(Records_1600, Records_800);
    } else if (fileExtension.equalsIgnoreCase("json")) {
      fileName1 = GcsJsonTestUtils.writeJsonFile(Records_800, 0);
      fileName2 = GcsJsonTestUtils.writeJsonFile(Records_1600, Records_800);
    } else if (fileExtension.equalsIgnoreCase("bin")) {
      fileName1 = GcsByteArrayTestUtils.writeByteArrayFile(Records_800, 0);
      fileName2 = GcsByteArrayTestUtils.writeByteArrayFile(Records_1600, Records_800);
    }

    final String filePrefix = "topics";

    final String gcsObjectKey1 =
        format("%s/gcs_topic/partition=0/gcs_topic+0+0000000000.%s", filePrefix, fileExtension);
    final String gcsObjectKey2 =
        format("%s/gcs_topic/partition=1/gcs_topic+1+0000000015.%s", filePrefix, fileExtension);

    BlobId blobId1 = BlobId.of(bucketName, gcsObjectKey1);
    BlobId blobId2 = BlobId.of(bucketName, gcsObjectKey2);

    BlobInfo blobInfo1 = BlobInfo.newBuilder(blobId1).setContentType("text/plain").build();
    BlobInfo blobInfo2 = BlobInfo.newBuilder(blobId2).setContentType("text/plain").build();

    gcs.create(blobInfo1, new FileInputStream(fileName1.toString()));
    gcs.create(blobInfo2, new FileInputStream(fileName2.toString()));
  }

  @SuppressWarnings("deprecation")
  private void uploadMoreDataToGCS(final String buckerName, final String fileExtension) throws IOException {
    Path fileName3 = GcsJsonTestUtils.writeJsonFile(Records_2000, Records_1600);
    Path fileName4 = GcsJsonTestUtils.writeJsonFile(Records_2300, Records_2000);

    final String gcsObjectKey3 =
        format("%s/gcs_topic/partition=0/gcs_topic+0+0000000030.%s", "topics", fileExtension);
    final String gcsObjectKey4 =
        format("%s/gcs_topic/partition=1/gcs_topic+1+0000000045.%s", "topics", fileExtension);

    BlobInfo blobInfo3 = BlobInfo.newBuilder(BlobId.of(buckerName, gcsObjectKey3))
        .setContentType("text/plain").build();
    BlobInfo blobInfo4 = BlobInfo.newBuilder(BlobId.of(buckerName, gcsObjectKey4))
        .setContentType("text/plain").build();

    gcs.create(blobInfo3, new FileInputStream(fileName3.toString()));
    gcs.create(blobInfo4, new FileInputStream(fileName4.toString()));
  }

  @SuppressWarnings("deprecation")
  private void uploadDataToGCSWithFieldPartitioner(final String buckerName,
      final String fileExtension) throws IOException {

    createNewGcsBucket(gcs, buckerName);

    Path fileName1 = GcsAvroTestUtils.writeDataToAvroFile(100, 0);
    Path fileName2 = GcsAvroTestUtils.writeDataToAvroFile(600, 100);
    Path fileName3 = GcsAvroTestUtils.writeDataToAvroFile(1500, 600);

    final String gcsObjectKey1 =
        format("%s/gcs_topic/firstname=Virat/age=1/gcs_topic+0+0000000001.%s", "topics", fileExtension);
    final String gcsObjectKey2 =
        format("%s/gcs_topic/firstname=Virat/age=2/gcs_topic+1+0000000002.%s", "topics", fileExtension);
    final String gcsObjectKey3 =
        format("%s/gcs_topic/firstname=Virat/age=3/gcs_topic+2+0000000003.%s", "topics", fileExtension);

    BlobInfo blobInfo1 = BlobInfo.newBuilder(BlobId.of(buckerName, gcsObjectKey1))
        .setContentType("text/plain").build();
    BlobInfo blobInfo2 = BlobInfo.newBuilder(BlobId.of(buckerName, gcsObjectKey2))
        .setContentType("text/plain").build();
    BlobInfo blobInfo3 = BlobInfo.newBuilder(BlobId.of(buckerName, gcsObjectKey3))
        .setContentType("text/plain").build();

    gcs.create(blobInfo1, new FileInputStream(fileName1.toString()));
    gcs.create(blobInfo2, new FileInputStream(fileName2.toString()));
    gcs.create(blobInfo3, new FileInputStream(fileName3.toString()));

  }

  @SuppressWarnings("deprecation")
  private void uploadDataToGCSWithTimeBasedPartitioner(final String buckerName,
      final String fileExtension) throws IOException {

    createNewGcsBucket(gcs, buckerName);
    // /topics-4/test_hdfs_x13/month=08/day=28/hour=12/test_hdfs_x13+0+0000000002+0000000002.avro

    Path fileName1 = GcsAvroTestUtils.writeDataToAvroFile(Records_200, 0);
    Path fileName2 = GcsAvroTestUtils.writeDataToAvroFile(Records_800, Records_200);
    Path fileName3 = GcsAvroTestUtils.writeDataToAvroFile(Records_1600, Records_800);

    final String gcsObjectKey1 =
        format("%s/gcs_topic/year=2019/month=08/day=28/hour=10/gcs_topic+0+0000000001.%s", "topics", fileExtension);
    final String gcsObjectKey2 =
        format("%s/gcs_topic/year=2019/month=09/day=28/hour=11/gcs_topic+1+0000000002.%s", "topics", fileExtension);
    final String gcsObjectKey3 =
        format("%s/gcs_topic/year=2019/month=10/day=28/hour=12/gcs_topic+2+0000000003.%s", "topics", fileExtension);

    BlobInfo blobInfo1 = BlobInfo.newBuilder(BlobId.of(buckerName, gcsObjectKey1))
        .setContentType("text/plain").build();
    BlobInfo blobInfo2 = BlobInfo.newBuilder(BlobId.of(buckerName, gcsObjectKey2))
        .setContentType("text/plain").build();
    BlobInfo blobInfo3 = BlobInfo.newBuilder(BlobId.of(buckerName, gcsObjectKey3))
        .setContentType("text/plain").build();

    gcs.create(blobInfo1, new FileInputStream(fileName1.toString()));
    gcs.create(blobInfo2, new FileInputStream(fileName2.toString()));
    gcs.create(blobInfo3, new FileInputStream(fileName3.toString()));

  }

}
