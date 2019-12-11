/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage.integration;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlockBlobClient;
import com.azure.storage.blob.ContainerClient;
import com.azure.storage.common.credentials.SASTokenCredential;
import com.azure.storage.common.credentials.SharedKeyCredential;

/**
 * To run the integration tests, you will have to update these values with Azure Account credentials
 * 1. AZURE_ACCOUNT_NAME in BaseConnectorIT file
 * 2. AZURE_ACCOUNT_KEY in BaseConnectorIT file
 * 
 * These credentials are used to create Azure Blob container and upload files during IT tests.
 * 
 *
 */
@Category(IntegrationTest.class)
public class AzureBlobStorageSourceConnectorIT extends BaseConnectorIT {

  private static final Logger log = 
      LoggerFactory.getLogger(AzureBlobStorageSourceConnectorIT.class);

  private static final String CONNECTOR_NAME = "azure-blob-source-connect";
  private static String KAFKA_TOPIC = "abs_topic";
  private static final int NUM_RECORDS_PRODUCED = 15;
  private static final int NUMBER_OF_PARTITIONS = 3;
  
  private ContainerClient containerClient;

  @Before
  public void setup() throws IOException {
    startConnect();
  }

  @After
  public void close() {
    /**
     * Delete the created container for testing
     */
    deleteContainerCreatedForTesting();
    
    stopConnect();
  }

  @Test
  public void testWithAvroDataSource() throws Exception {
    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    Map<String, String> props = getCommonConfiguration();
    props.put("format.class", "io.confluent.connect.azure.blob.storage.format.avro.AvroFormat");
    props.put("azblob.container.name", "blob-test-avro");
    
    /**
     * Create Azure Storage Client, container and upload test data.
     * 
     */
    createStorageClient(props);
    createABSContainerAndUploadFile("avro", "DefaultPartitioner");
    
    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = TASKS_MAX; // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    // consume all records from the source topic or fail, to ensure that they were
    // correctly
    // produced.
    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka()
        .consume(2000, CONSUME_MAX_DURATION_MS,
            KAFKA_TOPIC);
    assertEquals(2000, records.count());
    AtomicInteger counter = new AtomicInteger(0);
    StringBuilder recordsString = new StringBuilder();
    records.forEach(r -> {
      recordsString.append(new String(r.value()));
    });
    records.forEach(r -> {
      assertTrue(recordsString.toString().contains("age=" + counter.getAndAdd(1)));
    });
    

  }
  
  @Test
  public void testWithAvroDataSourceWithTimeBasedPartitioner() throws Exception {
    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    Map<String, String> props = getCommonConfiguration();
    props.put("format.class", "io.confluent.connect.azure.blob.storage.format.avro.AvroFormat");
    props.put("azblob.container.name", "blob-test-time");
    props.put("partitioner.class", "io.confluent.connect.storage.partitioner.TimeBasedPartitioner");
    
    props.put("path.format", "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH");
    props.put("partition.duration.ms", "600000");
    props.put("locale","en_US");
    props.put("timezone","UTC");
    props.put("timestamp.extractor", "Record");
    
    /**
     * Create Azure Storage Client, container and upload test data.
     * 
     */
    createStorageClient(props);
    createABSContainerAndUploadFile("avro", "TimeBasedPartitioner");
    
    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = TASKS_MAX; // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    // consume all records from the source topic or fail, to ensure that they were
    // correctly
    // produced.
    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka()
        .consume(2000, CONSUME_MAX_DURATION_MS,
            KAFKA_TOPIC);
    assertEquals(2000, records.count());
    AtomicInteger counter = new AtomicInteger(0);
    StringBuilder recordsString = new StringBuilder();
    records.forEach(r -> {
      recordsString.append(new String(r.value()));
    });
    records.forEach(r -> {
      assertTrue(recordsString.toString().contains("age=" + counter.getAndAdd(1)));
    });
   }

  @Test
  public void testWithAvroDataSourceWithDailyBasedPartitioner() throws Exception {
    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    Map<String, String> props = getCommonConfiguration();
    props.put("format.class", "io.confluent.connect.azure.blob.storage.format.avro.AvroFormat");
    props.put("azblob.container.name", "blob-test-daily");
    props.put("partitioner.class", "io.confluent.connect.storage.partitioner.DailyPartitioner");
    
    props.put("path.format", "'year'=YYYY/'month'=MM/'day'=dd");
    props.put("partition.duration.ms", "86400000");
    props.put("locale","en_US");
    props.put("timezone","UTC");
    props.put("timestamp.extractor", "Record");
    
    /**
     * Create Azure Storage Client, container and upload test data.
     * 
     */
    createStorageClient(props);
    createABSContainerAndUploadFile("avro", "DailyPartitioner");
    
    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = TASKS_MAX; // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    // consume all records from the source topic or fail, to ensure that they were
    // correctly
    // produced.
    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka()
        .consume(2000, CONSUME_MAX_DURATION_MS,
            KAFKA_TOPIC);
    assertEquals(2000, records.count());
    AtomicInteger counter = new AtomicInteger(0);
    StringBuilder recordsString = new StringBuilder();
    records.forEach(r -> {
      recordsString.append(new String(r.value()));
    });
    records.forEach(r -> {
      assertTrue(recordsString.toString().contains("age=" + counter.getAndAdd(1)));
    });
  }

  
  @Test
  public void testWithJsonDataSource() throws Exception {
    
    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC);

    Map<String, String> props = getCommonConfiguration();
    props.put("format.class", "io.confluent.connect.azure.blob.storage.format.json.JsonFormat");
    props.put("azblob.container.name", "blob-test-json");
    
    /**
     * Create Azure Storage Client, container and upload test data.
     * 
     */
    createStorageClient(props);
    createABSContainerAndUploadFile("json", "DefaultPartitioner");
    
    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = TASKS_MAX; // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    // consume all records from the source topic or fail, to ensure that they were
    // correctly produced.
    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka()
        .consume(2000, CONSUME_MAX_DURATION_MS,
            KAFKA_TOPIC);
    assertEquals(2000, records.count());
    AtomicInteger counter = new AtomicInteger(0);
    StringBuilder recordsString = new StringBuilder();
    records.forEach(r -> {
      recordsString.append(new String(r.value()));
    });
    records.forEach(r -> {
      assertTrue(recordsString.toString().contains("age=" + counter.getAndAdd(1)));
    });
  }
  
  @Test
  public void testWithByteArrayDataSource() throws Exception {
    
    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC);

    Map<String, String> props = getCommonConfiguration();
    props.put("format.class", "io.confluent.connect.azure.blob.storage.format."
        + "bytearray.ByteArrayFormat");
    props.put("azblob.container.name", "blob-test-bytearray");
    
    /**
     * Create Azure Storage Client, container and upload test data.
     * 
     */
    createStorageClient(props);
    createABSContainerAndUploadFile("bin", "DefaultPartitioner");
    
    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = TASKS_MAX; // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    // consume all records from the source topic or fail, to ensure that they were
    // correctly produced.
    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka()
        .consume(2000, CONSUME_MAX_DURATION_MS,
            KAFKA_TOPIC);
    assertEquals(2000, records.count());

  }
  
  @Test
  public void testUsingLiveConsumer() throws Exception {
    
    final String containerName = "blob-live-test-json";
    
    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);
    
    Map<String, String> props = getCommonConfiguration();
    props.put("format.class", "io.confluent.connect.azure.blob.storage.format.json.JsonFormat");
    props.put("azblob.container.name", containerName);

    /**
     * Create Azure Storage Client, container and upload test data.
     * 
     */
    createStorageClient(props);
    createABSContainerAndUploadFile("json", "DefaultPartitioner");
    
    // start a Azure blob Storage source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    //Create a consumer to consume the records.
    KafkaConsumer<byte[], byte[]> consumer =
        connect.kafka().createConsumerAndSubscribeTo(Collections.emptyMap(), KAFKA_TOPIC);

    // Consume records
    ConsumerRecords<byte[], byte[]> records = consumeRecordsFromConsumer(consumer,
        TimeUnit.SECONDS.toMillis(10));

    assertEquals(2000, records.count());
    AtomicInteger counter = new AtomicInteger(0);
    records.forEach(r -> {
      log.info(new String(r.value()));
      assertTrue(new String(r.value()).contains("age=" + counter.getAndAdd(1)));
    });

    // Commit the read records.
    consumer.commitSync();

    // Add more files to azure container.
    uploadMoreDataToAzure(containerName, "json");

    log.info("Waiting for more records in {} topic...", KAFKA_TOPIC);

    // Consume more records.
    records = consumeRecordsFromConsumer(consumer, TimeUnit.SECONDS.toMillis(40));

    // The consumer should have consumed only the new records and none of the older records.
    assertEquals(800, records.count());
    
  }

  @Test
  public void testConsumerAbleToReadNewRecordsOnceConnectorIsRestarted() throws Exception {

    final String containerName = "blob-test-json-restart";

    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC, NUMBER_OF_PARTITIONS);

    Map<String, String> props = getCommonConfiguration();
    props.put("format.class", "io.confluent.connect.azure.blob.storage.format.json.JsonFormat");
    props.put("azblob.container.name", containerName);
    
    /**
     * Create Azure Storage Client, container and upload test data.
     * 
     */
    createStorageClient(props);
    createABSContainerAndUploadFile("json", "DefaultPartitioner");

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

    assertEquals(2000, records.count());
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
    uploadMoreDataToAzure(containerName, "json");

    // Reconfigure and start connector.
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Consume more records.
    records = consumeRecordsFromConsumer(consumer, TimeUnit.SECONDS.toMillis(10));

    // The consumer should have consumed only the new records and none of the older records.
    assertEquals(800, records.count());
    
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
  
  public void createStorageClient(Map<String, String> props) throws InterruptedException {
    /*
     * Create a BlobServiceClient object
     * 
     */
    BlobServiceClient storageClient = null;
    
    /*
     * From the Azure portal, get your Storage account name for creating blob service URL endpoint.
     * The URL typically looks like this:
     */
    String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", props.get("azblob.account.name"));
    
    if (props.get("azblob.sas.token") != null && !"".equals(props.get("azblob.sas.token"))) {
      /*
       * From the Azure portal, get your Storage account's shared access signature (SAS) to 
       * create a credential object. This is used to access your account.
       */
      final SASTokenCredential credential = SASTokenCredential
          .fromSASTokenString(props.get("azblob.sas.token"));
      /*
       * BlobServiceClient object wraps the service endpoint, 
       * saskeycredential and a request pipeline.
       */
      storageClient = new BlobServiceClientBuilder()
          .endpoint(endpoint).credential(credential).buildClient();
    } else {
      /*
       * From the Azure portal, get your Storage account's name and key to create a 
       * credential object. This is used to access your account.
       */
      final SharedKeyCredential credential = new SharedKeyCredential(props.get("azblob.account.name"),
          props.get("azblob.account.key"));
      /*
       * BlobServiceClient object wraps the service endpoint, 
       * sharedkeycredential and a request pipeline.
       */
      storageClient = new BlobServiceClientBuilder()
          .endpoint(endpoint).credential(credential).buildClient();
    }
    /*
     * Create a client that references a to-be-created container in your Azure Storage account. This returns a
     * ContainerClient object that wraps the container's endpoint, credential and a request pipeline (inherited from storageClient).
     * Note that container names require lowercase.
     */
    containerClient = storageClient.getContainerClient(props.get("azblob.container.name"));

    /*
     * Delete if the container already exists.
     */
    boolean deleted =false;
    while(containerClient.exists()) {
      log.info("Deleting the already existing container having same name as given container name..");
      if(!deleted) {
        containerClient.delete();
        deleted = true;
      }
      log.info("Container being deleted please try after it gets deleted.");
    }
    
    /*
     * Create a container in Storage blob account.
     */
    containerClient.create();
  }
  
  private void createABSContainerAndUploadFile(final String fileExtension,
      final String partition) throws IOException, FileNotFoundException {
    
    try {
          
      // Create file with respective dummy data.
      Path fileName1 = null;
      Path fileName2 = null;
      if (fileExtension.equalsIgnoreCase("avro")) {
        fileName1 = AzureBlobStorageAvroTestUtils.writeDataToAvroFile(0, 1000);
        fileName2 = AzureBlobStorageAvroTestUtils.writeDataToAvroFile(1000, 2000);
      } else if (fileExtension.equalsIgnoreCase("json")) { 
        fileName1 = AzureBlobStorageJsonTestUtils.writeJsonFile(0, 1000); 
        fileName2 = AzureBlobStorageJsonTestUtils.writeJsonFile(1000, 2000); 
      } else if(fileExtension.equalsIgnoreCase("bin")) { 
        fileName1 = AzureBlobStorageByteArrayTestUtils.writeByteArrayFile(1000, 0); 
        fileName2 = AzureBlobStorageByteArrayTestUtils.writeByteArrayFile(2000, 1000); 
      }
         
      final String filePrefix = "topics";
      String absObjectKey1 = "";
      String absObjectKey2 = "";
      if(partition.equalsIgnoreCase("DefaultPartitioner")) {
        absObjectKey1 =
            format("%s/abs_topic/partition=0/abs_topic+0+0000000000.%s", filePrefix, fileExtension);
        absObjectKey2 =
            format("%s/abs_topic/partition=0/abs_topic+0+0000000001.%s", filePrefix, fileExtension);
      } else if (partition.equalsIgnoreCase("TimeBasedPartitioner")) {
        absObjectKey1 =
            format("%s/abs_topic/year=2019/month=09/day=05/hour=13/abs_topic+0+0000000000.%s", filePrefix, fileExtension);
        absObjectKey2 =
            format("%s/abs_topic/year=2019/month=09/day=05/hour=13/abs_topic+0+0000000001.%s", filePrefix, fileExtension);
      } else if (partition.equalsIgnoreCase("DailyPartitioner")) {
        absObjectKey1 =
            format("%s/abs_topic/year=2019/month=09/day=05/abs_topic+0+0000000000.%s", filePrefix, fileExtension);
        absObjectKey2 =
            format("%s/abs_topic/year=2019/month=10/day=05/abs_topic+0+0000000000.%s", filePrefix, fileExtension);
      }
      
      /*
       * Create a client that references a to-be-created blob in your Azure Storage account's container.
       * This returns a BlockBlobClient object that wraps the blob's endpoint, credential and a request pipeline
       * (inherited from containerClient). Note that blob names can be mixed case.
       */
      BlockBlobClient blobClient1 = containerClient.getBlobClient(absObjectKey1).asBlockBlobClient();
      BlockBlobClient blobClient2 = containerClient.getBlobClient(absObjectKey2).asBlockBlobClient();
  
      /*
       * Upload the large file to storage blob.
       */
      blobClient1.uploadFromFile(fileName1.toString());
      blobClient2.uploadFromFile(fileName2.toString());
      
    } catch (Exception e) {
      log.error("Exception while uploading file.");
    }
  }
  
  private void uploadMoreDataToAzure(final String bucketName, final String fileExtension) throws IOException {
    Path fileName3 = AzureBlobStorageJsonTestUtils.writeJsonFile(1500, 2000);
    Path fileName4 = AzureBlobStorageJsonTestUtils.writeJsonFile(2000, 2300);
    final String absObjectKey3 =
        format("%s/abs_topic/partition=0/abs_topic+0+0000000030.%s", "topics", fileExtension);
    final String absObjectKey4 =
        format("%s/abs_topic/partition=1/abs_topic+1+0000000045.%s", "topics", fileExtension);
    /*
     * Create a client that references a to-be-created blob in your Azure Storage account's container.
     * This returns a BlockBlobClient object that wraps the blob's endpoint, credential and a request pipeline
     * (inherited from containerClient). Note that blob names can be mixed case.
     */
    BlockBlobClient blobClient3 = containerClient.getBlobClient(absObjectKey3).asBlockBlobClient();
    BlockBlobClient blobClient4 = containerClient.getBlobClient(absObjectKey4).asBlockBlobClient();

    /*
     * Upload the large file to storage blob.
     */
    blobClient3.uploadFromFile(fileName3.toString());
    blobClient4.uploadFromFile(fileName4.toString());
  }

  
  /**
   * Delete the created container for testing
   */
  public void deleteContainerCreatedForTesting() {
    containerClient.delete();
  }

}
