/*
 * Copyright [2019 - 2019] Confluent Inc.
 */


package io.confluent.connect.azure.blob.storage;

import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.azure.blob.storage.format.avro.AvroFormat;

public class AzureBlobStorageSourceConnectorTestBase {

  static final String ZERO_PAD_FMT = "%010d";
  private static final String IGNORE_FOR_CONVENIENCE_CLASS = "DUMMY_URL";
  private static final String BASE64_KEY = "bXlfa2V5";
  private static final String AZURE_TEST_BUCKET_NAME = "kafka-bucket";
  protected static final String AZURE_TEST_ACCOUNT_NAME = "myaccount";
  protected static final String AZURE_TEST_FOLDER_TOPIC_NAME = "topics/test-topic";
  protected static final Time SYSTEM_TIME = new SystemTime();
  protected static final String AVRO_FORMAT_CLASS = "io.confluent.connect.azure.blob.storage.format.avro.AvroFormat";
  protected static final String DEFAULT_PARTITIONER_CLASS = "io.confluent.connect.storage.partitioner.DefaultPartitioner";

  private static final Logger log = LoggerFactory
      .getLogger(AzureBlobStorageSourceConnectorTestBase.class);
  @Rule
  public TestRule watcher = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      log.info(
          "Starting test: {}.{}",
          description.getTestClass().getSimpleName(),
          description.getMethodName()
      );
    }
  };

  protected MockAzureBlobStorage storage;
  protected AzureBlobStorageSourceConnectorConfig connectorConfig;
  String topicsDir;
  //Map<String, Object> properties;
  protected Map<String, String> properties;

  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<String, String>();
    props.put("storage.class", MockAzureBlobStorage.class.getName());
    props.put("azblob.container.name", AZURE_TEST_BUCKET_NAME);
    props.put("format.class", AvroFormat.class.getName());
    props.put("flush.size", "3");
    props.put("azblob.account.key", BASE64_KEY);
    props.put("store.url", "");
    props.put("confluent.topic.bootstrap.servers", "localhost:9092");
    props.put("confluent.topic.replication.factor", "1");
    props.put("azblob.account.name", AZURE_TEST_ACCOUNT_NAME);
    props.put("folders", AZURE_TEST_FOLDER_TOPIC_NAME);
    props.put("format.class", AVRO_FORMAT_CLASS);
    props.put("partitioner.class", DEFAULT_PARTITIONER_CLASS);
    return props;
  }

  public void setUp() throws Exception {
    this.properties = this.createProps();
    connectorConfig = new AzureBlobStorageSourceConnectorConfig(properties);
  }
  
  public void tearDown() throws Exception {
    //MockAzureBlobStorage.clear();
  }
  
}

