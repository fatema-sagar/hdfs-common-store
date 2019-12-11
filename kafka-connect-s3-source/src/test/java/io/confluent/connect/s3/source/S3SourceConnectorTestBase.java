/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class S3SourceConnectorTestBase {
  private static final Logger log = LoggerFactory.getLogger(S3SourceConnectorTestBase.class);

  protected static final int S3_TEST_PORT = 8181;
  protected static final String S3_TEST_URL = "http://127.0.0.1:" + S3_TEST_PORT;
  protected static final String S3_TEST_BUCKET_NAME = "test-bucket";
  protected static final String S3_TEST_FOLDER_TOPIC_NAME = "topics/test-topic";
  
  protected static final String AVRO_FORMAT_CLASS = "io.confluent.connect.s3.format.avro.AvroFormat";
  protected static final String DEFAULT_PARTITIONER_CLASS = "io.confluent.connect.storage.partitioner.DefaultPartitioner";

  protected S3SourceConnectorConfig connectorConfig;

  protected static final String url = S3_TEST_URL;
  protected Map<String, String> properties;

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

  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();
    props.put("s3.bucket.name", S3_TEST_BUCKET_NAME);

    props.put("confluent.topic.bootstrap.servers", "localhost:9092");
    props.put("confluent.topic.replication.factor", "1");

    props.put("folders", S3_TEST_FOLDER_TOPIC_NAME);
    props.put("record.batch.max.size", "1");
    props.put("format.class", AVRO_FORMAT_CLASS);
    props.put("partitioner.class", DEFAULT_PARTITIONER_CLASS);

    return props;
  }

  public void setUp() throws Exception {
    this.properties = this.createProps();
    connectorConfig = new S3SourceConnectorConfig(properties);
  }

  public void tearDown() throws Exception {}

  public AmazonS3 newS3Client(S3SourceConnectorConfig config) {
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
        .withAccelerateModeEnabled(config.getBoolean(S3SourceConnectorConfig.WAN_MODE_CONFIG))
        .withPathStyleAccessEnabled(true)
        .withCredentials(new DefaultAWSCredentialsProviderChain());

    builder = url == null ?
              builder.withRegion(config.getString(S3SourceConnectorConfig.REGION_CONFIG)) :
              builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                  url,
                  ""
              ));

    return builder.build();
  }
}
