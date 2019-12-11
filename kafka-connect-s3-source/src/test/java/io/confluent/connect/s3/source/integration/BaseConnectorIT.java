/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source.integration;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.amazonaws.services.s3.AmazonS3;

import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG;
import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(75);

  protected static final int TASKS_MAX = 1;
  protected static final String AVRO_FORMAT_CLASS = "io.confluent.connect.s3.format.avro.AvroFormat";
  protected static final String BYTE_ARRAY_FORMAT_CLASS = "io.confluent.connect.s3.format.bytearray.ByteArrayFormat";
  protected static final String JSON_FORMAT_CLASS = "io.confluent.connect.s3.format.json.JsonFormat";
  protected static final String S3_BUCKET = "mytestbucket";

  protected EmbeddedConnectCluster connect;

  @ClassRule
  public static TemporaryFolder s3mockRoot = new TemporaryFolder();

  @ClassRule
  public static S3MockRule S3_MOCK_RULE;

  protected static AmazonS3 s3;

  static {
    try {
      s3mockRoot.create();
      File s3mockDir = s3mockRoot.newFolder("s3-tests-" + UUID.randomUUID().toString());
      log.info("Create folder: " + s3mockDir.getCanonicalPath());
      S3_MOCK_RULE = S3MockRule.builder()
          .withRootFolder(s3mockDir.getAbsolutePath()).silent().build();
    } catch (IOException e) {
      log.error("Erorr while running S3 mock. {}", e);
    }
  }

  protected void startConnect() throws Exception {
    connect = new EmbeddedConnectCluster.Builder()
        .name("my-connect-cluster")
        .build();
    connect.start();
  }

  protected void stopConnect() throws Exception {
    // stop all Connect, Kafka and Zk threads.
    connect.stop();
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the
   * given name to start the specified number of tasks.
   *
   * @param name the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @return the time this method discovered the connector has started, in milliseconds past epoch
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time."
    );
    return System.currentTimeMillis();
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks the expected number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
          && info.tasks().size() == numTasks
          && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
          && info.tasks()
          .stream()
          .allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.warn("Could not check connector state info.", e.getMessage());
      return Optional.empty();
    }
  }

  protected Map<String, String> connectorConfiguration(Map<String, String> overwrite) {
    // setup up props for the source connector
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, "io.confluent.connect.s3.source.S3SourceConnector");
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
    props.put("s3.bucket.name", S3_BUCKET);
    props.put("format.class", BYTE_ARRAY_FORMAT_CLASS);
    props.put("partitioner.class", "io.confluent.connect.storage.partitioner.DefaultPartitioner");
    props.put("store.url", "http://localhost:" + S3_MOCK_RULE.getHttpPort());
    props.put("s3.poll.interval.ms", String.valueOf(1000));
    
    // license properties
    props.put(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    props.put(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG, "1");

    props.putAll(overwrite);

    return props;
  }

}
