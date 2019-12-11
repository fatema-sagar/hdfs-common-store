/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source.integration;

import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG;
import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.eclipse.jetty.server.Server;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(45);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
  protected static final int TASKS_MAX = 1;
  
  // TODO: Update the GCP credentials in file gcpcreds.json to run the integration tests.
  protected final String PATH_TO_CREDENTIALS = this.getClass().getClassLoader()
                                                    .getResource("gcpcreds.json")
                                                    .getPath();

  protected EmbeddedConnectCluster connect;
  
  //Uncomment below code to use schema registry.
  protected Server registryServer;

  @ClassRule
  public static TemporaryFolder gcsMockRoot = new TemporaryFolder();

  protected void startConnect() throws IOException, Exception {
    connect = new EmbeddedConnectCluster.Builder()
        .name("my-connect-cluster")
        .build();

    // start the clusters
    connect.start();

    // To run schema registry.
    Properties props = new Properties();
    props.setProperty("kafkastore.connection.url", connect.kafka().zKConnectString());
    props.setProperty("listeners", "http://0.0.0.0:" + findAvailableOpenPort());

    SchemaRegistryRestApplication schemaRegistry = new SchemaRegistryRestApplication(
        new SchemaRegistryConfig(props));
    registryServer = schemaRegistry.createServer();
    registryServer.start();

    TestUtils.waitForCondition(() -> registryServer.isRunning(), 10000L, "Registry server did not start in time");
  }

  protected void stopConnect() {
    // stop all Connect, Kafka and Zk threads.
    connect.stop();
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
   * name to start the specified number of tasks.
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
          && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.warn("Could not check connector state info : " + e.getMessage());
      return Optional.empty();
    }
    
  }
  
  /**
   * Get common configurations to run Gcs source =connector.
   * @return
   */
  protected Map<String, String> getCommonConfig() {
    
    // setup up props for the source connector
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, "io.confluent.connect.gcs.GcsSourceConnector");
    props.put("gcs.credentials.path", PATH_TO_CREDENTIALS);
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
    
    // converters and formatters
    props.put("format.class", "io.confluent.connect.gcs.format.avro.AvroFormat");
    
    props.put(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    props.put(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.put("record.batch.max.size", "5");
    props.put("gcs.poll.interval.ms", String.valueOf(500));
    
    return props;
  }
 
  /**
   * This is a helper method to create GCS bucket and delete existing bucket if any.
   * @param storage  :Storage object
   * @param gcsBucketName : bucket name
   */
  protected static void createNewGcsBucket(Storage storage, String gcsBucketName) {

    // Delete GCS bucket if exists.
    if (storage.get(gcsBucketName, Storage.BucketGetOption.fields()) != null) {
      Iterable<Blob> blobs = storage.list(gcsBucketName, 
          Storage.BlobListOption.prefix("")).iterateAll();
      for (Blob blob : blobs) {
        blob.delete(Blob.BlobSourceOption.generationMatch());
      }
      log.trace("Deleting existing bucket - {}", gcsBucketName);
      storage.delete(gcsBucketName);
    }

    storage.create(BucketInfo.newBuilder(gcsBucketName)
        .setStorageClass(StorageClass.STANDARD)
        .setLocation("us-east1")
        .build());
  }

  private Integer findAvailableOpenPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException("Port for GCS Source connect test can't be allocated", e);
    }
  }

}
