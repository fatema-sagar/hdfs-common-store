/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage.integration;

import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG;
import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(100);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(500);
  protected static final int TASKS_MAX = 1;
  
  //Update the following integration endpoints to run the live integration test.
  //Never commit the credentials. Update and run the test case only for test purposes.
  /*
   * mention the Account Shared Access Signature for accessing the Azure Account
   * else use the azure account name and key configs
   */
  public final String AZ_ACCOUNT_SAS_TOKEN = "enter_azure_account_sas_token";

  //Mention the azure blob storage account name here
  private final String AZURE_ACCOUNT_NAME = "Azure Account Name";
  //use the below config if you want to access the azure storage using account key
  private final String AZURE_ACCOUNT_KEY = "Azure Acccount Key";

  protected EmbeddedConnectCluster connect;

  protected void startConnect() throws IOException {
    connect = new EmbeddedConnectCluster.Builder()
        .name("my-connect-cluster")
        .build();

    // start the clusters
    connect.start();

    //TODO: Start proxy or external system
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
      // log.error("Could not check connector state info.", e);
      return Optional.empty();
    }
  }
  
  protected Map<String, String> getCommonConfiguration() {
    // setup up props for the source connector
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, 
        "io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnector");
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
    
    // license properties
    props.put(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    props.put(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG, "1");

    props.put("record.batch.max.size", "3");
    
    //for accessing azure account through sas
    //props.put("azblob.sas.token", AZ_ACCOUNT_SAS_TOKEN);
    //enter the azure account name
    props.put("azblob.account.name", AZURE_ACCOUNT_NAME);
    //uncomment for accessing azure account through azure account key 
    props.put("azblob.account.key", AZURE_ACCOUNT_KEY);
   
    props.put("partitioner.class", 
        "io.confluent.connect.storage.partitioner.DefaultPartitioner");
    props.put("azblob.poll.interval.ms", "1000");

    return props;
  }
  
}
