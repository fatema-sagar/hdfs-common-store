/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.common;

import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.eclipse.jetty.server.Server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;

/**
 * The Base connector to start the kafka cluster and also start the provided connectors specified
 * in the configuration/properties..
 */
public class BaseConnector {

  private EmbeddedConnectCluster cluster;

  private Server registryServer;

  private String connectorName;

  /**
   * Instantiates a new Base connector.
   *
   * @param kafkaTopic     the kafka topic
   * @param partitionsSize the partitions size
   * @throws Exception the exception
   */
  public BaseConnector(String kafkaTopic, int partitionsSize) throws Exception {
    cluster = new EmbeddedConnectCluster.Builder()
        .name("my-connect-cluster")
        .build();

    // start the clusters
    cluster.start();

    // To run schema registry.
    Properties props = new Properties();
    props.setProperty("kafkastore.connection.url", cluster.kafka().zKConnectString());
    props.setProperty("listeners", "http://0.0.0.0:" + findAvailableOpenPort());

    SchemaRegistryRestApplication schemaRegistry = new SchemaRegistryRestApplication(
        new SchemaRegistryConfig(props));
    registryServer = schemaRegistry.createServer();
    registryServer.start();

    TestUtils.waitForCondition(() -> registryServer.isRunning(), 60000L, "Registry server did not" +
        " start in time");

    cluster.kafka().createTopic(kafkaTopic, partitionsSize);
  }

  /**
   * Shutdown the connectors and kafka clusters.
   */
  public void shutdown() throws IOException {
    cluster.deleteConnector(connectorName);
    // stop all Connect, Kafka and Zk threads.
    cluster.stop();
  }

  /**
   * Gets the kafka cluster.
   *
   * @return the connect
   */
  public EmbeddedConnectCluster getCluster() {
    return cluster;
  }

  /**
   * Gets registry server.
   *
   * @return the registry server
   */
  public Server getRegistryServer() {
    return registryServer;
  }

  /**
   * Configure connector to the kafka cluster.
   *
   * @param conectorName   the conector name
   * @param connectorProps the connector props
   * @throws IOException the io exception
   */
  public void configureConnector(String conectorName,
                                                Map<String, String> connectorProps) throws IOException{
    this.connectorName = conectorName;
    cluster.configureConnector(conectorName, connectorProps);
  }

  /**
   * Push records of Employee class to kafka cluster.
   *
   * @param employees  the employees
   * @param kafkaTopic the kafka topic
   * @throws InterruptedException the interrupted exception
   */
  public void pushRecordsToKafka(List<Employee> employees, String kafkaTopic) throws InterruptedException {
    // Send records to Kafka
    for (Employee employee : employees) {
      String kafkaKey = null;
      String kafkaValue = employee.asJsonWithSchema();
      cluster.kafka().produce(kafkaTopic, kafkaValue);
    }
  }

  private Integer findAvailableOpenPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException("Port for GCS Source connect test can't be allocated", e);
    }
  }
}
