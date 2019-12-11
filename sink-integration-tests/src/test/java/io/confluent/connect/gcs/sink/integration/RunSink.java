/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.sink.integration;

import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG;
import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.junit.Assert.assertEquals;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.common.BaseConnector;
import io.confluent.connect.common.Employee;

/**
 * Class used to run SINK connectors.
 * This is a generic class which can be re-used across all cloud connectors for running
 * integration tests.
 *
 * It implements AutoCloseable, which shutsdown the connector and the cluster for each object.
 */
public class RunSink implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RunSink.class);

    private BaseConnector baseConnector;
    private String kafkaTopic;
    private String pathToCredentials;
    private String bucketName;
    private int numOfRecordsToGenerate;
    private Map<String, String> propOverrides;

    /**
     * Instantiates a new Run sink.
     *
     * @param kafkaTopic             the kafka topic
     * @param pathToCredentials      the path to credentials
     * @param numOfPartitions        the num of partitions
     * @param bucketName             the bucket name
     * @param numOfRecordsToGenerate the num of records to generate
     * @param propOverrides          the prop overrides
     * @throws Exception the exception
     */
    public RunSink(String kafkaTopic, String pathToCredentials,
                   int numOfPartitions, String bucketName, int numOfRecordsToGenerate,
                   Map<String, String> propOverrides) throws Exception {
        this.kafkaTopic = kafkaTopic;
        this.pathToCredentials = pathToCredentials;
        this.bucketName = bucketName;
        this.numOfRecordsToGenerate = numOfRecordsToGenerate;
        this.propOverrides = propOverrides;
        this.baseConnector = new BaseConnector(this.kafkaTopic, numOfPartitions);
    }

    /**
     * Start executing the connector.
     *
     * @throws Exception the exception
     */
    public void start() throws Exception {
        // Run the sink
        configureAndRun();
    }

    @Override
    public void close() throws Exception {
        baseConnector.shutdown();
    }

    private void configureAndRun() throws Exception {
        Map<String, String> connectorProperties = getConfigForSink(baseConnector);
        connectorProperties.putAll(propOverrides);

        log.info("Configuring connector..");
        baseConnector.configureConnector(this.bucketName + "-" + this.kafkaTopic,
        connectorProperties);

        List<Employee> employees = Employee.generateEmployees(numOfRecordsToGenerate);
        baseConnector.pushRecordsToKafka(employees, kafkaTopic);

        // Need to sleep the thread as the connector takes time to push/write the records to the
        // cloud storage. When we read/consume from the cluster, we are reading from the kafka
        // and it is noticed that there is some amount of time taken to write the records. If we
        // do not allow the connector to write all the records, the source tests would fails as
        // there would be a mis-match of records since the sink shutsdown before writing all the
        // records to the storage.
        log.info("Sleeping for a minite to allow the sink to process..");
        Thread.sleep(120000);
        log.info("Continue with verifying the records ...");

        ConsumerRecords<byte[], byte[]> employeeRecords = baseConnector.getCluster().kafka()
            .consume(numOfRecordsToGenerate, Duration.ofSeconds(130).toMillis(), kafkaTopic);
        log.info("Number of records added in kafka {}", employeeRecords.count());

        assertEquals(numOfRecordsToGenerate, employeeRecords.count());
    }

    private Map<String, String> getConfigForSink(BaseConnector base) {

        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, "io.confluent.connect.gcs.GcsSinkConnector");
        props.put(TASKS_MAX_CONFIG, Integer.toString(1));

        props.put("gcs.part.size", "5242880");
        props.put("flush.size", "100");
        props.put("gcs.credentials.path", pathToCredentials);

        props.put("storage.class", "io.confluent.connect.gcs.storage.GcsStorage");
        props.put("format.class", "io.confluent.connect.gcs.format.json.JsonFormat");
        props.put("partitioner.class", "io.confluent.connect.storage.partitioner.DefaultPartitioner");

        props.put(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, base.getCluster().kafka().bootstrapServers());
        props.put(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG, "1");

        props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("key.converter.schemas.enable", "true");
        props.put("value.converter.schemas.enable", "true");

        props.put("gcs.bucket.name", bucketName);
        props.put("topics", kafkaTopic);

        return props;
    }
}
