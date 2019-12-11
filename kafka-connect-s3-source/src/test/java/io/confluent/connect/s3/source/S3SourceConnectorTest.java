/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.utils.licensing.ConnectLicenseManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class S3SourceConnectorTest extends TestWithMockedS3 {

  private S3SourceConnector connector;
  private ConnectLicenseManager licenseMgr;
  
  private static final String POLL_INTERVAL_MS_CONFIG = "s3.poll.interval.ms";

  @Before
  public void setup() throws Exception {
    super.setUp();
    // Use a mocked license manager by default
    licenseMgr = mock(ConnectLicenseManager.class);
    connector = new S3SourceConnector(storage, licenseMgr);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    connector.stop();
  }

  @Test
  public void shouldReturnNonNullVersion() {
    assertNotNull(connector.version());
  }

  @Test
  public void shouldStartWithoutError() {
    startConnector();
    verify(licenseMgr, times(1)).registerOrValidateLicense();
  }

  @Test
  public void shouldHaveAnEmptyConstructorAndReturnConfig() {
    ConfigDef configDef = new S3SourceConnector().config();
    assertNotNull(configDef);
  }

  @Test
  public void shouldReturnSourceTask() {
    assertEquals(S3SourceTask.class, connector.taskClass());
  }

  @Test
  public void shouldGenerateValidTaskConfigs() {
    s3.putObject(S3_TEST_BUCKET_NAME, "topics/test_the_config/partition=0/file?", "");
    startConnector();
    Map<String, String> clonedProperties = new HashMap<>(properties);
    clonedProperties
        .put(S3SourceConnectorConfig.FOLDERS_CONFIG, "topics/test_the_config/partition=0/");

    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
    assertTrue("zero task configs provided", !taskConfigs.isEmpty());
    for (Map<String, String> taskConfig : taskConfigs) {
      assertEquals(clonedProperties, taskConfig);
    }
  }

  @Test
  public void shouldGenerateOnlyNecessaryTasksBaseOnFolders() {
    s3.putObject(S3_TEST_BUCKET_NAME, "topics/test_the_config/partition=0/file?", "");
    startConnector();
    Map<String, String> clonedProperties = new HashMap<>(properties);
    clonedProperties
        .put(S3SourceConnectorConfig.FOLDERS_CONFIG, "topics/test_the_config/partition=0/");

    final int maxTask = 3;
    final int expectedForConnectorConfig = 1;

    List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTask);
    assertTrue("zero task configs provided", !taskConfigs.isEmpty());
    for (Map<String, String> taskConfig : taskConfigs) {
      assertEquals(clonedProperties, taskConfig);
    }
    assertEquals(expectedForConnectorConfig, taskConfigs.size());
  }

  @Test
  public void monitorThreadShouldTriggerAReconfigurationForDefaultPartitioner() {
    properties.put(POLL_INTERVAL_MS_CONFIG, "1000");
    s3.putObject(S3_TEST_BUCKET_NAME, "topics/test_the_config/partition=0/file?", "");
    final ConnectorContext mockContext = mock(ConnectorContext.class);
    connector.initialize(mockContext);
    startConnector();

    s3.putObject(S3_TEST_BUCKET_NAME, "topics/test_the_config/partition=1/file?", "");

    Mockito.verify(mockContext, timeout(5000).atLeastOnce()).requestTaskReconfiguration();
  }

  @Test
  public void monitorThreadShouldNotTriggerAReconfigurationForDefaultPartitioner() {
    properties.put(POLL_INTERVAL_MS_CONFIG, "100");
    s3.putObject(S3_TEST_BUCKET_NAME, "topics/test_the_config/partition=0/file1?", "");
    final ConnectorContext mockContext = mock(ConnectorContext.class);
    connector.initialize(mockContext);
    startConnector();

    s3.putObject(S3_TEST_BUCKET_NAME, "topics/test_the_config/partition=0/file2?", "");

    Mockito.verify(mockContext, after(2000L).never()).requestTaskReconfiguration();
  }

  @Test
  public void monitorThreadShouldTriggerAReconfigurationForTimeBased() throws InterruptedException {
    properties.put(POLL_INTERVAL_MS_CONFIG, "1000");
    properties.put("partitioner.class", DailyPartitioner.class.getName());
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        "topics/test_the_config/year=2017/month=09/day=29/file.avro",
        ""
    );
    final ConnectorContext mockContext = mock(ConnectorContext.class);
    connector.initialize(mockContext);
    startConnector();

    s3.putObject(
        S3_TEST_BUCKET_NAME,
        "topics/test_the_config/year=2018/month=09/day=29/file.avro",
        ""
    );

    Mockito.verify(mockContext, timeout(5000).atLeastOnce()).requestTaskReconfiguration();
  }

  @Test
  public void monitorThreadShouldNotTriggerAReconfigurationForTimeBased() {
    properties.put(POLL_INTERVAL_MS_CONFIG, "100");
    properties.put("partitioner.class", DailyPartitioner.class.getName());
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        "topics/test_the_config/year=2017/month=09/day=29/file1.avro",
        ""
    );
    final ConnectorContext mockContext = mock(ConnectorContext.class);
    connector.initialize(mockContext);
    startConnector();

    s3.putObject(
        S3_TEST_BUCKET_NAME,
        "topics/test_the_config/year=2017/month=09/day=29/file2.avro",
        ""
    );

    Mockito.verify(mockContext, after(2000L).never()).requestTaskReconfiguration();
  }

  @Test
  public void monitorThreadShouldTriggerAReconfigurationForFieldBased() {
    properties.put(POLL_INTERVAL_MS_CONFIG, "1000");
    properties.put("partitioner.class", FieldPartitioner.class.getName());
    properties.put("partition.field.name", "parition");
    s3.putObject(S3_TEST_BUCKET_NAME, "topics/test_the_config/partition=0/file?", "");
    final ConnectorContext mockContext = mock(ConnectorContext.class);
    connector.initialize(mockContext);
    startConnector();

    s3.putObject(S3_TEST_BUCKET_NAME, "topics/test_the_config/partition=1/file?", "");

    Mockito.verify(mockContext, timeout(5000).atLeastOnce()).requestTaskReconfiguration();
  }

  @Test
  public void monitorThreadShouldNotTriggerAReconfigurationForFieldBased() {
    properties.put(POLL_INTERVAL_MS_CONFIG, "100");
    properties.put("partitioner.class", FieldPartitioner.class.getName());
    properties.put("partition.field.name", "parition");
    s3.putObject(S3_TEST_BUCKET_NAME, "topics/test_the_config/partition=0/file1?", "");
    final ConnectorContext mockContext = mock(ConnectorContext.class);
    connector.initialize(mockContext);
    startConnector();

    s3.putObject(S3_TEST_BUCKET_NAME, "topics/test_the_config/partition=0/file2?", "");

    Mockito.verify(mockContext, after(2000L).never()).requestTaskReconfiguration();
  }

  @Test
  public void shouldStartAndStop() {
    startConnector();
    connector.stop();
  }

  @Test
  public void shouldNotHaveNullConfigDef() {
    // ConfigDef objects don't have an overridden equals() method; just make sure it's non-null
    assertNotNull(connector.config());
  }

  @Test
  public void version() {
    assertNotNull(connector.version());
    assertFalse(connector.version().equals("0.0.0.0"));
    assertTrue(connector.version().matches("^(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)(-\\w+)?$"));
  }

  /**
   * Start the connector, via the {@link S3SourceConnector#doStart()} method that uses our mock
   * license manager.
   */
  protected void startConnector() {
    connector.setConfig(properties);
    connector.doStart();
  }
}
