/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import io.confluent.connect.gcs.GcsSourceConnector;
import io.confluent.connect.gcs.source.GcsSourceTask;
import io.confluent.connect.utils.licensing.ConnectLicenseManager;
import io.confluent.connect.utils.licensing.LicenseConfigUtil;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig.STORE_URL_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.storage.Storage;


public class GcsSourceConnectorTest extends GcsTestUtils{

  private Map<String, String> settings;
  private GcsSourceConnector connector;
  private ConnectLicenseManager licenseMgr;
  private GcsSourceConnectorConfig config;
  private GcsSourceStorage storage;

  @Before
  public void before() throws IOException {
    settings = getCommonConfig();
    settings.put("folders", GCS_TEST_FOLDER_TOPIC_NAME);
    settings.put("format.class", AVRO_FORMAT_CLASS);
    settings.put(LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    config = new GcsSourceConnectorConfig(settings);
    // Use a mocked license manager by default
    licenseMgr = mock(ConnectLicenseManager.class);

    Storage gcs = uploadAvroDataToMockGcsBucket( "partition=0", "1");
    storage = new GcsSourceStorage(config.getString(STORE_URL_CONFIG), BUCKECT_NAME, gcs);

    connector = new GcsSourceConnector(storage, licenseMgr);
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
  public void shouldReturnSourceTask() {
    assertEquals(GcsSourceTask.class, connector.taskClass());
  }

  @Test
  public void shouldGenerateValidTaskConfigs() {
    startConnector();
    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
    assertTrue("zero task configs provided", !taskConfigs.isEmpty());
    for (Map<String, String> taskConfig : taskConfigs) {
      assertEquals(settings, taskConfig);
    }
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
   * Start the connector, via the {@link GcsSourceConnector#doStart()} method that uses our mock
   * license manager.
   */
  protected void startConnector() {
    final ConnectorContext mockContext = mock(ConnectorContext.class);
    connector.initialize(mockContext);
    connector.setConfig(settings);
    connector.doStart();
  }
}