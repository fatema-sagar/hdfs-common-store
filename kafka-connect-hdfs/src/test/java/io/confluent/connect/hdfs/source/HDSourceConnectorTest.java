/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.hdfs.source.HDSourceConnector;
import io.confluent.connect.hdfs.source.HDSourceConnectorConfig;
import io.confluent.connect.hdfs.source.HDSourceTask;
import io.confluent.connect.utils.licensing.ConnectLicenseManager;
import io.confluent.connect.utils.licensing.LicenseConfigUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HDSourceConnectorTest {

  Map<String, String> settings;
  HDSourceConnector connector;
  ConnectLicenseManager licenseMgr;

  @Before
  public void before() {
    settings = new HashMap<>();
    connector = new HDSourceConnector();
    // Use a mocked license manager by default
    licenseMgr = mock(ConnectLicenseManager.class);
    connector.licenseManager = licenseMgr;

    settings.put(HDSourceConnectorConfig.MY_SETTING_CONFIG, "local://localhost:2876");
    settings.put(LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
  }

  @Test
  public void shouldReturnNonNullVersion() {
    assertNotNull(connector.version());
  }

  @Test
  public void shouldStartWithoutError() {
    startConnector();
    verify(connector.licenseManager, times(1)).registerOrValidateLicense();
  }

  @Test
  public void shouldReturnSourceTask() {
    assertEquals(HDSourceTask.class, connector.taskClass());
  }

  @Test
  public void shouldGenerateValidTaskConfigs() {
    startConnector();
    //TODO: Change this logic to reflect expected behavior of your connector
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

//  @Test
//  public void version() {
//    assertNotNull(connector.version());
//    assertFalse(connector.version().equals("0.0.0.0"));
//    assertTrue(connector.version().matches("^(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)(-\\w+)?$"));
//  }

  /**
   * Start the connector, via the {@link HDSourceConnector#doStart()} method that uses our mock
   * license manager.
   */
  protected void startConnector() {
    connector.config = new HDSourceConnectorConfig(settings);
    connector.doStart();
  }
}