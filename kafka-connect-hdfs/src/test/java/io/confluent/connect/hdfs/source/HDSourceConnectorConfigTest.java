/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.hdfs.source.HDSourceConnectorConfig;
import io.confluent.connect.utils.licensing.LicenseConfigUtil;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HDSourceConnectorConfigTest {

  Map<String, String> settings;
  HDSourceConnectorConfig config;

  @Before
  public void before() {
    settings = new HashMap<>();
    settings.put(HDSourceConnectorConfig.MY_SETTING_CONFIG, "local://localhost:2876");
    settings.put(LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config = null;
  }

  @Test
  public void shouldAcceptValidConfig() {
    settings.put(HDSourceConnectorConfig.PORT_CONFIG, "10");
    config = new HDSourceConnectorConfig(settings);
    assertNotNull(config);
  }

  @Test
  public void shouldUseDefaults() {
    config = new HDSourceConnectorConfig(settings);
    assertEquals(HDSourceConnectorConfig.PORT_DEFAULT, config.port());
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidPort() {
    settings.put(HDSourceConnectorConfig.PORT_CONFIG, "-10");
    new HDSourceConnectorConfig(settings);
  }

  //TODO: Add more tests
}