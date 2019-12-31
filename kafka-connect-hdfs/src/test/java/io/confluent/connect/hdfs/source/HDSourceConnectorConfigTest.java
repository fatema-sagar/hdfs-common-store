/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class HDSourceConnectorConfigTest {

  private static final String AVRO_FORMAT_CLASS = "io.confluent.connect.hdfs.format.avro.AvroFormat";
  private Map<String, String> settings;
  private HDSourceConnectorConfig config;

  @Before
  public void before() {
    settings = minimumSettings();
    config = null;
  }

  @Test
  public void shouldAcceptValidMinimumConfig() {
    config = new HDSourceConnectorConfig(settings);
    assertNotNull(config);
  }

  @Test(expected = ConfigException.class)
  public void shouldInvalidateIfMinimumConfigSettingsNotSet() {
    settings.remove("store.url");
    config = new HDSourceConnectorConfig(settings);
  }

  
  private Map<String, String> minimumSettings() {
    Map<String, String> minSettings = new HashMap<>();
    minSettings.put("confluent.topic.bootstrap.servers", "localhost:9092");
    minSettings.put("store.url", "hdfs://localhost:9000/");
    minSettings.put("format.class", AVRO_FORMAT_CLASS);
    return minSettings;
  }
}