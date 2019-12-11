/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.utils.licensing.LicenseConfigUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

/**
 * Configuration class for {@link HDSourceConnector}.
 */
public class HDSourceConnectorConfig extends AbstractConfig {

  // TODO: Change these to use your own configuration settings
  public static final String MY_SETTING_CONFIG = "my.setting";
  private static final String MY_SETTING_DISPLAY = "My Setting";
  private static final String MY_SETTING_DOC = "This is a setting important to my connector.";
  static final Object MY_SETTING_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;

  public static final String PORT_CONFIG = "port";
  private static final String PORT_DISPAY = "Port";
  private static final String PORT_DOC = "This is a setting important to my connector.";
  static final int PORT_DEFAULT = 1234;

  public final String mySetting;
  private final int port;

  public HDSourceConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    mySetting = getString(MY_SETTING_CONFIG);
    port = getInt(PORT_CONFIG);
  }

  public static ConfigDef config() {
    // TODO: Change these to use your own configuration settings with validators and recommenders
    // (if needed)
    int orderInGroup = 0;
    ConfigDef result = new ConfigDef()
        .define(
            MY_SETTING_CONFIG,
            Type.STRING,
            MY_SETTING_DEFAULT,
            Importance.HIGH,
            MY_SETTING_DOC,
            "Connection",
            orderInGroup++,
            Width.NONE,
            MY_SETTING_DISPLAY
        ).define(
            PORT_CONFIG,
            Type.INT,
            PORT_DEFAULT,
            ConfigDef.Range.between(0, 33000),
            Importance.MEDIUM,
            PORT_DOC,
            "Connection",
            orderInGroup++,
            Width.SHORT,
            PORT_DISPAY
        );
    return LicenseConfigUtil.addToConfigDef(result);
  }

  public String mySetting() {
    return mySetting;
  }

  public int port() {
    return port;
  }

  public static void main(String[] args) {
    System.out.println(config().toEnrichedRst());
  }
}
