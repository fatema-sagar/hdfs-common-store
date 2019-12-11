/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.utils.Version;
import io.confluent.connect.utils.licensing.ConnectLicenseManager;
import io.confluent.connect.utils.licensing.LicenseConfigUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Source connector class for HDFS Source Connector.
 */
public class HDSourceConnector extends SourceConnector {

  /*
   * Your connector should never use System.out for logging.
   * All of your classes should use slf4j.
   */
  private static Logger log = LoggerFactory.getLogger(HDSourceConnector.class);

  HDSourceConnectorConfig config;
  ConnectLicenseManager licenseManager;

  @Override
  public String version() {
    return Version.forClass(this.getClass());
  }

  @Override
  public Class<? extends Task> taskClass() {
    return HDSourceTask.class;
  }

  @Override
  public void start(Map<String, String> map) {
    config = new HDSourceConnectorConfig(map);

    // Start the Confluent license manager
    licenseManager = LicenseConfigUtil.createLicenseManager(config);
    doStart();
  }

  // For testing
  void doStart() {
    // First, ensure the license is valid or register a trial license
    licenseManager.registerOrValidateLicense();

    /*
     * This will be executed once per connector. This can be used to handle connector level setup.
     * For example if you are persisting state, you can use this to method to create your state
     * table. You could also use this to verify permissions.
     */
    // TODO: Add things you need to do to setup your connector
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    //TODO: Define the individual task configurations that will be executed.

    /*
     * This is used to schedule the number of tasks that will be running.
     * This should not exceed maxTasks. Here is a spot where you can dish out work.
     * For example if you are reading from multiple tables in a database, you can
     * assign a table per task.
     */

    List<Map<String, String>> configs = new ArrayList<>();
    Map<String, String> taskConfig = new HashMap<>(config.originalsStrings());
    //TODO: Add extra configs for the task
    configs.add(taskConfig);
    //TODO: Repeat this for additional tasks

    return configs;
  }

  @Override
  public void stop() {
    //TODO: Do things that are necessary to stop your connector.
  }

  @Override
  public ConfigDef config() {
    return HDSourceConnectorConfig.config();
  }
}