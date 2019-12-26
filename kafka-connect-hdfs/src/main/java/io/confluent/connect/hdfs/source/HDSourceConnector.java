/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.cloud.storage.source.AbstractCloudStorageSourceConnector;
import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.utils.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Source connector class for HDFS Source Connector.
 */
public class HDSourceConnector extends AbstractCloudStorageSourceConnector {

  /*
   * Your connector should never use System.out for logging.
   * All of your classes should use slf4j.
   */
  private static Logger log = LoggerFactory.getLogger(HDSourceConnector.class);

  public static final int MAX_TIMEOUT = 10;

  protected HDSourceConnectorConfig config;

  public HDSourceConnector() {}

  @Override
  public String version() {
    return Version.forClass(this.getClass());
  }

  @Override
  public Class<? extends Task> taskClass() {
    return HDSourceTask.class;
  }

  @Override
  protected CloudStorageSourceConnectorCommonConfig createConfig(Map<String, String> props) {
    props.put(HDSourceConnectorConfig.FILE_NAME_REGEX_PATTERN,
            HDSourceConnectorConfig.FILE_NAME_REGEX_PATTERN_VALUE);
    this.config = new HDSourceConnectorConfig(props);
    return this.config;
  }

  @Override
  protected CloudSourceStorage createStorage() {
    try {
      return new HDStorage(config, config.getHdfsUrl());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  protected Partitioner getPartitioner(CloudSourceStorage storage) {
    return config.getPartitioner(storage);
  }

  void setConfig(Map<String, String> props) {
    this.config = new HDSourceConnectorConfig(props);
    super.config = this.config;
  }

  @Override
  public ConfigDef config() {
    return HDSourceConnectorConfig.config();
  }
}